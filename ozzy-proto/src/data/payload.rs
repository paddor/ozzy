//! Owned payload parts of one record, sized for single-part records.

use bytes::Bytes;
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

/// Original payload parts of one record, including empty parts. Derefs to
/// `[Bytes]`. A single part is held directly; other part counts share one
/// boxed slice, so moving a record copies 32 bytes of payload handle.
#[derive(Clone, Default)]
pub struct RecordPayload(Repr);

#[derive(Clone)]
enum Repr {
    One(Bytes),
    // Zero or at least two parts; one part is always `One`.
    Parts(Box<[Bytes]>),
}

impl Default for Repr {
    fn default() -> Self {
        Self::Parts(Box::default())
    }
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<RecordPayload>() == 32);

impl RecordPayload {
    /// All parts in order.
    #[inline]
    pub fn as_slice(&self) -> &[Bytes] {
        self
    }
}

impl Deref for RecordPayload {
    type Target = [Bytes];

    #[inline]
    fn deref(&self) -> &[Bytes] {
        match &self.0 {
            Repr::One(part) => std::slice::from_ref(part),
            Repr::Parts(parts) => parts,
        }
    }
}

impl DerefMut for RecordPayload {
    #[inline]
    fn deref_mut(&mut self) -> &mut [Bytes] {
        match &mut self.0 {
            Repr::One(part) => std::slice::from_mut(part),
            Repr::Parts(parts) => parts,
        }
    }
}

impl From<Bytes> for RecordPayload {
    #[inline]
    fn from(part: Bytes) -> Self {
        Self(Repr::One(part))
    }
}

impl From<Vec<Bytes>> for RecordPayload {
    fn from(mut parts: Vec<Bytes>) -> Self {
        match parts.len() {
            1 => Self(Repr::One(parts.swap_remove(0))),
            _ => Self(Repr::Parts(parts.into_boxed_slice())),
        }
    }
}

impl FromIterator<Bytes> for RecordPayload {
    fn from_iter<I: IntoIterator<Item = Bytes>>(parts: I) -> Self {
        let mut parts = parts.into_iter();
        let Some(first) = parts.next() else {
            return Self::default();
        };
        let Some(second) = parts.next() else {
            return Self(Repr::One(first));
        };
        Self(Repr::Parts(
            [first, second].into_iter().chain(parts).collect(),
        ))
    }
}

impl<'a> IntoIterator for &'a RecordPayload {
    type Item = &'a Bytes;
    type IntoIter = std::slice::Iter<'a, Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a mut RecordPayload {
    type Item = &'a mut Bytes;
    type IntoIter = std::slice::IterMut<'a, Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl PartialEq for RecordPayload {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for RecordPayload {}

impl fmt::Debug for RecordPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_counts_keep_order_and_one_part_needs_no_box() {
        for count in [0, 1, 2, 5] {
            let parts: Vec<Bytes> = (0..count).map(|n| Bytes::from(vec![n; n.into()])).collect();
            let collected: RecordPayload = parts.iter().cloned().collect();
            let converted = RecordPayload::from(parts.clone());
            assert_eq!(&*collected, parts.as_slice());
            assert_eq!(collected, converted);
            assert_eq!(matches!(collected.0, Repr::One(_)), count == 1);
            assert_eq!(matches!(converted.0, Repr::One(_)), count == 1);
        }
        let mut payload = RecordPayload::from(Bytes::from_static(b"a"));
        payload[0] = Bytes::from_static(b"b");
        assert_eq!(payload, RecordPayload::from(vec![Bytes::from_static(b"b")]));
        assert_ne!(payload, RecordPayload::default());
        assert_eq!(format!("{payload:?}"), "[b\"b\"]");
    }
}
