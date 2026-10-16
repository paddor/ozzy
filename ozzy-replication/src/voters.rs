//! Positions of the three configured voters, per-voter values, and voter sets.

use std::ops::{Index, IndexMut};

/// Position of one configured voter. Indexing per-voter values with it cannot fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VoterIndex {
    First,
    Second,
    Third,
}

impl VoterIndex {
    /// Every position, in configured order.
    pub(crate) const ALL: [Self; 3] = [Self::First, Self::Second, Self::Third];

    /// Rotating primary position for `view`.
    pub(crate) const fn primary(view: u64) -> Self {
        match view % 3 {
            0 => Self::First,
            1 => Self::Second,
            _ => Self::Third,
        }
    }

    /// The two other positions.
    pub(crate) const fn others(self) -> [Self; 2] {
        match self {
            Self::First => [Self::Second, Self::Third],
            Self::Second => [Self::Third, Self::First],
            Self::Third => [Self::First, Self::Second],
        }
    }

    const fn bit(self) -> u8 {
        match self {
            Self::First => 1,
            Self::Second => 2,
            Self::Third => 4,
        }
    }
}

/// One value per configured voter, in configured order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Voters<T>(pub(crate) [T; 3]);

impl<T: Copy> Voters<T> {
    /// The same value for every voter.
    pub(crate) const fn splat(value: T) -> Self {
        Self([value; 3])
    }
}

impl<T> Voters<T> {
    /// Values in configured order.
    pub(crate) fn values(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Values paired with their positions, in configured order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (VoterIndex, &T)> {
        VoterIndex::ALL.into_iter().zip(&self.0)
    }
}

impl<T> Index<VoterIndex> for Voters<T> {
    type Output = T;

    fn index(&self, voter: VoterIndex) -> &T {
        let [first, second, third] = &self.0;
        match voter {
            VoterIndex::First => first,
            VoterIndex::Second => second,
            VoterIndex::Third => third,
        }
    }
}

impl<T> IndexMut<VoterIndex> for Voters<T> {
    fn index_mut(&mut self, voter: VoterIndex) -> &mut T {
        let [first, second, third] = &mut self.0;
        match voter {
            VoterIndex::First => first,
            VoterIndex::Second => second,
            VoterIndex::Third => third,
        }
    }
}

/// Set of configured voters, one bit each in configured order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct VoterSet(u8);

impl VoterSet {
    pub(crate) const EMPTY: Self = Self(0);

    /// Wire and public form: bit `n` is the voter at configured position `n`.
    pub(crate) const fn mask(self) -> u8 {
        self.0
    }

    pub(crate) const fn contains(self, voter: VoterIndex) -> bool {
        self.0 & voter.bit() != 0
    }

    pub(crate) const fn len(self) -> u32 {
        self.0.count_ones()
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(crate) fn insert(&mut self, voter: VoterIndex) {
        self.0 |= voter.bit();
    }

    pub(crate) fn remove(&mut self, voter: VoterIndex) {
        self.0 &= !voter.bit();
    }

    /// Remove and return the first member in configured order.
    pub(crate) fn pop_first(&mut self) -> Option<VoterIndex> {
        let voter = VoterIndex::ALL
            .into_iter()
            .find(|&voter| self.contains(voter))?;
        self.remove(voter);
        Some(voter)
    }
}

impl FromIterator<VoterIndex> for VoterSet {
    fn from_iter<I: IntoIterator<Item = VoterIndex>>(voters: I) -> Self {
        let mut set = Self::EMPTY;
        voters.into_iter().for_each(|voter| set.insert(voter));
        set
    }
}
