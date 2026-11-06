//! Every confirmation and every read, checked as it arrives, and every
//! copy's log at the end. Records are named by producer and logical record
//! number, which their message ID and body carry; epochs map their own
//! sequences onto them.
//!
//! A message ID has one offset. On a dedup partition a resend under a new
//! epoch may store it again once the window passed since its first send,
//! less the clock slack.
//!
//! With age retention, a record may retire unread once it is old enough:
//! the floor that passes it, a copy's log start or a consumer's retention
//! gap, comes at least the age after its first send, less twice the slack.
//! Two leaders' clocks stamp its time and judge its age.

use super::Producers;
use crate::broker::CLOCK_TOLERANCE;
use std::collections::BTreeMap;

/// What one offset holds, as confirmations and reads saw it.
#[derive(Debug, Clone, Copy)]
struct Entry {
    record: (usize, u64),
    confirmed: bool,
    read: bool,
}

/// Every confirmed and every read record, checked as it arrives: one
/// offset and schedule sequence per record, one record per offset and
/// schedule sequence, and each producer's unscheduled records in sequence
/// order.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// Offsets past the first that a record was allowed to take.
    repeats: usize,
    /// Offset of each record, by producer and sequence, and whether it was
    /// scheduled.
    records: BTreeMap<(usize, u64), (u64, bool)>,
    offsets: BTreeMap<u64, Entry>,
    confirmed: usize,
    /// Schedule sequence of each confirmed scheduled record, and the reverse.
    schedules: BTreeMap<(usize, u64), u64>,
    scheduled: BTreeMap<u64, (usize, u64)>,
    /// Offsets below this one may have retired unread.
    retired: u64,
}

impl Ledger {
    /// Distinct confirmed records.
    pub(crate) fn len(&self) -> usize {
        self.confirmed + self.schedules.len()
    }

    /// Confirmed scheduled records.
    pub(crate) fn scheduled(&self) -> usize {
        self.schedules.len()
    }

    /// Records stored again after their dedup window.
    pub(crate) fn repeats(&self) -> usize {
        self.repeats
    }

    /// The offset below which records may have retired unread.
    pub(crate) fn retired(&self) -> u64 {
        self.retired
    }

    /// Every confirmed scheduled record was read, or its release may have
    /// retired unread by true time `wall`.
    pub(crate) fn released(&self, wall: u64, age: Option<u64>, producers: &Producers) -> bool {
        self.schedules.keys().all(|&record| {
            self.records.contains_key(&record) || self.release_retired(record, wall, age, producers)
        })
    }

    /// A release may have retired unread by true time `wall`: retention
    /// passed some offset, and the age passed since the record was due.
    fn release_retired(
        &self,
        (producer, sequence): (usize, u64),
        wall: u64,
        age: Option<u64>,
        producers: &Producers,
    ) -> bool {
        let (Some(age), Some(due)) = (age, producers.due(producer, sequence)) else {
            return false;
        };
        self.retired > 0
            && wall + 2 * producers.slack() >= due.saturating_sub(CLOCK_TOLERANCE) + age
    }

    /// Retention passed every offset below `floor` by true time `wall`:
    /// a copy's log starts there, or a consumer was told so. Each record
    /// there must be `age` old. One released no earlier than its due time
    /// less the tolerance.
    pub(crate) fn retire(
        &mut self,
        floor: u64,
        wall: u64,
        age: u64,
        producers: &Producers,
    ) -> Result<(), String> {
        if floor <= self.retired {
            return Ok(());
        }
        for (&offset, entry) in self.offsets.range(self.retired..floor) {
            let (producer, sequence) = entry.record;
            let sent = producers.first_sent(producer, sequence).unwrap_or(0);
            let stored = producers
                .due(producer, sequence)
                .map_or(sent, |due| sent.max(due.saturating_sub(CLOCK_TOLERANCE)));
            if wall + 2 * producers.slack() < stored + age {
                return Err(format!(
                    "offset {offset} (producer {producer} record {sequence}) retired at true \
                     time {wall}, though stored no earlier than {stored}, with a {age} ms age \
                     and {} ms clock slack",
                    producers.slack()
                ));
            }
        }
        self.retired = floor;
        Ok(())
    }

    /// Records `records` of `producer` stored, or found stored before,
    /// from `first_offset`, confirmed at true time `wall`. `repeat` gives
    /// when a record may take a second offset.
    pub(super) fn confirm(
        &mut self,
        producer: usize,
        records: std::ops::Range<u64>,
        first_offset: u64,
        repeat: impl Fn(u64) -> Option<u64>,
        wall: u64,
    ) -> Result<(), String> {
        for record in records.clone() {
            let offset = first_offset + (record - records.start);
            self.note(producer, record, offset, true, false, repeat(record), wall)?;
        }
        Ok(())
    }

    /// Records held until due, confirmed with schedule sequences from
    /// `first_schedule`.
    pub(super) fn schedule(
        &mut self,
        producer: usize,
        sequences: std::ops::Range<u64>,
        first_schedule: u64,
    ) -> Result<(), String> {
        for sequence in sequences.clone() {
            let schedule = first_schedule + (sequence - sequences.start);
            let record = (producer, sequence);
            if let Some(&old) = self.schedules.get(&record)
                && old != schedule
            {
                return Err(format!(
                    "producer {producer} record {sequence} scheduled as {schedule}, as {old} \
                     before"
                ));
            }
            if let Some(&other) = self.scheduled.get(&schedule)
                && other != record
            {
                return Err(format!(
                    "schedule sequence {schedule} confirmed for producer {producer} record \
                     {sequence}, for {other:?} before"
                ));
            }
            self.schedules.insert(record, schedule);
            self.scheduled.insert(schedule, record);
        }
        Ok(())
    }

    /// A consumer read producer `producer`'s record `sequence` at `offset`
    /// at true time `wall`. A scheduled record, due at `due`, released at
    /// most the clock tolerance early.
    pub(crate) fn read(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
        due: Option<u64>,
        repeat: Option<u64>,
        wall: u64,
    ) -> Result<(), String> {
        if let Some(due) = due
            && due > wall + CLOCK_TOLERANCE
        {
            return Err(format!(
                "producer {producer} record {sequence} read at offset {offset} {} ms before \
                 its due time",
                due - wall
            ));
        }
        self.note(
            producer,
            sequence,
            offset,
            false,
            due.is_some(),
            repeat,
            wall,
        )
    }

    /// One record at one offset. A record seen at another offset before
    /// fails, unless `repeat` allows it by true time `wall`.
    #[allow(clippy::too_many_arguments)]
    fn note(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
        confirmed: bool,
        scheduled: bool,
        repeat: Option<u64>,
        wall: u64,
    ) -> Result<(), String> {
        let how = if confirmed { "confirmed" } else { "read" };
        let repeated = match self.records.get(&(producer, sequence)) {
            Some(&(old, _)) if old != offset && repeat.is_some_and(|from| wall >= from) => true,
            Some(&(old, _)) if old != offset => {
                return Err(format!(
                    "producer {producer} record {sequence} {how} at offset {offset}, seen at \
                     {old} before"
                ));
            }
            _ => false,
        };
        if let Some(entry) = self.offsets.get(&offset)
            && entry.record != (producer, sequence)
        {
            return Err(format!(
                "offset {offset} {how} for producer {producer} record {sequence}, seen with {:?} \
                 before",
                entry.record
            ));
        }
        // One producer's unscheduled records keep their sequence order.
        let unscheduled = |(_, (_, scheduled)): &(&(usize, u64), &(u64, bool))| !*scheduled;
        let before = self
            .records
            .range((producer, 0)..(producer, sequence))
            .rev()
            .find(unscheduled);
        let after = self
            .records
            .range((producer, sequence + 1)..(producer + 1, 0))
            .find(unscheduled);
        if !scheduled
            && !repeated
            && (before.is_some_and(|(_, &(earlier, _))| earlier >= offset)
                || after.is_some_and(|(_, &(later, _))| later <= offset))
        {
            return Err(format!(
                "producer {producer} record {sequence} {how} at offset {offset}, out of order \
                 with {before:?} and {after:?}"
            ));
        }
        if !repeated {
            self.records
                .insert((producer, sequence), (offset, scheduled));
        }
        let entry = self.offsets.entry(offset).or_insert_with(|| {
            self.repeats += usize::from(repeated);
            Entry {
                record: (producer, sequence),
                confirmed: false,
                read: false,
            }
        });
        if confirmed && !entry.confirmed {
            self.confirmed += 1;
        }
        entry.confirmed |= confirmed;
        entry.read |= !confirmed;
        Ok(())
    }

    /// At the end, on a copy whose log ends at `end`: consumers read every
    /// offset below it but retired ones, each confirmed record is among
    /// them, scheduled ones in release order, and a record nobody confirmed
    /// comes from a batch whose outcome stayed unknown.
    pub(crate) fn check_log(
        &self,
        broker: usize,
        end: u64,
        read_through: u64,
        (wall, age): (u64, Option<u64>),
        producers: &Producers,
    ) -> Result<(), String> {
        if let Some((&offset, entry)) = self.offsets.range(end..).next() {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, but offset {offset} holds {:?} \
                 (confirmed {}, read {})",
                entry.record, entry.confirmed, entry.read
            ));
        }
        if read_through != end {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, consumers read through {read_through}"
            ));
        }
        let floor = self.retired.min(end);
        let seen = self.offsets.range(floor..).count() as u64;
        if seen != end - floor {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, but {seen} offsets from {floor} were \
                 seen"
            ));
        }
        for (&offset, entry) in &self.offsets {
            let (producer, sequence) = entry.record;
            if !entry.read && offset >= floor {
                return Err(format!(
                    "broker {broker}: confirmed offset {offset} was never read"
                ));
            }
            let confirmed = entry.confirmed || self.schedules.contains_key(&entry.record);
            // A resend under a new epoch settled what an attempt before it
            // may have stored.
            if !confirmed
                && !producers.may_hold(producer, sequence)
                && producers.repeat_from(producer, sequence).is_none()
            {
                return Err(format!(
                    "broker {broker}: offset {offset} holds producer {producer} record \
                     {sequence}, never confirmed though its batch was settled"
                ));
            }
        }
        self.check_releases(broker, wall, age, producers)
    }

    /// Every confirmed scheduled record was released, in order of due time,
    /// then schedule sequence: a later schedule sequence that is due no
    /// later never releases first. Acceptance times only grow with the
    /// schedule sequence, so they cannot reverse that.
    fn check_releases(
        &self,
        broker: usize,
        wall: u64,
        age: Option<u64>,
        producers: &Producers,
    ) -> Result<(), String> {
        let mut released = Vec::new();
        for (&(producer, sequence), &schedule) in &self.schedules {
            let Some(&(offset, _)) = self.records.get(&(producer, sequence)) else {
                if self.release_retired((producer, sequence), wall, age, producers) {
                    continue;
                }
                return Err(format!(
                    "broker {broker}: producer {producer} record {sequence}, confirmed as \
                     schedule {schedule}, was never released"
                ));
            };
            let due = producers
                .due(producer, sequence)
                .ok_or_else(|| format!("producer {producer} record {sequence} has no due time"))?;
            released.push((offset, due, schedule));
        }
        released.sort_unstable();
        for (at, &(offset, due, schedule)) in released.iter().enumerate() {
            if let Some(&(later, later_due, later_schedule)) =
                released[at + 1..]
                    .iter()
                    .find(|(_, later_due, later_schedule)| {
                        *later_schedule < schedule && *later_due <= due
                    })
            {
                return Err(format!(
                    "broker {broker}: schedule {later_schedule} due {later_due} released at offset \
                     {later}, after schedule {schedule} due {due} at offset {offset}"
                ));
            }
        }
        Ok(())
    }
}
