//! Test-only constructors and observations of writer state.

use super::*;

pub(in crate::replicated::writer) struct TestRecord {
    pub(in crate::replicated::writer) payload: Payload,
    pub(in crate::replicated::writer) lengths: Vec<usize>,
}

impl Shared {
    pub(in crate::replicated::writer) fn channel_with_capacity(
        config: WriterConfig,
        lane_capacity: usize,
    ) -> (Writer, Driver) {
        let runtime = WriterRuntime::new().unwrap();
        let owned = runtime.clone();
        futures::executor::block_on(
            runtime
                .driver()
                .spawn(async move { Self::open_reserved(owned, config, lane_capacity, None) }),
        )
        .unwrap()
    }
}

impl Driver {
    pub(in crate::replicated::writer) fn settle(&mut self) {
        self.drain();
    }

    pub(in crate::replicated::writer) fn record(&mut self, sequence: u64) -> Option<TestRecord> {
        self.settle();
        let first = self.admission.records.front()?.sequence;
        let index = usize::try_from(sequence.checked_sub(first)?).ok()?;
        self.admission.records.get(index)?;
        let payload = self.admission.payload(index);
        let record = &self.admission.records[index];
        Some(TestRecord {
            payload,
            lengths: record.lengths().collect(),
        })
    }

    /// Remote confirmation releases local retry state and drops original payloads.
    /// Every confirmed record was selected from the admission queue, so no
    /// intake drain is needed to cover `end`.
    pub(in crate::replicated::writer) fn confirm(
        &mut self,
        first: u64,
        end: u64,
        first_offset: u64,
    ) -> Result<usize, WriterError> {
        let records = end
            .checked_sub(first)
            .and_then(|count| u32::try_from(count).ok())
            .ok_or(crate::replicated::Error::Response)?;
        self.confirm_outcomes(
            first,
            OutcomeRun {
                disposition: AppendDisposition::Stored,
                first_offset: ozzy_proto::Offset::new(first_offset),
                records,
            },
        )
    }
}
