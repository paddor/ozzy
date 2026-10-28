//! Only batch bounds are needed to extend the offset index.

use ozzy_journal::operation::{OperationBody, OperationKind};
use ozzy_proto::{Offset, PartitionIncarnation};

pub(crate) struct ReadBatch {
    pub(crate) partition: PartitionIncarnation,
    pub(crate) first_offset: Offset,
    pub(crate) records: usize,
    pub(crate) append_timestamp_millis: u64,
}

pub(crate) trait ReadIndexBody {
    fn kind(&self) -> OperationKind;
    fn batches(&self) -> impl Iterator<Item = ReadBatch>;
}

impl ReadIndexBody for OperationBody<'_> {
    fn kind(&self) -> OperationKind {
        self.kind()
    }

    fn batches(&self) -> impl Iterator<Item = ReadBatch> {
        let batches = match self {
            Self::Append(append) => append.batches.as_slice(),
            Self::ResolvedAppend(append) => std::slice::from_ref(&append.batch),
            Self::Release(release) => std::slice::from_ref(&release.batch),
            _ => &[],
        };
        batches.iter().map(|batch| ReadBatch {
            partition: batch.partition,
            first_offset: batch.first_offset,
            records: batch.records.len(),
            append_timestamp_millis: batch.append_timestamp_millis,
        })
    }
}
