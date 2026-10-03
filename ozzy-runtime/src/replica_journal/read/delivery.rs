//! Native delivery from shared RAM selections or a completed historical read.

use super::{PartitionReadCursor, PartitionReadError, PartitionReadLease, ReadPartition};
use crate::completion;
use crate::replica_journal::{JournalError, records::CapturedRecords};
use ozzy_journal_segment::ResidentRecordRead;
use ozzy_proto::reader::RecordsEncoder;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub(in crate::replica_journal) enum ReadReply {
    Copied(completion::Sender<Result<ReadPartition, JournalError>>),
    Direct(completion::Sender<Result<ReadDelivery, JournalError>>),
}

impl ReadReply {
    pub(in crate::replica_journal) fn finish(
        self,
        result: Result<ReadPartition, JournalError>,
        permit: OwnedSemaphorePermit,
    ) -> bool {
        use crate::replica_journal::commands::finish_read;
        match self {
            Self::Copied(done) => finish_read(done, result, permit),
            Self::Direct(done) => finish_read(done, result.map(ReadDelivery::Copied), permit),
        }
    }
}

/// One outstanding delivery per subscriber. A contiguous reply shares canonical
/// backing through the bounded transport lease; mixed backings are copied.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "one bounded completion per reader; keep captured ranges inline"
)]
pub(crate) enum ReadDelivery {
    Resident(ResidentPartition),
    Copied(ReadPartition),
}

#[derive(Debug)]
pub(crate) struct ResidentPartition {
    pub(in crate::replica_journal) cursor: PartitionReadCursor,
    // Keep the subscriber's existing admission bound across capture and encode.
    pub(in crate::replica_journal) _lease: PartitionReadLease,
    pub(in crate::replica_journal) records: Option<ResidentSource>,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "reuse inline captured ranges without allocating a box per reply"
)]
pub(in crate::replica_journal) enum ResidentSource {
    Stored(ResidentRecordRead),
    Pending(CapturedRecords),
}

impl ReadDelivery {
    /// Encode one reader message. `maximum` is the reader's full window: a
    /// compressed producer batch that fits it waits for room instead of
    /// being split.
    pub(crate) fn encode(
        self,
        output: &mut RecordsEncoder<'_>,
        maximum: ozzy_proto::data::DataLimits,
    ) -> Result<PartitionReadCursor, JournalError> {
        let started = crate::profiling::start();
        let result = self.encode_inner(output, maximum);
        crate::profiling::finish(crate::profiling::Stage::ReaderEncode, started);
        result
    }

    fn encode_inner(
        self,
        output: &mut RecordsEncoder<'_>,
        maximum: ozzy_proto::data::DataLimits,
    ) -> Result<PartitionReadCursor, JournalError> {
        match self {
            Self::Resident(delivery) => delivery.encode(output, maximum),
            Self::Copied(read) => {
                if read.records().len() != 0 {
                    #[cfg(feature = "storage-metrics")]
                    crate::storage_metrics::add(
                        &crate::storage_metrics::HISTORICAL_READ_DELIVERIES,
                        1,
                    );
                    output
                        .extend(read.records())
                        .map_err(|_| PartitionReadError::Limits)?;
                }
                Ok(read.cursor())
            }
        }
    }
}

/// A message carries one producer LZ4 block. Forward whole compressed batches
/// unchanged: end the message before one (`Err(None)`), and wait for room
/// (`Err(Some(_))`) rather than decompress a batch the reader's window can hold.
fn whole_compressed_batch(
    span: &ozzy_journal_segment::RecordSpan<'_>,
    output: &RecordsEncoder<'_>,
    maximum: ozzy_proto::data::DataLimits,
) -> Result<(), Option<PartitionReadError>> {
    if !span.is_prepared() || !span.starts_batch() {
        return Ok(());
    }
    if !output.is_empty() {
        return Err(None);
    }
    let (records, bytes, parts) = span.batch_totals();
    let remaining = output.remaining();
    let short = span.len() < records
        || records > remaining.max_records
        || bytes > remaining.envelope.max_payload_bytes
        || parts > remaining.max_parts;
    if short
        && records <= maximum.max_records
        && bytes <= maximum.envelope.max_payload_bytes
        && parts <= maximum.max_parts
    {
        return Err(Some(PartitionReadError::RecordTooLarge { bytes, parts }));
    }
    Ok(())
}

fn advance(cursor: &mut PartitionReadCursor, records: usize) {
    cursor.next = ozzy_proto::Offset::new(
        cursor
            .next
            .get()
            .checked_add(records as u64)
            .expect("validated read end"),
    );
}

impl ResidentPartition {
    fn encode(
        mut self,
        output: &mut RecordsEncoder<'_>,
        maximum: ozzy_proto::data::DataLimits,
    ) -> Result<PartitionReadCursor, JournalError> {
        #[cfg(feature = "storage-metrics")]
        if self.records.is_some() {
            crate::storage_metrics::add(&crate::storage_metrics::DIRECT_RESIDENT_READS, 1);
        }
        let mut error = None;
        let receive = |span: &ozzy_journal_segment::RecordSpan<'_>| {
            let before = output.len();
            if let Err(stop) = whole_compressed_batch(span, output, maximum) {
                error = stop;
                return 0;
            }
            if span.len() <= output.remaining().max_records
                && let Some((descriptors, decoded_bytes, body, encoded)) =
                    span.prepared_backing(output.shared_backing_limit())
                && output.extend_shared_prepared_lz4(
                    descriptors,
                    decoded_bytes,
                    body,
                    encoded,
                    span.len(),
                ) == Ok(true)
            {
                advance(&mut self.cursor, span.len());
                return span.len();
            }
            if span.len() <= output.remaining().max_records
                && let Some((descriptors, body, range)) =
                    span.encoded_backing(output.shared_backing_limit())
                && output.extend_shared_packed(descriptors, body, range, span.len()) == Ok(true)
            {
                advance(&mut self.cursor, span.len());
                return span.len();
            }
            for record in span.records() {
                let limits = output.remaining();
                let bytes = record.payload_bytes();
                let parts = record.parts().len();
                if limits.max_records == 0 {
                    break;
                }
                if bytes > limits.envelope.max_payload_bytes || parts > limits.max_parts {
                    if output.is_empty() {
                        error = Some(PartitionReadError::RecordTooLarge { bytes, parts });
                    }
                    break;
                }
                let shared = match record.payload_backing(output.shared_backing_limit()) {
                    Some((body, ranges)) => {
                        output.push_shared(record.message_id(), record.encoding(), body, ranges)
                    }
                    None => Ok(false),
                };
                let encoded = if shared == Ok(true) {
                    Ok(())
                } else if shared.is_err() {
                    shared.map(|_| ())
                } else if record.encoding() == ozzy_proto::data::Encoding::Raw && parts == 1 {
                    output.push_raw(
                        record.message_id(),
                        record.parts().next().expect("one part"),
                    )
                } else {
                    output.extend(std::iter::once((
                        record.message_id(),
                        record.encoding(),
                        record.parts(),
                    )))
                };
                if encoded.is_err() {
                    error = Some(PartitionReadError::Limits);
                    break;
                }
                self.cursor.next = self.cursor.next.checked_next().expect("validated read end");
            }
            output.len() - before
        };
        match self.records {
            Some(ResidentSource::Stored(read)) => read.visit_spans(receive)?,
            Some(ResidentSource::Pending(read)) => {
                #[cfg(feature = "storage-metrics")]
                crate::storage_metrics::add(&crate::storage_metrics::BACKGROUND_RESIDENT_READS, 1);
                read.visit_spans(receive);
            }
            None => {}
        }
        if let Some(error) = error {
            return Err(error.into());
        }
        Ok(self.cursor)
    }
}
