use std::fs::OpenOptions;
use std::io::{self, IoSlice};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, ChainPosition, DecodeLimits, Digest, OperationKind, SEGMENT_HEADER_BYTES,
    SegmentHeader, SegmentIo, SegmentWriter, WriterError, encode_group, logical_operation_digest,
};
use ozzy_proto::GroupId;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Default)]
struct MemoryIo {
    bytes: Vec<u8>,
    stable: Vec<u8>,
    max_write: usize,
    write_budget: Option<usize>,
    sync_calls: usize,
    fail_sync_call: Option<usize>,
    set_len_calls: Arc<AtomicUsize>,
    vectored_calls: usize,
    max_slices: usize,
    interrupt_vectored: bool,
    vectored_result: Option<usize>,
}

impl MemoryIo {
    fn with_max_write(max_write: usize) -> Self {
        Self {
            max_write,
            ..Self::default()
        }
    }
}

impl SegmentIo for MemoryIo {
    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.bytes.clone())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let offset = usize::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
        let budget = self.write_budget.unwrap_or(usize::MAX);
        if budget == 0 {
            return Err(io::Error::other("injected write failure"));
        }
        let written = bytes.len().min(self.max_write.max(1)).min(budget);
        let end = offset
            .checked_add(written)
            .ok_or(io::ErrorKind::InvalidInput)?;
        self.bytes.resize(self.bytes.len().max(end), 0);
        self.bytes[offset..end].copy_from_slice(&bytes[..written]);
        if let Some(remaining) = &mut self.write_budget {
            *remaining -= written;
        }
        Ok(written)
    }

    fn write_vectored_at(&mut self, mut offset: u64, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        self.vectored_calls += 1;
        self.max_slices = self.max_slices.max(buffers.len());
        if self.interrupt_vectored && self.vectored_calls % 2 == 1 {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if let Some(result) = self.vectored_result {
            return Ok(result);
        }
        let mut total = 0;
        for buffer in buffers {
            let available = self.max_write.max(1) - total;
            if available == 0 {
                break;
            }
            let written = self.write_at(offset, &buffer[..buffer.len().min(available)])?;
            total += written;
            offset += written as u64;
            if written < buffer.len() {
                break;
            }
        }
        Ok(total)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.set_len_calls.fetch_add(1, Ordering::Relaxed);
        let len = usize::try_from(len).map_err(|_| io::ErrorKind::InvalidInput)?;
        self.bytes.resize(len, 0);
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.sync_calls += 1;
        if self.fail_sync_call == Some(self.sync_calls) {
            return Err(io::Error::other("injected sync failure"));
        }
        self.stable.clone_from(&self.bytes);
        Ok(())
    }
}

fn group_id() -> GroupId {
    GroupId::from_bytes([0x11; 16])
}

fn segment(id: u64) -> SegmentHeader {
    SegmentHeader::new(group_id(), id, None, Digest::ZERO, 64 * MIB).unwrap()
}

fn operation(
    number: u64,
    previous_digest: Digest,
    body: &'static [u8],
) -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: group_id(),
        configuration_epoch: 1,
        original_view: 1,
        op_number: number,
        previous_digest,
        kind: OperationKind::Barrier,
        body,
    }
}

#[test]
fn vectored_groups_cross_slice_and_syscall_boundaries_without_flattening() {
    let mut chain = ChainPosition::GENESIS;
    let operations: Vec<_> = (0..600)
        .map(|i| {
            let op = operation(
                i + 1,
                chain.previous_digest(),
                if i % 2 == 0 { b"tiny" } else { b"" },
            );
            chain = ChainPosition::new(i + 2, logical_operation_digest(&op));
            op
        })
        .collect();
    let expected = encode_group(
        &segment(1),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    for max_write in [1, 7, 193, 207, usize::MAX] {
        let mut io = MemoryIo::with_max_write(max_write);
        io.interrupt_vectored = true;
        let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
        let written = writer.append(&operations).unwrap();
        assert_eq!(written.next_chain(), expected.next_chain());
        assert_eq!(written.end_offset(), expected.end_offset());
        assert_eq!(writer.durable_position().group_number(), 0);
        let io = writer.into_inner();
        assert_eq!(&io.bytes[SEGMENT_HEADER_BYTES..], expected.as_bytes());
        assert!(io.vectored_calls >= 4);
        let max_slices = if cfg!(any(target_os = "linux", target_os = "android")) {
            1024
        } else {
            64
        };
        assert_eq!(io.max_slices, max_slices);
    }
}

#[test]
fn invalid_vectored_results_fault_without_advancing_written_or_durable_prefix() {
    for result in [0, usize::MAX] {
        let mut io = MemoryIo::with_max_write(4096);
        io.vectored_result = Some(result);
        let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
        let before = writer.written_position();
        let error = writer
            .append(&[operation(1, Digest::ZERO, b"tiny")])
            .unwrap_err();
        assert!(matches!(error, WriterError::Io(_)));
        assert!(writer.is_faulted());
        assert_eq!(writer.written_position(), before);
        assert_eq!(writer.durable_position(), before);
    }
}

#[test]
fn vectored_failure_in_seal_never_confirms_and_recovery_trims_incomplete_group() {
    let mut io = MemoryIo::with_max_write(211);
    io.write_budget = Some(SEGMENT_HEADER_BYTES + 4096 - 1);
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    let before = writer.written_position();
    assert!(
        writer
            .append(&[operation(1, Digest::ZERO, b"tiny")])
            .is_err()
    );
    assert!(writer.is_faulted());
    assert_eq!(writer.written_position(), before);
    assert_eq!(writer.durable_position(), before);
    let mut io = writer.into_inner();
    io.write_budget = None;
    let recovered = SegmentWriter::recover(
        io,
        JournalGeneration(2),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        64 * MIB,
    )
    .unwrap();
    assert_eq!(recovered.written_position().group_number(), 0);
    assert_eq!(recovered.into_inner().bytes.len(), SEGMENT_HEADER_BYTES);
}

#[test]
fn partial_writes_form_complete_groups_and_sync_evidence_stays_frozen() {
    let io = MemoryIo::with_max_write(13);
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    let first = writer
        .append(&[operation(1, Digest::ZERO, b"one")])
        .unwrap();
    let first_sync = writer.begin_sync();
    assert_eq!(first, first_sync);
    assert_eq!(writer.durable_position().group_number(), 0);

    let second = writer
        .append(&[operation(2, first.next_chain().previous_digest(), b"two")])
        .unwrap();
    assert_eq!(writer.sync_through(first_sync).unwrap(), first);
    assert_eq!(writer.durable_position(), first);
    assert_eq!(writer.written_position(), second);

    let io = writer.into_inner();
    assert_eq!(io.bytes.len(), SEGMENT_HEADER_BYTES + 2 * 4096);
    assert_eq!(io.stable, io.bytes);
    let mut recovered = SegmentWriter::recover(
        io,
        JournalGeneration(2),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        64 * MIB,
    )
    .unwrap();
    assert_eq!(
        recovered.written_position().group_number(),
        second.group_number()
    );
    assert_eq!(
        recovered.written_position().end_offset(),
        second.end_offset()
    );
    assert_eq!(
        recovered.written_position().next_chain(),
        second.next_chain()
    );
    assert_eq!(recovered.durable_position(), recovered.written_position());
    assert_eq!(
        recovered.written_position().generation(),
        JournalGeneration(2)
    );
    assert!(matches!(
        recovered.sync_through(first_sync),
        Err(WriterError::InvalidSyncPosition)
    ));
}

#[test]
fn write_failure_faults_writer_and_recovery_removes_only_truncated_tail() {
    let mut io = MemoryIo::with_max_write(17);
    io.write_budget = Some(SEGMENT_HEADER_BYTES + 250);
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    assert!(matches!(
        writer.append(&[operation(1, Digest::ZERO, b"one")]),
        Err(WriterError::Io(_))
    ));
    assert!(writer.is_faulted());
    assert!(matches!(
        writer.append(&[operation(1, Digest::ZERO, b"one")]),
        Err(WriterError::Faulted)
    ));

    let mut io = writer.into_inner();
    assert_eq!(io.bytes.len(), SEGMENT_HEADER_BYTES + 250);
    io.write_budget = None;
    let recovered = SegmentWriter::recover(
        io,
        JournalGeneration(2),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        64 * MIB,
    )
    .unwrap();
    assert_eq!(recovered.written_position().group_number(), 0);
    assert_eq!(
        recovered.written_position().end_offset(),
        SEGMENT_HEADER_BYTES as u64
    );
    let io = recovered.into_inner();
    assert_eq!(io.bytes.len(), SEGMENT_HEADER_BYTES);
    assert_eq!(io.stable, io.bytes);
}

#[test]
fn recovery_never_truncates_a_manifest_protected_missing_operation() {
    let mut io = MemoryIo::with_max_write(17);
    io.write_budget = Some(SEGMENT_HEADER_BYTES + 250);
    let intended = operation(1, Digest::ZERO, b"one");
    let protected = ChainPosition::new(2, logical_operation_digest(&intended));
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    assert!(writer.append(&[intended]).is_err());
    let io = writer.into_inner();
    let set_len_calls = Arc::clone(&io.set_len_calls);

    assert!(matches!(
        SegmentWriter::recover_protecting(
            io,
            JournalGeneration(2),
            1,
            ChainPosition::GENESIS,
            DecodeLimits::default(),
            64 * MIB,
            protected,
        ),
        Err(WriterError::ProtectedPrefixMismatch(1))
    ));
    assert_eq!(set_len_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn sync_failure_never_advances_evidence_and_faults_writer() {
    let mut io = MemoryIo::with_max_write(4096);
    io.fail_sync_call = Some(2);
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    let written = writer
        .append(&[operation(1, Digest::ZERO, b"one")])
        .unwrap();
    assert!(matches!(
        writer.sync_through(written),
        Err(WriterError::Io(_))
    ));
    assert!(writer.is_faulted());
    assert_eq!(writer.durable_position().group_number(), 0);
    let io = writer.into_inner();
    assert_eq!(io.stable.len(), SEGMENT_HEADER_BYTES);
}

#[test]
fn recovery_rejects_corruption_without_truncating_it() {
    let io = MemoryIo::with_max_write(4096);
    let mut writer = SegmentWriter::initialize(io, segment(1), JournalGeneration(1)).unwrap();
    writer
        .append(&[operation(1, Digest::ZERO, b"one")])
        .unwrap();
    let mut io = writer.into_inner();
    io.bytes[SEGMENT_HEADER_BYTES + 192] ^= 1;
    let set_len_calls = Arc::clone(&io.set_len_calls);
    assert!(matches!(
        SegmentWriter::recover(
            io,
            JournalGeneration(2),
            1,
            ChainPosition::GENESIS,
            DecodeLimits::default(),
            64 * MIB,
        ),
        Err(WriterError::Codec(_))
    ));
    assert_eq!(set_len_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn sync_token_from_another_segment_is_rejected_without_faulting() {
    let other = SegmentWriter::initialize(
        MemoryIo::with_max_write(4096),
        segment(2),
        JournalGeneration(1),
    )
    .unwrap();
    let foreign = other.begin_sync();
    let mut writer = SegmentWriter::initialize(
        MemoryIo::with_max_write(4096),
        segment(1),
        JournalGeneration(1),
    )
    .unwrap();
    assert!(matches!(
        writer.sync_through(foreign),
        Err(WriterError::InvalidSyncPosition)
    ));
    assert!(!writer.is_faulted());
}

#[test]
fn real_file_reopens_the_synchronized_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("1.log");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer = SegmentWriter::initialize(file, segment(1), JournalGeneration(1)).unwrap();
    let written = writer
        .append(&[operation(1, Digest::ZERO, b"one")])
        .unwrap();
    writer.sync_through(written).unwrap();
    drop(writer);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let recovered = SegmentWriter::recover(
        file,
        JournalGeneration(2),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        64 * MIB,
    )
    .unwrap();
    assert_eq!(
        recovered.durable_position().group_number(),
        written.group_number()
    );
    assert_eq!(
        recovered.durable_position().end_offset(),
        written.end_offset()
    );
    assert_eq!(
        recovered.durable_position().next_chain(),
        written.next_chain()
    );
    assert_eq!(
        recovered.durable_position().generation(),
        JournalGeneration(2)
    );
}
