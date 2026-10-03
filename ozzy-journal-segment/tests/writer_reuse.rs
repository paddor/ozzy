//! The public writer must use reserved scratch, not the allocating codec wrapper.

#[path = "support/allocations.rs"]
mod allocations;

use std::io;

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalOperation, Digest, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, OperationKind,
    SEGMENT_HEADER_BYTES, SegmentHeader, SegmentIo, SegmentWriter, WRITE_GROUP_ALIGNMENT,
    encode_group_with_body_encoding, encode_segment_header, logical_operation_digest,
};
use ozzy_proto::GroupId;

#[derive(Debug)]
struct FixedIo {
    bytes: Vec<u8>,
    length: usize,
}

impl SegmentIo for FixedIo {
    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.length as u64)
    }

    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.bytes[..self.length].to_vec())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let start = usize::try_from(offset).unwrap();
        let end = start + bytes.len();
        self.bytes[start..end].copy_from_slice(bytes);
        self.length = self.length.max(end);
        Ok(bytes.len())
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.length = usize::try_from(length).unwrap();
        assert!(self.length <= self.bytes.len());
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn exercise(encoding: BodyEncoding, count: usize) {
    const BODY_BYTES: usize = 4096;
    const CAPACITY: usize = 8 * 1024 * 1024;
    let group = GroupId::from_bytes([1; 16]);
    let header = SegmentHeader::new(group, 1, None, Digest::ZERO, CAPACITY as u64).unwrap();
    let mut expected = encode_segment_header(&header).to_vec();
    let io = FixedIo {
        bytes: vec![0; CAPACITY],
        length: 0,
    };
    let mut writer = SegmentWriter::initialize(io, header.clone(), JournalGeneration(1)).unwrap();
    let reserve =
        count * (BODY_BYTES + ENTRY_HEADER_BYTES + 7) + GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT;
    writer.reserve_encode_buffer(reserve / 2, encoding).unwrap();
    writer.reserve_encode_buffer(reserve, encoding).unwrap();
    // Low-level physical codec bodies are opaque; directory tests enforce syntax.
    let body = [42; BODY_BYTES];
    for iteration in 0..16 {
        let before = writer.written_position();
        let mut digest = before.next_chain().previous_digest();
        let operations: Vec<_> = (0..count)
            .map(|index| {
                let operation = CanonicalOperation {
                    group_id: group,
                    configuration_epoch: 1,
                    original_view: 1,
                    op_number: (iteration * count + index + 1) as u64,
                    previous_digest: digest,
                    kind: OperationKind::Barrier,
                    body: &body,
                };
                digest = logical_operation_digest(&operation);
                operation
            })
            .collect();
        let encoded = encode_group_with_body_encoding(
            &header,
            iteration as u64 + 1,
            before.end_offset(),
            before.next_chain(),
            &operations,
            encoding,
        )
        .unwrap();
        if iteration != 0 {
            let mut invalid = operations.clone();
            invalid[0].previous_digest = Digest::from_bytes([99; 32]);
            let (rejected, allocations) =
                allocations::measure(|| writer.append_with_body_encoding(&invalid, encoding));
            assert!(rejected.is_err());
            assert_eq!(allocations, 0, "rejected group discarded reserved scratch");
            assert_eq!(writer.written_position(), before);
            assert!(!writer.is_faulted());
        }
        let (written, allocations) = allocations::measure(|| {
            writer
                .append_with_body_encoding(&operations, encoding)
                .unwrap()
        });
        assert_eq!(
            allocations, 0,
            "{encoding:?}, {count} operations, iteration {iteration}"
        );
        assert_eq!(written.next_chain(), encoded.next_chain());
        assert_eq!(
            writer.durable_position().end_offset(),
            SEGMENT_HEADER_BYTES as u64
        );
        expected.extend_from_slice(encoded.as_bytes());
    }
    let io = writer.into_inner();
    assert_eq!(&io.bytes[..io.length], expected);
}

#[test]
fn reserved_raw_groups_do_not_allocate_and_match_stateless_codec() {
    for count in [1, 8, 64] {
        exercise(BodyEncoding::Raw, count);
    }
}

#[cfg(feature = "lz4")]
#[test]
fn reserved_lz4_groups_do_not_allocate_and_match_stateless_codec() {
    for count in [1, 8, 64] {
        exercise(
            BodyEncoding::Lz4 {
                min_savings_bytes: 32,
            },
            count,
        );
        exercise(
            BodyEncoding::Lz4 {
                min_savings_bytes: usize::MAX,
            },
            count,
        );
    }
}
