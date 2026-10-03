#![cfg(feature = "storage-metrics")]

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalOperation, Digest, OperationKind, SEGMENT_HEADER_BYTES, SegmentHeader,
    SegmentWriter, write_metrics,
};
use ozzy_proto::GroupId;

#[test]
fn encoded_bytes_exclude_preallocation_and_rejected_appends() {
    let encodings = [
        BodyEncoding::Raw,
        #[cfg(feature = "lz4")]
        BodyEncoding::Lz4 {
            min_savings_bytes: 8,
        },
    ];
    let group = GroupId::from_bytes([7; 16]);
    let body = vec![42; 64 * 1024];
    for encoding in encodings {
        let before = write_metrics::encoded_group_bytes();
        let file = tempfile::tempfile().unwrap();
        let allocated = file.try_clone().unwrap();
        let header = SegmentHeader::new(group, 1, None, Digest::ZERO, 1024 * 1024).unwrap();
        let mut writer = SegmentWriter::initialize(file, header, JournalGeneration(1)).unwrap();
        allocated.set_len(1024 * 1024).unwrap();
        assert_eq!(write_metrics::encoded_group_bytes(), before);
        let operation = CanonicalOperation {
            group_id: group,
            configuration_epoch: 1,
            original_view: 1,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: OperationKind::Barrier,
            body: &body,
        };
        let position = writer
            .append_with_body_encoding(std::slice::from_ref(&operation), encoding)
            .unwrap();
        let expected = position.end_offset() - SEGMENT_HEADER_BYTES as u64;
        assert_eq!(write_metrics::encoded_group_bytes() - before, expected);
        assert!(
            writer
                .append_with_body_encoding(&[operation], encoding)
                .is_err()
        );
        assert_eq!(write_metrics::encoded_group_bytes() - before, expected);
        let file = writer.into_inner();
        assert!(expected < file.metadata().unwrap().len());
        if !matches!(encoding, BodyEncoding::Raw) {
            assert!(expected < body.len() as u64);
        }
    }
}
