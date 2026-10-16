use super::*;
use crate::operation::RecordParts;
use ozzy_proto::MessageId;

fn fingerprint(id: u8, encoding: Encoding, parts: &[&[u8]]) -> RetryFingerprint {
    retry_fingerprint(RecordRef {
        encoding,
        message_id: MessageId::from_bytes([id; 16]),
        parts: RecordParts::Described(parts),
    })
    .unwrap()
}

#[test]
fn multipart_boundaries_empty_parts_identity_encoding_and_bytes_are_bound() {
    let base = fingerprint(1, Encoding::Raw, &[b"a", b"bc"]);
    for changed in [
        fingerprint(2, Encoding::Raw, &[b"a", b"bc"]),
        fingerprint(1, Encoding::Raw, &[b"ab", b"c"]),
        fingerprint(1, Encoding::Raw, &[b"abc"]),
        fingerprint(1, Encoding::Raw, &[b"a", b"bc", b""]),
        fingerprint(1, Encoding::Raw, &[b"a", b"bd"]),
        fingerprint(1, Encoding::Lz4 { decoded_bytes: 3 }, &[b"a", b"bc"]),
        fingerprint(1, Encoding::Lz4 { decoded_bytes: 4 }, &[b"a", b"bc"]),
    ] {
        assert_ne!(base, changed);
    }
    assert_ne!(
        fingerprint(1, Encoding::Raw, &[b""]),
        fingerprint(1, Encoding::Raw, &[b"", b""])
    );
    assert_eq!(RetryFingerprint::from_bytes(*base.as_bytes()), base);
}

#[test]
fn borrowed_and_owned_parts_have_the_same_fingerprint() {
    let owned = [
        bytes::Bytes::from_static(b"a"),
        bytes::Bytes::new(),
        bytes::Bytes::from_static(b"bc"),
    ];
    let actual = retry_fingerprint(RecordRef {
        encoding: Encoding::Raw,
        message_id: MessageId::from_bytes([1; 16]),
        parts: RecordParts::Owned(&owned),
    })
    .unwrap();
    assert_eq!(actual, fingerprint(1, Encoding::Raw, &[b"a", b"", b"bc"]));
}

#[test]
fn empty_identity_or_missing_parts_rejects_without_generating_evidence() {
    for record in [
        RecordRef {
            encoding: Encoding::Raw,
            message_id: MessageId::from_bytes([0; 16]),
            parts: RecordParts::Single(b"x"),
        },
        RecordRef {
            encoding: Encoding::Raw,
            message_id: MessageId::from_bytes([1; 16]),
            parts: RecordParts::Described(&[]),
        },
    ] {
        assert!(retry_fingerprint(record).is_err());
    }
}

#[test]
fn fingerprint_matches_independent_canonical_bytes() {
    let canonical = [
        &b"ozzy producer retry fingerprint\0"[..],
        &[1; 16],
        &[0],
        &2u64.to_be_bytes(),
        &1u64.to_be_bytes(),
        b"a",
        &2u64.to_be_bytes(),
        b"bc",
    ]
    .concat();
    assert_eq!(
        fingerprint(1, Encoding::Raw, &[b"a", b"bc"]).as_bytes(),
        blake3::hash(&canonical).as_bytes()
    );
}
