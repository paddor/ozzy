use std::fs;
use std::sync::{Arc, Barrier};

use ozzy_journal_segment::{
    CheckpointError, CheckpointLimits, CheckpointSpec, Digest, LogPosition, build_checkpoint,
    checkpoint_name, decode_checkpoint_manifest, open_checkpoint,
};
use ozzy_proto::{CheckpointId, GroupId, StoreId};
use tempfile::TempDir;

fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

fn spec(checkpoint_id: CheckpointId) -> CheckpointSpec {
    CheckpointSpec {
        group_id: GroupId::from_bytes([0x11; 16]),
        store_id: StoreId::from_bytes([0x22; 16]),
        checkpoint_id,
        position: LogPosition {
            op_number: 19,
            digest: digest(0x33),
        },
        configuration_epoch: 4,
        source_manifest_generation: 7,
        source_manifest_digest: digest(0x44),
        state_schema_digest: digest(0x55),
        chunk_bytes: 7,
    }
}

fn directories() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temporary = TempDir::new().unwrap();
    let checkpoints = temporary.path().join("checkpoints");
    let staging = temporary.path().join("staging");
    fs::create_dir(&checkpoints).unwrap();
    fs::create_dir(&staging).unwrap();
    (temporary, checkpoints, staging)
}

#[test]
fn chunked_checkpoint_round_trips_and_reuses_exact_artifact() {
    let (_temporary, checkpoints, staging) = directories();
    let checkpoint_id = CheckpointId::from_bytes([0x66; 16]);
    let state = b"canonical checkpoint state";
    let built = build_checkpoint(
        &checkpoints,
        &staging,
        spec(checkpoint_id),
        state,
        CheckpointLimits::default(),
    )
    .unwrap();
    assert_eq!(built.manifest().chunks.len(), 4);
    assert_eq!(
        built.read_state(CheckpointLimits::default()).unwrap(),
        state
    );

    let reopened = open_checkpoint(
        checkpoints.join(checkpoint_name(checkpoint_id)),
        spec(checkpoint_id).group_id,
        spec(checkpoint_id).store_id,
        CheckpointLimits::default(),
    )
    .unwrap();
    assert_eq!(reopened, built);
    assert_eq!(
        build_checkpoint(
            &checkpoints,
            &staging,
            spec(checkpoint_id),
            state,
            CheckpointLimits::default(),
        )
        .unwrap(),
        built
    );
    assert!(matches!(
        build_checkpoint(
            &checkpoints,
            &staging,
            spec(checkpoint_id),
            b"different state",
            CheckpointLimits::default(),
        ),
        Err(CheckpointError::ImmutableConflict)
    ));
}

#[test]
fn concurrent_identical_checkpoint_builds_reuse_one_artifact() {
    let (_temporary, checkpoints, staging) = directories();
    let checkpoint_id = CheckpointId::from_bytes([0x6a; 16]);
    let start = Arc::new(Barrier::new(2));
    let builders = (0..2)
        .map(|_| {
            let checkpoints = checkpoints.clone();
            let staging = staging.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                build_checkpoint(
                    checkpoints,
                    staging,
                    spec(checkpoint_id),
                    b"same canonical state",
                    CheckpointLimits::default(),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let images = builders
        .into_iter()
        .map(|builder| builder.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(images[0], images[1]);
    assert_eq!(fs::read_dir(&staging).unwrap().count(), 0);
}

#[test]
fn manifest_decoder_rejects_every_truncation_and_corruption() {
    let (_temporary, checkpoints, staging) = directories();
    let checkpoint_id = CheckpointId::from_bytes([0x67; 16]);
    let built = build_checkpoint(
        &checkpoints,
        &staging,
        spec(checkpoint_id),
        b"state split into chunks",
        CheckpointLimits::default(),
    )
    .unwrap();
    let manifest = fs::read(built.root().join("manifest")).unwrap();
    for length in 0..manifest.len() {
        assert!(
            decode_checkpoint_manifest(&manifest[..length], CheckpointLimits::default()).is_err()
        );
    }
    let mut corrupt = manifest;
    corrupt[128] ^= 1;
    assert!(matches!(
        decode_checkpoint_manifest(&corrupt, CheckpointLimits::default()),
        Err(CheckpointError::DigestMismatch("checkpoint manifest"))
    ));
}

#[test]
fn open_hashes_chunks_and_rejects_missing_or_extra_files() {
    let (_temporary, checkpoints, staging) = directories();
    let checkpoint_id = CheckpointId::from_bytes([0x68; 16]);
    let built = build_checkpoint(
        &checkpoints,
        &staging,
        spec(checkpoint_id),
        b"abcdefghijklmnop",
        CheckpointLimits::default(),
    )
    .unwrap();
    let first_chunk = built.root().join("CHUNK.00000000");
    let mut bytes = fs::read(&first_chunk).unwrap();
    bytes[0] ^= 1;
    fs::write(&first_chunk, bytes).unwrap();
    assert!(matches!(
        open_checkpoint(
            built.root(),
            spec(checkpoint_id).group_id,
            spec(checkpoint_id).store_id,
            CheckpointLimits::default(),
        ),
        Err(CheckpointError::DigestMismatch("checkpoint chunk"))
    ));

    fs::remove_dir_all(built.root()).unwrap();
    let built = build_checkpoint(
        &checkpoints,
        &staging,
        spec(checkpoint_id),
        b"abcdefghijklmnop",
        CheckpointLimits::default(),
    )
    .unwrap();
    fs::remove_file(built.root().join("CHUNK.00000001")).unwrap();
    assert!(matches!(
        open_checkpoint(
            built.root(),
            spec(checkpoint_id).group_id,
            spec(checkpoint_id).store_id,
            CheckpointLimits::default(),
        ),
        Err(CheckpointError::MissingChunk)
    ));

    fs::write(built.root().join("unexpected"), b"x").unwrap();
    assert!(matches!(
        open_checkpoint(
            built.root(),
            spec(checkpoint_id).group_id,
            spec(checkpoint_id).store_id,
            CheckpointLimits::default(),
        ),
        Err(CheckpointError::UnexpectedFile)
    ));
}

#[test]
fn checkpoint_limits_are_enforced_before_publication() {
    let (_temporary, checkpoints, staging) = directories();
    let checkpoint_id = CheckpointId::from_bytes([0x69; 16]);
    let limits = CheckpointLimits {
        max_state_bytes: 3,
        ..CheckpointLimits::default()
    };
    assert!(matches!(
        build_checkpoint(&checkpoints, &staging, spec(checkpoint_id), b"four", limits,),
        Err(CheckpointError::LimitExceeded {
            kind: "checkpoint state bytes",
            ..
        })
    ));
    assert!(fs::read_dir(checkpoints).unwrap().next().is_none());
}
