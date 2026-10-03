use super::*;
use crate::{CheckpointError, CheckpointReference, checkpoint_name, open_checkpoint};
use ozzy_proto::CheckpointId;

fn id() -> CheckpointId {
    CheckpointId::from_bytes([0x77; 16])
}
fn schema() -> Digest {
    Digest::from_bytes([0x78; 32])
}
const STATE: &[u8] = b"checkpoint test state";

async fn commit(journal: &mut Journal) {
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    journal.install_metadata(next).await.unwrap();
}

async fn build(journal: &mut Journal) -> Result<CheckpointReference, DirectoryError> {
    journal
        .build_checkpoint(id(), schema(), 8, STATE)
        .await
        .map(|checkpoint| checkpoint.reference())
}

#[tokio::test]
async fn async_checkpoint_build_reuses_exact_artifacts_and_selects_legacy_readable_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("group");
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let mut journal = Journal::format(
        path.clone(),
        Local::new(clients.remove(0)),
        spec(CommitMode::External),
        JournalGeneration(1),
        limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        build(&mut journal).await,
        Err(DirectoryError::CheckpointAtGenesis)
    ));
    append_confirmed(&mut journal).await;
    commit(&mut journal).await;
    let first = build(&mut journal).await.unwrap();
    assert_eq!(build(&mut journal).await.unwrap(), first);
    assert!(journal.manifest.checkpoint.is_none());
    journal.install_checkpoint(id()).await.unwrap();
    assert_eq!(journal.manifest.checkpoint, Some(first));
    assert_eq!(
        journal.read_checkpoint_state().await.unwrap().unwrap(),
        STATE
    );
    assert!(matches!(
        journal.install_checkpoint(id()).await,
        Err(DirectoryError::CheckpointRegression)
    ));
    assert!(!journal.is_faulted());
    drop(journal);
    pool.shutdown().await;
    let legacy = GroupDirectory::open_with_configuration(
        &path,
        identity(),
        limits().metadata,
        b"test deployment",
    )
    .unwrap()
    .recover(JournalGeneration(2), limits().decode, limits().operations)
    .unwrap();
    assert_eq!(legacy.committed_position().unwrap(), first.position);
    let checkpoint = open_checkpoint(
        path.join("checkpoints").join(checkpoint_name(id())),
        identity().group_id,
        identity().store_id,
        limits().checkpoint,
    )
    .unwrap();
    assert_eq!(checkpoint.read_state(limits().checkpoint).unwrap(), STATE);
}

#[test]
fn conflicting_checkpoint_build_never_overwrites_the_existing_artifact() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    drive(&mut controller, build(&mut journal)).unwrap();
    let path = PathBuf::from("/group/checkpoints")
        .join(checkpoint_name(id()))
        .join("manifest");
    let before = controller.image().bytes(&path, false).unwrap().to_vec();
    let result = drive(
        &mut controller,
        journal.build_checkpoint(id(), schema(), 8, b"conflicting state"),
    );
    assert!(matches!(
        result,
        Err(DirectoryError::Checkpoint(
            CheckpointError::ImmutableConflict
        ))
    ));
    assert_eq!(controller.image().bytes(&path, false).unwrap(), before);
    assert!(journal.manifest.checkpoint.is_none());
}

#[test]
fn checkpoint_name_must_match_embedded_identity_before_selection() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    drive(&mut controller, build(&mut journal)).unwrap();
    let wrong = CheckpointId::from_bytes([0x79; 16]);
    drive(
        &mut controller,
        journal.access.done(Operation::Rename {
            source: journal
                .root()
                .join("checkpoints")
                .join(checkpoint_name(id())),
            destination: journal
                .root()
                .join("checkpoints")
                .join(checkpoint_name(wrong)),
        }),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, journal.install_checkpoint(wrong)),
        Err(DirectoryError::CheckpointMismatch)
    ));
    assert!(journal.manifest.checkpoint.is_none());
}

#[test]
fn checkpoint_build_and_selection_survive_every_execution_and_observation_cut() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, commit(&mut journal));
    drop(journal);
    let image = controller.crash(true).unwrap().0;
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..500 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(async {
                    build(&mut journal).await?;
                    journal.install_checkpoint(id()).await
                });
                let mut done = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        done = true;
                        break;
                    }
                    let (id, stage) = controller.jobs()[0];
                    match stage {
                        Stage::Queued => {
                            controller.execute(id, Effect::Normal).unwrap();
                            if immediate {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                done
            };
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 4)).unwrap();
            assert_eq!(journal.committed_position().unwrap().op_number, 1);
            let state = drive(&mut controller, journal.read_checkpoint_state()).unwrap();
            if let Some(state) = state {
                assert_eq!(state, STATE);
            } else {
                assert!(!done);
            }
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}
