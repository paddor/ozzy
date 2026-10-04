use super::*;
use crate::replica_journal::ShardJournalConfig;
use std::path::Path;

#[test]
fn automatic_retention_reuses_a_checkpoint_without_an_intervening_append() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = seed(&mut controller, io, local_config());
    let body = OperationBody::PartitionPolicy(PartitionPolicy {
        partition: partition(),
        expected_revision: 1,
        new_revision: 2,
        retention: RetentionPolicy {
            max_age_millis: None,
            max_bytes: NonZeroU64::new(32768),
        },
        operation_id: OperationId::from_bytes([97; 16]),
    });
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .push(
            body.kind(),
            &encode_operation_body(&body, journal.limits.operations).unwrap(),
        )
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let ticket = driver.begin_validation().unwrap();
    let selected = retire(&mut controller, &mut journal, ticket, 92, &[1]);
    let mut adapter = journal
        .into_shard_journal(ShardJournalConfig::default(), || 999)
        .unwrap();
    for id in [93, 94, 95] {
        let mut completion = Box::pin(
            adapter
                .retention_turn(ticket, OperationId::from_bytes([id; 16]), true)
                .unwrap(),
        );
        let turn = drive(
            &mut controller,
            std::future::poll_fn(|cx| {
                assert!(
                    adapter.poll_stopped(cx).is_pending(),
                    "retention must not fence the journal"
                );
                completion.as_mut().poll(cx)
            }),
        )
        .unwrap();
        assert!(turn.enabled);
        assert!(turn.proposal.is_none());
        let image = controller.image();
        let current = ozzy_journal_segment::decode_current(
            image.bytes(Path::new("/local/CURRENT"), false).unwrap(),
        )
        .unwrap();
        let manifest = ozzy_journal_segment::decode_manifest(
            image
                .bytes(
                    &Path::new("/local").join(format!("MANIFEST.{}", current.generation)),
                    false,
                )
                .unwrap(),
            ozzy_journal_segment::MetadataLimits::default(),
        )
        .unwrap();
        assert_eq!(manifest.checkpoint, Some(selected));
        assert_eq!(
            manifest
                .segments
                .iter()
                .map(|r| r.segment_id)
                .collect::<Vec<_>>(),
            [3]
        );
        assert_eq!(prefix(manifest.accepted), ticket.accepted());
        assert_eq!(prefix(manifest.committed), ticket.committed());
    }
    drive(&mut controller, adapter.shutdown()).unwrap();
}
