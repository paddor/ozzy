use super::*;
use bytes::Bytes;
use ozzy_proto::{EnvelopeLimits, MessageId, PartitionIncarnation, ProducerId};
use std::sync::Arc;
use std::time::Duration;

mod batching;
mod payload;
mod preparation;
mod producers;
mod progress;

fn config(records: usize, bytes: usize) -> WriterConfig {
    WriterConfig {
        compress_payloads: true,
        batch_target_bytes: 4 * 1024 * 1024,
        policy: Policy::QuorumReplicatedPersisting,
        partition: PartitionIncarnation::from_bytes([1; 16]),
        owner_epoch: 1,
        producer_id: ProducerId::from_bytes([2; 16]),
        producer_epoch: 1,
        next_sequence: 0,
        limits: DataLimits {
            max_record_bytes: bytes,
            envelope: EnvelopeLimits {
                max_metadata_bytes: 512,
                max_payload_bytes: bytes,
            },
            max_records: 1,
            max_parts: 4,
        },
        max_producers: 1,
        inflight_appends: records,
    }
}

fn channel(config: WriterConfig) -> (Writer, state::Driver) {
    let capacity = config.inflight_appends;
    Shared::channel_with_capacity(config, capacity)
}

// Direct-driver tests model request preparation before publishing confirmation.
fn confirm(driver: &mut state::Driver, end: u64, base: u64) -> Result<usize, WriterError> {
    driver.settle();
    let first = driver.progress.confirmed().min(end.saturating_sub(1));
    let offset = base.checked_add(first).ok_or(super::Error::Response)?;
    let result = driver.confirm(first, end, offset)?;
    driver.stage(end);
    Ok(result)
}

fn input(id: u8, body: &'static [u8]) -> RecordInput {
    RecordInput::multipart(MessageId::from_bytes([id; 16]), [Bytes::from_static(body)])
}

#[tokio::test]
async fn final_sequence_is_admitted_once_without_wrapping_or_losing_confirmation() {
    for producers in [1, 2] {
        let mut settings = config(2, 64);
        settings.max_producers = producers;
        settings.next_sequence = MAX_SEQUENCE;
        let (mut writer, mut driver) = channel(settings);
        if producers == 1 {
            assert!(matches!(
                writer.try_clone(),
                Err(WriterError::ProducerLimit)
            ));
        }
        let last = writer.send(input(1, b"last")).await.unwrap();
        assert_eq!(last.sequence(), MAX_SEQUENCE);
        assert!(matches!(
            writer.send(input(2, b"overflow")).await,
            Err(WriterError::Configuration)
        ));
        assert_eq!(driver.next.load(Ordering::Acquire), MAX_SEQUENCE + 1);
        confirm(&mut driver, MAX_SEQUENCE + 1, 0).unwrap();
        assert_eq!(last.confirmed().await.unwrap().offset, MAX_SEQUENCE);
    }
}

#[tokio::test]
async fn compact_pending_handles_preserve_writer_identity_after_owner_drop() {
    assert!(size_of::<PendingRecord>() <= 32);
    let mut settings = config(2, 64);
    settings.owner_epoch = 7;
    settings.producer_epoch = 11;
    settings.next_sequence = 91;
    let (mut writer, mut shared) = channel(settings.clone());
    let pending = writer.send(input(9, b"body")).await.unwrap();
    assert_eq!(pending.sequence(), 91);
    confirm(&mut shared, 92, 100).unwrap();
    drop(writer);
    drop(shared);
    let expected = RecordReceipt {
        partition: settings.partition,
        owner_epoch: settings.owner_epoch,
        key: AppendKey {
            producer_id: settings.producer_id,
            producer_epoch: settings.producer_epoch,
            first_sequence: 91,
        },
        message_id: MessageId::from_bytes([9; 16]),
        offset: 191,
        policy: settings.policy,
    };
    assert_eq!(pending.confirmed().await.unwrap(), expected);
    assert_eq!(pending.confirmed().await.unwrap(), expected);
}

#[test]
fn startup_rejects_invalid_identity_bounds_and_sequence_exhaustion() {
    let valid = config(4, 8);
    assert!(valid.parameters_with_local_group(false).is_ok());
    for invalid in [
        WriterConfig {
            inflight_appends: 0,
            ..valid.clone()
        },
        WriterConfig {
            max_producers: 0,
            ..valid.clone()
        },
        WriterConfig {
            batch_target_bytes: usize::MAX,
            ..valid.clone()
        },
        WriterConfig {
            owner_epoch: 0,
            ..valid.clone()
        },
        WriterConfig {
            producer_epoch: 0,
            ..valid.clone()
        },
        WriterConfig {
            partition: PartitionIncarnation::from_bytes([0; 16]),
            ..valid.clone()
        },
        WriterConfig {
            producer_id: ProducerId::from_bytes([0; 16]),
            ..valid.clone()
        },
        WriterConfig {
            next_sequence: u64::MAX,
            ..valid.clone()
        },
        WriterConfig {
            limits: DataLimits {
                max_records: 0,
                ..valid.limits
            },
            ..valid
        },
    ] {
        assert!(matches!(
            invalid.parameters_with_local_group(false),
            Err(WriterError::Configuration)
        ));
    }
}

#[tokio::test]
async fn independent_handles_and_flush_capture_do_not_retain_payloads() {
    let (mut writer, mut shared) = channel(config(4, 64));
    let first = writer.send(input(3, b"first")).await.unwrap();
    let checkpoint = writer.flush();
    let second = writer.send(input(4, b"second")).await.unwrap();
    assert_eq!(first.sequence(), 0);
    assert_eq!(second.sequence(), 1);
    confirm(&mut shared, 1, 90).unwrap();
    checkpoint.await.unwrap();
    let receipt = first.confirmed().await.unwrap();
    assert_eq!(receipt.offset, 90);
    assert_eq!(receipt.message_id, MessageId::from_bytes([3; 16]));
    confirm(&mut shared, 2, 90).unwrap();
    assert_eq!(second.confirmed().await.unwrap().offset, 91);
}

#[tokio::test]
async fn invalid_record_and_canceled_admission_do_not_consume_sequence() {
    let (mut writer, mut shared) = channel(config(1, 8));
    for invalid in [
        RecordInput::multipart(MessageId::new(), []),
        input(0, b"zero"),
        input(4, b"too many bytes"),
        RecordInput::multipart(MessageId::new(), vec![Bytes::new(); 5]),
    ] {
        assert!(matches!(
            writer.send(invalid).await,
            Err(WriterError::RecordLimits)
        ));
    }
    let first = writer.send(input(3, b"12345678")).await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(1),
            writer.send(input(4, b"x"))
        )
        .await
        .is_err()
    );
    confirm(&mut shared, 1, 0).unwrap();
    first.confirmed().await.unwrap();
    assert_eq!(writer.send(input(5, b"y")).await.unwrap().sequence(), 1);
}

#[tokio::test]
async fn confirmed_prefix_survives_failure_and_last_owner_drop() {
    let (mut writer, mut shared) = channel(config(2, 8));
    let first = writer.send(input(3, b"a")).await.unwrap();
    let second = writer.send(input(4, b"b")).await.unwrap();
    confirm(&mut shared, 1, 10).unwrap();
    drop(writer);
    assert_eq!(first.confirmed().await.unwrap().offset, 10);
    assert!(matches!(second.confirmed().await, Err(WriterError::Closed)));
    assert!(shared.stop.is_closed());
}

#[tokio::test]
async fn transport_owned_payload_does_not_hold_inbox_capacity() {
    let (mut writer, mut shared) = channel(config(1, 8));
    let first = writer.send(input(3, b"abc")).await.unwrap();
    let transport = shared.record(0).unwrap().payload;
    confirm(&mut shared, 1, 0).unwrap();
    first.confirmed().await.unwrap();
    assert_eq!(writer.send(input(4, b"x")).await.unwrap().sequence(), 1);
    assert_eq!(transport.as_slice(), b"abc");
    drop(transport);
}

#[tokio::test]
async fn single_part_reuses_storage_until_last_transport_reference_drops() {
    let (mut writer, mut shared) = channel(config(1, 1024));
    let bytes = Bytes::from(vec![7; 1024]);
    let pointer = bytes.as_ptr();
    let pending = writer
        .send(RecordInput::multipart(
            MessageId::from_bytes([3; 16]),
            [bytes],
        ))
        .await
        .unwrap();
    let transport = shared.record(0).unwrap().payload;
    assert_eq!(
        transport.as_slice().as_ptr(),
        pointer,
        "owned body must not be copied"
    );
    assert_eq!(transport.as_slice(), &[7; 1024]);
    confirm(&mut shared, 1, 0).unwrap();
    pending.confirmed().await.unwrap();
    drop(transport);
    assert_eq!(writer.send(input(4, b"next")).await.unwrap().sequence(), 1);
}

#[tokio::test]
async fn admission_releases_custom_backing_storage_of_small_slices() {
    struct LargeBody {
        bytes: Vec<u8>,
        _lifetime: Arc<()>,
    }
    impl AsRef<[u8]> for LargeBody {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    let lifetime = Arc::new(());
    let weak = Arc::downgrade(&lifetime);
    let bytes = Bytes::from_owner(LargeBody {
        bytes: vec![9; 1024 * 1024],
        _lifetime: lifetime,
    })
    .slice(8..16);
    let (mut writer, mut shared) = channel(config(1, 8));
    let pending = writer
        .send(RecordInput::multipart(
            MessageId::from_bytes([3; 16]),
            [bytes],
        ))
        .await
        .unwrap();
    assert!(weak.upgrade().is_none(), "large source must be released");
    assert_eq!(shared.record(0).unwrap().payload.as_slice(), &[9; 8]);
    confirm(&mut shared, 1, 0).unwrap();
    pending.confirmed().await.unwrap();
}

#[tokio::test]
async fn empty_records_still_charge_record_capacity_and_wake_all_waiters() {
    let (mut writer, mut shared) = channel(config(1, 8));
    let first = writer.send(input(3, b"")).await.unwrap();
    let checkpoint = writer.flush();
    let (receipt, (), next) = tokio::join!(
        first.confirmed(),
        async {
            tokio::task::yield_now().await;
            confirm(&mut shared, 1, 0).unwrap();
        },
        writer.send(input(4, b"")),
    );
    assert_eq!(receipt.unwrap().offset, 0);
    assert_eq!(next.unwrap().sequence(), 1);
    checkpoint.await.unwrap();
}
