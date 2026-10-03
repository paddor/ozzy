use super::*;

#[tokio::test]
async fn tiny_multipart_views_preserve_empty_parts_after_confirmation_and_shutdown() {
    for bytes in [0, 1, 64, 128, 1024] {
        let (mut writer, mut driver) = channel(config(2, 2048));
        let parts = [Bytes::new(), Bytes::from(vec![7; bytes]), Bytes::new()];
        writer
            .send(RecordInput::multipart(MessageId::new(), parts))
            .await
            .unwrap();
        let record = driver.record(0).unwrap();
        assert_eq!(record.lengths, [0, bytes, 0]);
        let view = record.payload.as_bytes();
        confirm(&mut driver, 1, 0).unwrap();
        writer.send(input(9, b"new")).await.unwrap();
        drop(writer);
        drop(driver);
        drop(record);
        assert_eq!(view.as_ref(), vec![7; bytes]);
    }
}

#[tokio::test]
async fn driver_teardown_drops_unread_and_retained_bodies_with_live_producers() {
    use super::super::payload::Body;
    use omq_tokio::message::Payload;
    struct Owner {
        bytes: [u8; 256],
        _alive: Arc<()>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    let (mut writer, mut driver) = channel(config(4, 512));
    let pending = writer.send(input(1, b"placeholder")).await.unwrap();
    driver.settle();
    let alive = Arc::new(());
    let weak = Arc::downgrade(&alive);
    driver.admission.records[0].body =
        Body::Packed(Payload::from_bytes(Bytes::from_owner(Owner {
            bytes: [7; 256],
            _alive: alive,
        })));
    let transport = driver.record(0).unwrap().payload;
    writer.send(input(2, b"unread")).await.unwrap();
    drop(driver);
    assert!(matches!(
        pending.confirmed().await,
        Err(WriterError::Closed)
    ));
    assert_eq!(transport.as_slice(), &[7; 256]);
    drop(transport);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn inline_inputs_pack_under_load_and_singleton_aliases_survive_retirement() {
    use super::super::{batch::Batch, payload::Body};
    for bytes in [63, 64, 128] {
        let mut settings = config(2, 2 * bytes);
        settings.limits.max_records = 2;
        let (mut writer, mut driver) = channel(settings);
        for fill in [7, 8] {
            writer
                .send(RecordInput::copy_from_slice(
                    MessageId::from_bytes([fill; 16]),
                    &vec![fill; bytes],
                ))
                .await
                .unwrap();
        }
        driver.settle();
        let mut batch = Batch::new();
        assert!(
            batch
                .select(0, driver.config.limits, false, &mut driver)
                .unwrap()
        );
        // Bulk packing never materializes transport allocations per record.
        assert!(
            driver
                .admission
                .records
                .iter()
                .all(|record| matches!(record.body, Body::Inline(_)))
        );
        let group = batch.take_payload().unwrap().as_bytes();
        confirm(&mut driver, 2, 700).unwrap();
        writer
            .send(RecordInput::copy_from_slice(
                MessageId::from_bytes([9; 16]),
                &vec![9; bytes],
            ))
            .await
            .unwrap();
        assert!(
            batch
                .select(2, driver.config.limits, false, &mut driver)
                .unwrap()
        );
        let singleton = batch.take_payload().unwrap().as_bytes();
        assert!(
            batch
                .select(2, driver.config.limits, false, &mut driver)
                .unwrap()
        );
        let retry = batch.take_payload().unwrap();
        assert_eq!(retry.as_slice().as_ptr(), singleton.as_ptr());
        confirm(&mut driver, 3, 700).unwrap();
        drop((writer, driver));
        assert_eq!(&group[..bytes], vec![7; bytes]);
        assert_eq!(&group[bytes..], vec![8; bytes]);
        assert_eq!(singleton.as_ref(), vec![9; bytes]);
        assert_eq!(retry.as_slice(), singleton.as_ref());
    }
}
