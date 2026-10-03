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
    driver.admission.records[0].body = Body(Payload::from_bytes(Bytes::from_owner(Owner {
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
