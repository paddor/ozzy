use super::*;

#[tokio::test]
async fn typed_intake_stays_raw_until_the_sdk_groups_records() {
    let runtime = WriterRuntime::new().unwrap();
    let sdk = runtime.clone();
    let (mut writer, mut driver) = runtime
        .driver()
        .spawn(async move {
            let config = config(2, 8192);
            Shared::open_reserved(sdk, config, 2, None)
        })
        .await
        .unwrap();
    let large = writer
        .send(RecordInput::single(
            MessageId::new(),
            Bytes::from(vec![7; 4096]),
        ))
        .await
        .unwrap();
    let small = writer.send(input(3, b"tiny")).await.unwrap();
    driver.settle();
    assert_eq!(driver.inbox.used(), 2);
    assert_eq!(driver.admission.records.len(), 2);
    assert_eq!(
        driver.admission.records[0].encoding,
        ozzy_proto::data::Encoding::Raw
    );
    assert_eq!(driver.admission.records[1].body.bytes(), b"tiny");
    assert_eq!((large.sequence(), small.sequence()), (0, 1));
    confirm(&mut driver, 1, 100).unwrap();
    assert_eq!(large.confirmed().await.unwrap().offset, 100);
    assert!(small.try_confirmed().is_none());
    confirm(&mut driver, 2, 100).unwrap();
    assert_eq!(small.confirmed().await.unwrap().offset, 101);
}
