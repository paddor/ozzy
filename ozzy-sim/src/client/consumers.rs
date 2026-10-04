//! Bounded slow readers retain independent evidence and returned backing aliases.
use super::{Client, TopicReader};
use ozzy_proto::{NodeId, handshake};
use ozzy_runtime::replicated::{
    BrokerLinks, ReaderStart, TopicCheckpoint, TopicReaderConfig, WriterRuntime,
};
use std::time::Duration;

impl Client {
    /// Pause a second consumer through a burst, then verify PEER repair and
    /// returned payload aliases while consuming slowly and after closing it.
    pub async fn slow_consumer(
        &mut self,
        reader: &mut TopicReader,
        wave: usize,
    ) -> (usize, u64, u64) {
        let positions = self.positions();
        let (links, mut paused) = self.consumer(None).await;
        {
            let mut next = std::pin::pin!(paused.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        let pending = self.queue_varied(wave * 6 + 2).await;
        let records = pending.len();
        self.confirm(pending).await;
        self.read(reader, positions.clone()).await;
        let mut positions = positions;
        let mut aliases = Vec::new();
        for _ in 0..records {
            let record = paused.next().await.unwrap();
            self.verify_record(&record, &mut positions);
            if aliases.len() < self.keys.len() {
                let expected = self.history[record.partition as usize]
                    [record.offset.get() as usize - self.bases[record.partition as usize]]
                    .1
                    .clone();
                aliases.push((record.payload.to_vec(), expected));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(positions, self.positions());
        let stats = paused.stats();
        assert!(
            stats.replayed_records > 0,
            "paused consumer never exercised PEER repair"
        );
        paused.close().await.unwrap();
        links.shutdown().await.unwrap();
        for (actual, expected) in aliases {
            assert_eq!(actual, expected);
        }
        (records, stats.replayed_records, stats.live_records)
    }

    async fn consumer(&self, selected: Option<Vec<u32>>) -> (BrokerLinks, TopicReader) {
        let runtime = WriterRuntime::with_context(self.runtime.context().clone()).unwrap();
        let mut config = self.links_config.clone();
        config.local = NodeId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        config.parameters.roles = handshake::CONSUMER;
        config.append = None;
        let links = BrokerLinks::connect(&runtime, config).await.unwrap();
        let reader = TopicReader::open(
            links.clone(),
            "orders",
            TopicReaderConfig {
                start: ReaderStart::Checkpoint(TopicCheckpoint {
                    topic: self.writer.metadata().id(),
                    positions: self
                        .positions()
                        .iter()
                        .enumerate()
                        .filter(|(number, _)| {
                            selected
                                .as_ref()
                                .is_none_or(|partitions| partitions.contains(&(*number as u32)))
                        })
                        .map(|(number, &offset)| {
                            (number as u32, ozzy_proto::Offset::new(offset as u64))
                        })
                        .collect(),
                }),
                partitions: selected,
                ..TopicReaderConfig::default()
            },
        )
        .await
        .unwrap();
        (links, reader)
    }

    /// Keep a saved consumer checkpoint behind rolling retention while another
    /// reader verifies bounded cohorts. Reopening must report the explicit gap.
    pub async fn retention_lag(&mut self, reader: &mut TopicReader, wave: usize) -> usize {
        let old = self.positions()[0];
        let (links, mut lagging) = self.consumer(Some(vec![0])).await;
        {
            let mut next = std::pin::pin!(lagging.next());
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        let mut records = 0;
        for chunk in 0..256 {
            let positions = self.positions();
            let number = u32::try_from(wave.checked_mul(256).unwrap() + chunk).unwrap();
            let pending = self.queue_large(number, 64).await;
            records += pending.len();
            self.confirm(pending).await;
            self.read(reader, positions).await;
            self.discard_verified();
            if self.retained_floor().await.get() <= old as u64 {
                continue;
            }
            lagging.close().await.unwrap();
            let mut resumed = TopicReader::open(
                links.clone(),
                "orders",
                TopicReaderConfig {
                    partitions: Some(vec![0]),
                    start: ReaderStart::Checkpoint(TopicCheckpoint {
                        topic: self.writer.metadata().id(),
                        positions: vec![(0, ozzy_proto::Offset::new(old as u64))],
                    }),
                    ..TopicReaderConfig::default()
                },
            )
            .await
            .unwrap();
            match resumed.next().await {
                Err(ozzy_runtime::replicated::TopicReaderError::RetentionGap {
                    partition,
                    earliest,
                }) => {
                    assert_eq!(partition, 0);
                    assert!(
                        earliest.get() > old as u64 && earliest.get() <= self.positions()[0] as u64
                    );
                }
                result => panic!("expired checkpoint did not report a retention gap: {result:?}"),
            }
            resumed.close().await.unwrap();
            links.shutdown().await.unwrap();
            return records;
        }
        panic!("bounded retention workload never retired the lagging checkpoint");
    }
}
