use super::*;
use ozzy_proto::{NodeId, append::Policy, directory::BrokerEndpoint};

fn page(first: u32, count: u32) -> directory::TopicPage {
    let node = NodeId::from_bytes([1; 16]);
    directory::TopicPage {
        id: TopicId::from_bytes([2; 16]),
        name: "orders".to_owned(),
        partitioner_seed: 3,
        total: 4,
        policy: Policy::LocalDurable,
        brokers: vec![BrokerEndpoint {
            node,
            peer: "inproc://peer".to_owned(),
            reader_pub: "inproc://readers".to_owned(),
            follower_pub: None,
        }],
        first,
        partitions: (first..first + count)
            .map(|number| directory::TopicPartition {
                number,
                group: GroupId::from_bytes([number as u8 + 1; 16]),
                config_epoch: 1,
                incarnation: PartitionIncarnation::from_bytes([number as u8 + 20; 16]),
                members: vec![node],
            })
            .collect(),
    }
}

#[test]
fn serves_arbitrary_ranges_and_shrinks_to_reply_limit() {
    let catalog = TopicCatalog::new([page(0, 2), page(2, 2)], 1, 4).unwrap();
    let request = directory::TopicRequest {
        name: "orders".to_owned(),
        first: 1,
        maximum: 4,
    };
    let first = catalog.page(&request, 4096).unwrap();
    assert_eq!(first.first, 1);
    assert_eq!(first.partitions.len(), 1);
    assert_eq!(first.total, 4);
    let last_request = directory::TopicRequest {
        first: 2,
        ..request
    };
    let last = catalog.page(&last_request, 4096).unwrap();
    assert_eq!(last.partitions.len(), 2);
    let shrunk = catalog
        .page(&last_request, catalog.largest_single_bytes())
        .unwrap();
    assert_eq!(shrunk.partitions.len(), 1);
    assert_eq!(
        catalog.page(&last_request, catalog.largest_single_bytes() - 1),
        Err(CatalogError::Size)
    );
}

#[test]
fn rejects_gaps_duplicate_groups_and_changed_topic_header() {
    assert!(matches!(
        TopicCatalog::new([page(0, 2), page(3, 1)], 1, 4),
        Err(CatalogError::Invalid)
    ));
    let mut duplicate = page(2, 2);
    duplicate.partitions[0].group = page(0, 2).partitions[0].group;
    assert!(matches!(
        TopicCatalog::new([page(0, 2), duplicate], 1, 4),
        Err(CatalogError::Invalid)
    ));
    let mut changed = page(2, 2);
    changed.partitioner_seed += 1;
    assert!(matches!(
        TopicCatalog::new([page(0, 2), changed], 1, 4),
        Err(CatalogError::Invalid)
    ));
}
