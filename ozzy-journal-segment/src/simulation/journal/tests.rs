use super::*;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

mod publication;

fn voter() -> Journal {
    Journal::format(
        GroupIdentity {
            group_id: GroupId::from_bytes([1; 16]),
            replica_node_id: NodeId::from_bytes([2; 16]),
            volume_id: VolumeId::from_bytes([3; 16]),
            store_id: StoreId::from_bytes([4; 16]),
            store_generation: 1,
        },
        vec![5; 32],
        1,
        JournalGeneration(1),
    )
    .unwrap()
}
