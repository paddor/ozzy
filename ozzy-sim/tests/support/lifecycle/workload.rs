use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, CreatePartition, OpenProducer, RetentionPolicy,
};
use ozzy_proto::{
    MessageId, Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch,
    ProducerId, ProducerSequence,
};

use super::*;

pub(super) fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([10; 16])
}

impl Replica {
    /// Caller supplies a unique logical ID independently of consensus state.
    /// Assignment reads only this primary's speculative application image.
    pub(super) fn record_operations(&self, id: u128) -> Vec<Operation> {
        let scope = self.driver.as_ref().unwrap().scope();
        let state = self.images.speculative().partition(partition());
        let mut operations = Vec::new();
        let mut previous = tail(&self.accepted);
        if state.is_none() {
            for body in [
                OperationBody::CreatePartition(CreatePartition {
                    partition: partition(),
                    stream: "events",
                    topic: "orders",
                    partition_id: PartitionId::new(0),
                    owner_epoch: OwnerEpoch::new(1),
                    retention: RetentionPolicy::default(),
                }),
                OperationBody::OpenProducer(OpenProducer {
                    partition: partition(),
                    producer_id: ProducerId::from_bytes([11; 16]),
                    expected_epoch: None,
                    new_epoch: ProducerEpoch::new(1),
                    operation_id: OperationId::from_bytes([12; 16]),
                }),
            ] {
                let operation = Operation::new(scope, previous, &body);
                previous = operation.prefix();
                operations.push(operation);
            }
        }
        let payload = format!(
            "{{\"order_id\":{id},\"sku\":\"sku-{}\",\"quantity\":{},\"paid\":true}}",
            id % 37,
            id % 7 + 1
        );
        let binary = id.to_be_bytes();
        let body = OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: partition(),
                owner_epoch: OwnerEpoch::new(1),
                producer_id: ProducerId::from_bytes([11; 16]),
                producer_epoch: ProducerEpoch::new(1),
                first_sequence: state.map_or(ProducerSequence::new(0), |state| {
                    state
                        .producer(ProducerId::from_bytes([11; 16]))
                        .unwrap()
                        .next_producer_sequence
                }),
                first_offset: state.map_or(Offset::new(0), |state| state.next_offset),
                append_timestamp_millis: 123,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes(id.to_be_bytes()),
                    parts: vec![
                        b"order.created".as_slice(),
                        payload.as_bytes(),
                        &binary,
                        &[],
                    ]
                    .into(),
                }]
                .into(),
            }],
        });
        operations.push(Operation::new(scope, previous, &body));
        operations
    }
}
