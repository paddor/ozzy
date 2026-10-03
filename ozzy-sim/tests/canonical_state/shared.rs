use super::*;

const WRITERS: usize = 4;
const BOOTSTRAP: u64 = WRITERS as u64 + 1;

fn writer_id(writer: usize) -> ProducerId {
    ProducerId::from_bytes([writer as u8 + 1; 16])
}

fn opening(writer: usize) -> OperationBody<'static> {
    let OperationBody::OpenProducer(mut body) = open() else {
        unreachable!()
    };
    body.producer_id = writer_id(writer);
    body.operation_id = OperationId::from_bytes([writer as u8 + 0x30; 16]);
    OperationBody::OpenProducer(body)
}

// Plain record-order oracle. No canonical cursor, retry span or snapshot code.
fn record(writers: &[usize], offset: usize) -> OperationBody<'static> {
    let writer = writers[offset];
    let sequence = writers[..offset].iter().filter(|&&id| id == writer).count();
    let OperationBody::Append(mut body) = append(offset as u64, offset as u8 + 1) else {
        unreachable!()
    };
    body.batches[0].producer_id = writer_id(writer);
    body.batches[0].first_sequence = ProducerSequence::new(sequence as u64);
    OperationBody::Append(body)
}

fn recover(writers: &[usize], committed: usize) -> CanonicalSimulation {
    let mut recovery = CanonicalRecovery::new(StateLimits::default(), 16, 256);
    recovery.apply(1, &create(), true).unwrap();
    for writer in 0..WRITERS {
        recovery
            .apply(writer as u64 + 2, &opening(writer), true)
            .unwrap();
    }
    for offset in 0..writers.len() {
        recovery
            .apply(
                BOOTSTRAP + offset as u64 + 1,
                &record(writers, offset),
                offset < committed,
            )
            .unwrap();
    }
    recovery.finish().unwrap()
}

fn check(state: &CanonicalState, writers: &[usize]) {
    let partition = state.partition(partition()).unwrap();
    assert_eq!(partition.next_offset.get(), writers.len() as u64);
    for writer in 0..WRITERS {
        let session = partition.producer(writer_id(writer)).unwrap();
        let offsets = writers
            .iter()
            .enumerate()
            .filter_map(|(offset, &id)| (id == writer).then_some(offset))
            .collect::<Vec<_>>();
        assert_eq!(session.next_producer_sequence.get(), offsets.len() as u64);
        for (sequence, offset) in offsets.iter().enumerate() {
            assert_eq!(
                session.result_offset(ProducerSequence::new(sequence as u64)),
                Some(Offset::new(*offset as u64))
            );
        }
        assert_eq!(
            session.result_offset(ProducerSequence::new(offsets.len() as u64)),
            None
        );
    }
}

#[test]
fn interleaved_writer_oracle_survives_delayed_commit_suffix_loss_and_restart() {
    for seed in 1_u64..=32 {
        let mut random = seed;
        let mut writers = Vec::new();
        let mut committed = 0;
        let mut sim = recover(&writers, committed);
        for _ in 0..192 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            match (random >> 32) % 8 {
                0 => {
                    committed = writers.len();
                    sim.commit_through(BOOTSTRAP + committed as u64).unwrap();
                }
                1 => {
                    writers.truncate(committed);
                    sim.discard_suffix();
                }
                2 => sim = recover(&writers, committed),
                _ => {
                    writers.push((random >> 48) as usize % WRITERS);
                    let offset = writers.len() - 1;
                    sim.admit(BOOTSTRAP + offset as u64 + 1, &record(&writers, offset))
                        .unwrap();
                }
            }
            check(sim.speculative(), &writers);
            check(sim.committed(), &writers[..committed]);
        }
    }
}
