//! One free socket slot must eventually carry every continuously queued class.

use super::*;

#[test]
fn identical_control_retries_leave_room_for_distinct_announcements() {
    let voters = [1, 2, 3].map(|id| NodeId::from_bytes([id; 16]));
    let limits = QueueLimits {
        messages: 2,
        bytes: 128,
        message_bytes: 64,
    };
    let mut outbox = ReplicaOutbox::from_voters(&voters, voters[0], limits, limits).unwrap();
    let message =
        |value| Message::multipart([Bytes::copy_from_slice(&[value]), Bytes::new(), Bytes::new()]);
    for _ in 0..32 {
        outbox
            .try_enqueue(voters[1], SendClass::Control, message(1))
            .unwrap();
    }
    outbox
        .try_enqueue(voters[1], SendClass::Control, message(2))
        .unwrap();
    assert_eq!(outbox.queued(voters[1], SendClass::Control), Some((2, 34)));
    let mut sent = Vec::new();
    while outbox.has_pending() {
        outbox
            .flush_with(|message| {
                sent.push(message.part_bytes(1).unwrap()[0]);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [1, 2]);
    // Once submitted, a lost control still needs an actual retransmission.
    outbox
        .try_enqueue(voters[1], SendClass::Control, message(1))
        .unwrap();
    assert_eq!(outbox.queued(voters[1], SendClass::Control), Some((1, 17)));
}

#[test]
fn recurring_controls_cannot_starve_history_or_flow_with_one_send_slot() {
    let voters = [1, 2, 3].map(|id| NodeId::from_bytes([id; 16]));
    let limits = QueueLimits {
        messages: 2,
        bytes: 128,
        message_bytes: 64,
    };
    let classes = [
        SendClass::Control,
        SendClass::Exchange,
        SendClass::Receipt,
        SendClass::Data,
    ];
    let mut outbox = ReplicaOutbox::from_voters(&voters, voters[0], limits, limits).unwrap();
    let mut sent = [[0; 4]; 2];
    for round in 0..16 {
        for peer in &voters[1..] {
            for (tag, class) in classes.iter().enumerate() {
                if outbox.queued(*peer, *class).unwrap().0 == 0 {
                    outbox
                        .try_enqueue(
                            *peer,
                            *class,
                            Message::multipart([
                                Bytes::copy_from_slice(&[tag as u8]),
                                Bytes::new(),
                                Bytes::new(),
                            ]),
                        )
                        .unwrap();
                }
            }
        }
        // Unsuccessful readiness polls cannot spend another class's turn.
        for _ in 0..round % 5 {
            let progress = outbox
                .flush_with(|message| Err(TrySendError::Full(message)))
                .unwrap();
            assert!(!progress.advanced());
        }
        // A stalled peer is independent: peer 1 gets no capacity for eight rounds.
        let mut available = [round >= 8, true];
        outbox
            .flush_with(|message| {
                let route = message.part_bytes(0).unwrap();
                let peer = usize::from(route.as_ref() == voters[2].as_bytes());
                if !std::mem::take(&mut available[peer]) {
                    return Err(TrySendError::Full(message));
                }
                sent[peer][usize::from(message.part_bytes(1).unwrap()[0])] += 1;
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [[2; 4], [4; 4]]);
}
