//! Group activation through native `GroupAssign` requests.

use super::*;

fn grouped(peer: u8, session: u8) -> Link {
    Link {
        remote: handshake::Parameters::group(limits(), handshake::CONSUMER).unwrap(),
        ..link(peer, session)
    }
}

fn assign_message(request: ozzy_proto::group::AssignRequest, link: Link, id: u8) -> Message {
    let mut metadata = Vec::with_capacity(ozzy_proto::group::ASSIGN_BYTES);
    let header = ozzy_proto::group::encode_assign(
        envelope(Opcode::GroupAssign, link, id),
        request,
        &mut metadata,
        limits().envelope,
    )
    .unwrap();
    crate::native_frames::message(
        link.binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::new(),
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn native_activation_allocates_epochs_captures_cursors_and_fences_older_plans() {
    use ozzy_proto::group::{self, AssignRequest, Start};
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(&mut controller, io, History::Available);
    let hint = actor.authority_hint();
    let binding = grouped(70, 80);
    let request = AssignRequest {
        authority: hint.authority,
        partition: partition(),
        group: assignment().group,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        plan_revision: 2,
        initial: Start::Earliest,
        policy: Policy::LocalDurable,
    };
    let mut output = Vec::new();
    assert_eq!(
        intake
            .receive(&assign_message(request, binding, 90), binding, hint)
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, false);
    }
    assert!(output.is_empty(), "activation waits for durability");
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let activated = group::decode_assigned(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!(
        (
            activated.epoch,
            activated.plan_revision,
            activated.cursor,
            activated.retained_from,
            activated.end
        ),
        (2, 2, Offset::ZERO, Offset::ZERO, Offset::new(3))
    );
    assert_eq!(activated.member, request.member);
    let confirmed_prefix = actor.snapshot().accepted;
    output.clear();

    // Old-epoch progress is fenced; the new owner acknowledges under epoch 2.
    assert_eq!(
        intake
            .receive(
                &message(hint.authority, consumer(70, 80), Some(2), 91),
                consumer(70, 80),
                hint
            )
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(
        ozzy_proto::nack::decode(packet(&output[0]), limits().envelope)
            .unwrap()
            .code,
        6
    );
    output.clear();
    let new_owner = wire::Request {
        assignment: wire::Assignment {
            member: request.member.unwrap(),
            epoch: 2,
            ..assignment()
        },
        ..wire::Request {
            authority: hint.authority,
            assignment: assignment(),
            policy: Policy::LocalDurable,
            cursor: Some(Offset::new(2)),
        }
    };
    assert_eq!(
        intake
            .receive(
                &with_request(new_owner, consumer(70, 80), 92),
                consumer(70, 80),
                hint
            )
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(
        wire::decode_confirmed(packet(&output[0]), limits().envelope)
            .unwrap()
            .cursor,
        Offset::new(2)
    );
    output.clear();

    // A repeated revision returns the captured activation, not newer progress.
    let before = actor.snapshot().accepted;
    assert_eq!(
        intake
            .receive(&assign_message(request, binding, 93), binding, hint)
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let repeated = group::decode_assigned(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!((repeated.epoch, repeated.cursor), (2, Offset::ZERO));
    assert_eq!(actor.snapshot().accepted, before);
    assert!(before.op > confirmed_prefix.op);
    output.clear();

    let mut stale = request;
    stale.authority.view += 1;
    for (request, source, code) in [
        (
            AssignRequest {
                plan_revision: 1,
                ..request
            },
            binding,
            6,
        ),
        (
            AssignRequest {
                member: Some(ConsumerMemberId::from_bytes([99; 16])),
                ..request
            },
            binding,
            8,
        ),
        (
            AssignRequest {
                group: ConsumerGroupId::from_bytes([70; 16]),
                plan_revision: 1,
                initial: Start::Offset(Offset::new(9)),
                ..request
            },
            binding,
            7,
        ),
        (
            AssignRequest {
                policy: Policy::QuorumDurable,
                ..request
            },
            binding,
            2,
        ),
        (request, consumer(70, 80), 3),
        (stale, binding, 5),
    ] {
        assert_eq!(
            intake
                .receive(&assign_message(request, source, 94), source, hint)
                .unwrap(),
            NativeReceive::Accepted
        );
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        assert_eq!(output.len(), 1);
        let rejected = ozzy_proto::nack::decode(packet(&output[0]), limits().envelope).unwrap();
        assert_eq!(rejected.code, code);
        if code == 7 {
            assert_eq!(
                group::InitialPosition::decode(rejected.detail).unwrap(),
                group::InitialPosition {
                    cursor: Offset::new(9),
                    retained_from: Offset::ZERO,
                    end: Offset::new(3),
                }
            );
        }
        assert_eq!(actor.snapshot().accepted, before);
        output.clear();
    }

    // A new group starts at the confirmed end when it selects the latest position.
    let latest = AssignRequest {
        group: ConsumerGroupId::from_bytes([70; 16]),
        plan_revision: 1,
        initial: Start::Latest,
        ..request
    };
    assert_eq!(
        intake
            .receive(&assign_message(latest, binding, 95), binding, hint)
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let created = group::decode_assigned(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!((created.epoch, created.cursor), (1, Offset::new(3)));
    drop(intake);
    close(&mut controller, actor);
}
