//! Group activation over shared SDK links.

use super::*;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
async fn sdk_activation_over_shared_links_allocates_epochs_and_reports_typed_rejections() {
    use ozzy_proto::group::{self, AssignRequest, Start};
    let (mut harness, links, _, hint) = setup_progress(1, None).await;
    let sockets = links.socket_count();
    let request = AssignRequest {
        authority: hint.authority,
        partition: identity().partition,
        group: identity().group,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        plan_revision: 2,
        initial: Start::Earliest,
        policy: Policy::LocalDurable,
    };
    harness.quiesce().await;
    let before = accepted(&harness);
    let mut pending = pin!(links.group_assign(hint.primary, request));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    harness
        .until(false, "activation admission", |h| accepted(h) > before)
        .await;
    assert!(
        futures::poll!(pending.as_mut()).is_pending(),
        "accepted activation is not confirmed"
    );
    let activated = harness.drive(pending, true).await.unwrap();
    assert_eq!(
        (
            activated.epoch,
            activated.plan_revision,
            activated.cursor,
            activated.end
        ),
        (2, 2, Offset::ZERO, Offset::new(3))
    );
    let confirmed = accepted(&harness);
    let repeated = harness
        .drive(links.group_assign(hint.primary, request), true)
        .await
        .unwrap();
    assert_eq!((repeated.epoch, repeated.cursor), (2, Offset::ZERO));
    assert_eq!(
        accepted(&harness),
        confirmed,
        "repeated plans do not append"
    );
    assert!(matches!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        plan_revision: 1,
                        ..request
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    assert!(matches!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        member: Some(ConsumerMemberId::from_bytes([99; 16])),
                        ..request
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Rejected { code: 8, .. })
    ));
    assert_eq!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        group: ConsumerGroupId::from_bytes([70; 16]),
                        plan_revision: 1,
                        initial: Start::Offset(Offset::new(9)),
                        ..request
                    }
                ),
                true
            )
            .await
            .map(|_| ())
            .unwrap_err()
            .to_string(),
        BrokerLinkError::InitialPosition(group::InitialPosition {
            cursor: Offset::new(9),
            retained_from: Offset::ZERO,
            end: Offset::new(3),
        })
        .to_string()
    );
    assert!(matches!(
        harness
            .drive(
                links.group_progress(
                    hint.primary,
                    progress::Request {
                        assignment: progress::Assignment {
                            member: request.member.unwrap(),
                            epoch: 2,
                            ..request_at(hint.authority).assignment
                        },
                        ..request_at(hint.authority)
                    }
                ),
                true
            )
            .await,
        Ok(progress::Confirmed { cursor, .. }) if cursor == Offset::ZERO
    ));
    assert!(matches!(
        harness
            .drive(
                links.group_progress(hint.primary, request_at(hint.authority)),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    assert_eq!(links.socket_count(), sockets);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

fn request_at(authority: ozzy_proto::append::Authority) -> progress::Request {
    request(authority, None)
}
