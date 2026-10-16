use super::*;
use crate::replicated::{ProcessingIdentity, processing::window::Window};
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, Offset, append::Policy};
use std::sync::Arc;
use tokio::sync::Semaphore;

fn identity() -> ProcessingIdentity {
    ProcessingIdentity {
        group: ConsumerGroupId::from_bytes([1; 16]),
        member: ConsumerMemberId::from_bytes([2; 16]),
        partition: PartitionIncarnation::from_bytes([3; 16]),
        assignment_epoch: 1,
        policy: Policy::QuorumDurable,
    }
}

#[tokio::test]
async fn owner_drains_confirmations_and_enforces_deadlines_without_application_polls() {
    for confirm in [false, true] {
        let (mut window, mut publisher) =
            Window::new(identity(), Offset::ZERO, 4, &Arc::new(Semaphore::new(4096))).unwrap();
        let token = window.deliver(Offset::ZERO).unwrap();
        let pending = window.submit(token).unwrap();
        let owned = Owned {
            partition: identity().partition,
            epoch: 1,
            plan_revision: 1,
        };
        let stop = CloseSignal::default();
        let mut sources = Sources::new(Duration::from_millis(50));
        sources.replace(&[Observation {
            owned,
            progress: window.observer(),
            stop: stop.clone(),
            generation: 0,
            draining: false,
        }]);
        let now = Duration::from_secs(20);
        assert_eq!(sources.owned(&[], 0, now), [owned]);
        assert_eq!(sources.deadline(), Some(now + Duration::from_millis(50)));
        if confirm {
            publisher.confirm(1, 1).unwrap();
            assert_eq!(sources.owned(&[], 0, now), [] as [Owned; 0]);
            assert!(pending.try_confirmed().unwrap().is_ok());
        } else {
            assert_eq!(
                sources.owned(&[], 0, now + Duration::from_millis(49)),
                [owned]
            );
            assert_eq!(
                sources.owned(&[], 0, now + Duration::from_millis(50)),
                [] as [Owned; 0]
            );
            publisher.fail(crate::replicated::ProgressError::Fenced);
            assert!(pending.try_confirmed().unwrap().is_err());
        }
        assert!(futures::FutureExt::now_or_never(stop.closed()).is_some());
        // An unchanged application registry cannot resurrect a released window.
        assert_eq!(
            sources.owned(&[], 0, now + Duration::from_secs(1)),
            [] as [Owned; 0]
        );
        assert_eq!(sources.deadline(), None);
    }
}
