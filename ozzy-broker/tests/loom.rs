//! Model the production broker shutdown boundary, not OMQ queue internals.
#![cfg(ozzy_loom)]
#![expect(
    dead_code,
    reason = "production lifecycle exposes more than these models use"
)]

pub use ozzy_broker::StartupError;
#[path = "../src/shards/lifecycle.rs"]
mod lifecycle;
pub use lifecycle::Shutdown;

use lifecycle::{Registration, State};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[test]
fn queue_closure_after_either_owner_stops_is_an_expected_shutdown() {
    for frontend_first in [false, true] {
        loom::model(move || {
            let (application, frontend) = lifecycle::startup_groups(2);
            race_owner_exit(application, frontend, frontend_first);
        });
    }
}

#[test]
fn failure_in_either_owner_requests_peer_shutdown_before_closing_its_queue() {
    for frontend_failed in [false, true] {
        loom::model(move || {
            let (application, frontend) = lifecycle::startup_groups(2);
            let application_state = application.state();
            let frontend_state = frontend.state();
            let (failed, peer) = if frontend_failed {
                (frontend_state, application_state)
            } else {
                (application_state, frontend_state)
            };
            let closed = Arc::new(AtomicBool::new(false));
            let exit = {
                let closed = closed.clone();
                thread::spawn(move || {
                    failed.fail(StartupError::Runtime("injected owner failure".into()));
                    closed.store(true, Ordering::Release);
                })
            };
            if closed.load(Ordering::Acquire) {
                assert!(peer.stop.is_requested());
            }
            exit.join().unwrap();
            assert!(peer.stop.is_requested());
            assert!(peer.result().is_ok());
        });
    }
}

// A sequential registration test cannot expose the interval between these two
// requests. Keep the previous arrangement as a sensitivity check for the model.
#[test]
#[should_panic(expected = "peer queue closed before this owner observed shutdown")]
fn separate_stop_signals_allow_a_closed_queue_to_look_like_a_runtime_failure() {
    loom::model(|| {
        let application = Registration::new(std::sync::Arc::new(State::new(2)));
        let frontend = Registration::new(std::sync::Arc::new(State::new(1)));
        race_owner_exit(application, frontend, true);
    });
}

fn race_owner_exit(application: Registration, frontend: Registration, frontend_first: bool) {
    let application_state = application.state();
    let frontend_state = frontend.state();
    // Closing a lane publishes the sender's exit before the receiver observes
    // closure. These atomics model that boundary; no OMQ implementation is copied.
    let application_closed = Arc::new(AtomicBool::new(false));
    let frontend_closed = Arc::new(AtomicBool::new(false));
    let application_owner = owner_exit(
        application,
        application_closed.clone(),
        frontend_closed.clone(),
    );
    let frontend_owner = owner_exit(frontend, frontend_closed, application_closed);
    let (first, second) = if frontend_first {
        (&frontend_state, &application_state)
    } else {
        (&application_state, &frontend_state)
    };
    first.stop.request();
    thread::yield_now();
    second.stop.request();
    application_owner.join().unwrap();
    frontend_owner.join().unwrap();
    assert!(application_state.finished.is_requested());
    assert!(frontend_state.finished.is_requested());
    assert!(application_state.result().is_ok());
    assert!(frontend_state.result().is_ok());
}

fn owner_exit(
    registration: Registration,
    own_closed: Arc<AtomicBool>,
    peer_closed: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let state = registration.state();
        if state.stop.is_requested() {
            own_closed.store(true, Ordering::Release);
        }
        if peer_closed.load(Ordering::Acquire) {
            assert!(
                state.stop.is_requested(),
                "peer queue closed before this owner observed shutdown"
            );
        }
        drop(registration);
    })
}
