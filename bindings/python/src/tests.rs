//! Execute the shipped Python facade against the existing real-broker harness.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::Cluster;
use pyo3::{
    prelude::*,
    types::{PyCFunction, PyDict, PyModule},
};
use std::{cell::RefCell, ffi::CString};

thread_local! {
    pub(crate) static CONTEXT: RefCell<Option<ozzy::Context>> = const { RefCell::new(None) };
}

fn run(mode: DeploymentMode, source: &str) {
    scenario(mode, source, "", false, "lz4");
}

fn scenario(mode: DeploymentMode, source: &str, case: &str, asynchronous: bool, codec: &str) {
    harness(mode, source, (case, asynchronous, codec), |topic| {
        topic.deduplication = Some(ozzy_config::TopicDeduplication {
            window_millis: 300_000,
            max_entries: 128,
            max_bytes: 4 * 1024 * 1024,
        });
    });
}

/// Run `source` against a cluster whose `orders` topic `configure` sets up.
fn harness(
    mode: DeploymentMode,
    source: &str,
    globals: (&str, bool, &str),
    configure: impl FnOnce(&mut ozzy_config::Topic),
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cluster = runtime.block_on(Cluster::configured(mode, None, |deployment| {
        configure(deployment.topics.get_mut("orders").unwrap());
    }));
    CONTEXT.with(|context| *context.borrow_mut() = Some(cluster.runtime.context().clone()));
    Python::initialize();
    let passed = Python::attach(|py| execute(py, &cluster, mode, source, globals));
    for control in &cluster.controls {
        control.hold_record_writes(false);
    }
    CONTEXT.with(|context| *context.borrow_mut() = None);
    runtime.block_on(cluster.shutdown());
    assert!(passed, "Python SDK test failed");
}

fn register(py: Python<'_>) {
    let native = PyModule::new(py, "ozzy._native").unwrap();
    super::native(&native).unwrap();
    let sys = py.import("sys").unwrap();
    sys.getattr("modules")
        .unwrap()
        .set_item("ozzy._native", native)
        .unwrap();
    sys.getattr("path")
        .unwrap()
        .call_method1(
            "insert",
            (0, concat!(env!("CARGO_MANIFEST_DIR"), "/python")),
        )
        .unwrap();
}

fn execute(
    py: Python<'_>,
    cluster: &Cluster,
    mode: DeploymentMode,
    source: &str,
    (case, asynchronous, codec): (&str, bool, &str),
) -> bool {
    register(py);
    let brokers: Vec<_> = cluster
        .configs
        .iter()
        .map(|(checked, local)| {
            let endpoints = &checked.deployment.deployment().brokers[&checked.plan.name].endpoints;
            (
                local.broker.to_string(),
                endpoints.peer.clone(),
                endpoints.data_peer.clone(),
            )
        })
        .collect();
    let globals = PyDict::new(py);
    globals.set_item("CASE", case).unwrap();
    globals.set_item("ASYNC", asynchronous).unwrap();
    globals.set_item("CODEC", codec).unwrap();
    globals.set_item("BROKERS", brokers).unwrap();
    globals
        .set_item(
            "POLICY",
            if mode == DeploymentMode::Single {
                "local_durable"
            } else {
                "quorum_durable"
            },
        )
        .unwrap();
    controls(py, &globals, cluster);
    if let Err(error) = py.run(&CString::new(source).unwrap(), Some(&globals), None) {
        error.print(py);
        return false;
    }
    true
}

fn controls(py: Python<'_>, globals: &Bound<'_, PyDict>, cluster: &Cluster) {
    let controls = cluster.controls.clone();
    globals
        .set_item(
            "hold_writes",
            PyCFunction::new_closure(py, None, None, move |args, _| -> PyResult<()> {
                let hold: bool = args.get_item(0)?.extract()?;
                for control in &controls {
                    control.hold_record_writes(hold);
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let controls = cluster.controls.clone();
    globals
        .set_item(
            "held_writes",
            PyCFunction::new_closure(py, None, None, move |_, _| -> usize {
                controls.iter().map(|control| control.held_writes()).sum()
            })
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn blocking_single() {
    run(DeploymentMode::Single, include_str!("../tests/blocking.py"));
}
#[test]
fn blocking_quorum() {
    run(DeploymentMode::Three, include_str!("../tests/blocking.py"));
}
#[test]
fn asyncio_single() {
    run(
        DeploymentMode::Single,
        include_str!("../tests/asyncio_sdk.py"),
    );
}
#[test]
fn asyncio_quorum() {
    run(
        DeploymentMode::Three,
        include_str!("../tests/asyncio_sdk.py"),
    );
}

macro_rules! cases {
    ($mode:expr, $asynchronous:expr, $codec:expr; $($name:ident),+ $(,)?) => {
        $(#[test]
        fn $name() {
            scenario($mode, include_str!("../tests/scenarios.py"), stringify!($name), $asynchronous, $codec);
        })+
    };
}
macro_rules! matrix {
    ($mode:expr, $asynchronous:expr, $codec:expr) => {
        cases!($mode, $asynchronous, $codec;
            payload_boundaries, mutable_snapshot, invalid_admission, identity_formats,
            routing, shared_partition, confirmation_lifetime, duplicate_identity,
            checkpoint_prefix, latest, independent_readers, checkpoint_rejections,
            context_cleanup, resume, seeded_roundtrip, many_ready_records, mixed_codecs, native_values,
            reader_bounds,
            reader_statistics,
        );
    };
}
macro_rules! codecs {
    ($mode:expr, $asynchronous:expr) => {
        mod raw {
            use super::*;
            matrix!($mode, $asynchronous, "raw");
        }
        mod lz4 {
            use super::*;
            matrix!($mode, $asynchronous, "lz4");
        }
    };
}
macro_rules! cancellations {
    ($mode:expr, $codec:expr; $($name:ident),+ $(,)?) => {
        $(#[test]
        fn $name() {
            scenario($mode, include_str!("../tests/cancellation.py"), stringify!($name), true, $codec);
        })+
    };
}
macro_rules! cancellation_matrix {
    ($mode:expr, $codec:expr) => {
        cancellations!($mode, $codec; receive_cancel, confirmation_observers, flush_cancel,
            close_cancel, independent_owners, early_cancel, owner_error_recovery, next_timeout,
            retained_backing, admission_pressure);
    };
}
mod cancellation {
    use super::*;
    mod single_raw {
        use super::*;
        cancellation_matrix!(DeploymentMode::Single, "raw");
    }
    mod single_lz4 {
        use super::*;
        cancellation_matrix!(DeploymentMode::Single, "lz4");
    }
    mod quorum_raw {
        use super::*;
        cancellation_matrix!(DeploymentMode::Three, "raw");
    }
    mod quorum_lz4 {
        use super::*;
        cancellation_matrix!(DeploymentMode::Three, "lz4");
    }
}

mod delay {
    use super::*;

    fn delayed(mode: DeploymentMode, asynchronous: bool) {
        let source = include_str!("../tests/delay.py");
        harness(mode, source, ("", asynchronous, "lz4"), |topic| {
            topic.delay = Some(ozzy_config::TopicDelay {
                max_secs: 60,
                max_entries: 64,
            });
        });
    }

    #[test]
    fn single_blocking() {
        delayed(DeploymentMode::Single, false);
    }
    #[test]
    fn single_asynchronous() {
        delayed(DeploymentMode::Single, true);
    }
    #[test]
    fn quorum_blocking() {
        delayed(DeploymentMode::Three, false);
    }
    #[test]
    fn quorum_asynchronous() {
        delayed(DeploymentMode::Three, true);
    }
}

#[test]
fn input_validation() {
    Python::initialize();
    Python::attach(|py| {
        register(py);
        py.run(
            &CString::new(include_str!("../tests/validation.py")).unwrap(),
            Some(&PyDict::new(py)),
            None,
        )
        .unwrap();
    });
}
mod single {
    use super::*;
    mod blocking {
        use super::*;
        codecs!(DeploymentMode::Single, false);
    }
    mod asynchronous {
        use super::*;
        codecs!(DeploymentMode::Single, true);
    }
}
mod quorum {
    use super::*;
    mod blocking {
        use super::*;
        codecs!(DeploymentMode::Three, false);
    }
    mod asynchronous {
        use super::*;
        codecs!(DeploymentMode::Three, true);
    }
}
