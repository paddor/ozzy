//! Deterministic workloads and fault schedules around production cores.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    reason = "simulation test support"
)]

#[cfg(feature = "broker")]
pub mod broker;
pub mod canonical;
#[cfg(feature = "broker")]
pub mod client;
pub mod schedule;
#[cfg(feature = "broker")]
pub mod soak;
