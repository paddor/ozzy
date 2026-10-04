//! Deterministic workloads and fault schedules around production cores.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "broker")]
pub mod broker;
pub mod canonical;
#[cfg(feature = "broker")]
pub mod client;
pub mod schedule;
#[cfg(feature = "broker")]
pub mod soak;
