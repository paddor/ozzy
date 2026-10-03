//! Deterministic application state, reader progress, and visibility.
//!
//! Group authority lives in `ozzy-replication`. These modules perform no I/O.
#![forbid(unsafe_code)]

pub mod live;
pub mod reader;
pub mod state;
