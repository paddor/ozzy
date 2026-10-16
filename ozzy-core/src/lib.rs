//! Deterministic application state, reader progress, and visibility.
//!
//! Group authority lives in `ozzy-replication`. These modules perform no I/O.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::indexing_slicing)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

pub mod checkpoint;
pub mod dedup;
pub mod live;
pub mod reader;
pub mod retention;
pub mod state;
