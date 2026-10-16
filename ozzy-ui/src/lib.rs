//! Ozzy admin console. The core turns admin replies and key presses into
//! screens. It has no I/O and reads no clock, so a terminal and a browser
//! frontend drive the same [`App`].
//!
//! The `terminal` feature adds [`client`] and the `ozzy-tui` binary: a
//! crossterm frontend that polls one broker's admin endpoint.
#![warn(missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        reason = "product code is linted without cfg(test)"
    )
)]

mod app;
#[cfg(feature = "terminal")]
pub mod client;
mod format;
mod model;
mod view;

pub use app::{Action, App, Key, Mouse, Screen};

#[cfg(test)]
mod tests;
