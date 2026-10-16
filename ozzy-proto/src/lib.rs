#![doc = "Sans-I/O application protocol and shared message types for Ozzy."]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

pub mod admin;
pub mod append;
pub mod data;
pub mod directory;
mod envelope;
pub mod group;
pub mod handshake;
pub mod layout;
pub mod nack;
pub mod producer;
pub mod progress;
pub mod reader;
mod types;
mod wire;

pub use envelope::{
    ENVELOPE_BYTES, Envelope, EnvelopeError, EnvelopeLimits, Frames, Opcode, Packet, VERSION,
    decode_packet,
};
pub use types::*;
