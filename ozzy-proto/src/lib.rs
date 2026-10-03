#![doc = "Sans-I/O application protocol and shared message types for Ozzy."]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod append;
pub mod data;
pub mod directory;
mod envelope;
pub mod handshake;
pub mod nack;
pub mod producer;
pub mod reader;
mod types;

pub use envelope::{
    ENVELOPE_BYTES, Envelope, EnvelopeError, EnvelopeLimits, Opcode, Packet, VERSION, decode_packet,
};
pub use types::*;
