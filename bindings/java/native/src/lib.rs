//! JNI lifecycle and FFM scalar adapters to the existing Rust SDK.
#![deny(unsafe_code)]
#![warn(missing_docs)]
mod execution;
mod ffi;
mod input;
mod state;
#[cfg(test)]
mod tests;
mod values;
