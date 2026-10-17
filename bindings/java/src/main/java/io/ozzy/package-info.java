/**
 * Typed blocking and asynchronous clients over Ozzy's Rust SDK. SDKs own no journals or bound
 * broker sockets. OMQ owns transport; Rust owns routing, admission, retry identity, batching,
 * confirmations, and gap repair. Use one mutable operation at a time per handle. Cancellation
 * returns its native lease before completing. Callbacks run outside native owner threads.
 */
package io.ozzy;
