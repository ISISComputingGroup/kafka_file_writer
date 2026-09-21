//! Contains stream consumers.
//!
//! At runtime, a Kafka 'stream consumer' is used. However, a fake stream consumer
//! is also available for testing and benchmarks.

#[cfg(any(test, feature = "bench"))]
pub mod fake;
pub mod kafka;
pub mod scope;
pub mod traits;
