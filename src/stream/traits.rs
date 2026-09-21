//! Common stream consumer traits and utilities
use miette::Diagnostic;
use rdkafka::Message;
use rdkafka::consumer::CommitMode;
use rdkafka::error::KafkaError;
use std::time::Duration;
use thiserror::Error;

/// Metadata about a message from Kafka
#[derive(Debug, PartialEq, Eq)]
pub struct KafkaMessageMeta<'a> {
    pub topic: &'a str,
    pub key: Option<&'a [u8]>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

/// Errors thrown by `Stream` methods at run time.
#[derive(Debug, Error, Diagnostic)]
#[error("Error from Kafka: {}", .context)]
pub struct StreamError {
    #[source]
    // Note: boxed because `KafkaError` is a fairly large type.
    pub source: Option<Box<KafkaError>>,
    pub context: String,
}

/// Wrapper around Kafka.
///
/// This exists to facilitate unit testing; this trait is implemented
/// for `KafkaStream` (used at runtime), but for unit tests it is also
/// implemented by `FakeStream`.
pub trait Stream {
    /// The type of messages yielded by `poll`.
    type Msg<'a>: Message
    where
        Self: 'a;

    fn assign_from_timestamp(
        &self,
        topics: &[&str],
        timestamp: i64,
        timeout: Duration,
    ) -> Result<(), StreamError>;

    fn poll(&self, timeout: Duration) -> Option<Result<Self::Msg<'_>, StreamError>>;

    fn subscribe(&self, topics: &[&str]) -> Result<(), StreamError>;

    fn unsubscribe(&self);

    fn unassign(&self) -> Result<(), StreamError>;

    fn commit_message(&self, message: &Self::Msg<'_>, mode: CommitMode) -> Result<(), StreamError>;
}
