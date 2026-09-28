//! Common stream consumer traits and utilities
use crate::stream::error::StreamError;
use rdkafka::Message;
use rdkafka::consumer::CommitMode;
use std::time::Duration;

/// Metadata about a message from Kafka
#[derive(Debug, PartialEq, Eq)]
pub struct KafkaMessageMeta<'a> {
    pub topic: &'a str,
    pub key: Option<&'a [u8]>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
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
