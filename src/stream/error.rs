//! Error types returned by `Stream` functions.
use miette::Diagnostic;
use rdkafka::error::RDKafkaErrorCode;
use thiserror::Error;

type DynErr = dyn std::error::Error + Send + Sync + 'static;

/// Errors thrown by `Stream` methods at run time.
#[derive(Debug, Error, Diagnostic)]
pub enum StreamError {
    #[error(
        "Missing metadata on topic '{}' (context: '{}'). Kafka error code={:?}",
        topic,
        context,
        err_code
    )]
    MissingMetadata {
        #[source]
        cause: Option<Box<DynErr>>,
        topic: String,
        err_code: Option<RDKafkaErrorCode>,
        context: &'static str,
    },

    #[error("Failed creating kafka consumer")]
    ConsumerCreationError {
        #[source]
        cause: Box<DynErr>,
    },

    #[error("Failed assigning/subscribing kafka consumer")]
    AssignmentError {
        #[source]
        cause: Box<DynErr>,
        topics: Vec<String>,
        context: &'static str,
    },

    #[error("Failed polling message from Kafka stream")]
    PollError {
        #[source]
        cause: Box<DynErr>,
    },

    #[error("Failed committing message to Kafka")]
    CommitError {
        #[source]
        cause: Box<DynErr>,
        topic: String,
        partition: i32,
        offset: i64,
    },

    #[cfg(test)]
    #[error("Error injected by unit test")]
    UnitTestError,
}
