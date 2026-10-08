//! Top-level error definitions for errors encountered while processing a run.
use crate::hdf::error::Hdf5Error;
use crate::run_writer::message_router::WriterId;
use crate::run_writer::nexus_structure::NexusStructureError;
use crate::stream::error::StreamError;
use crate::writer_module::WriterModuleError;
use miette::Diagnostic;
use rdkafka::Message;
use thiserror::Error;

/// Top-level error type for errors that can be encountered while processing a run.
///
/// This type usually wraps a lower-level error type with more context about the location
/// or cause of the underlying error.
#[derive(Debug, Diagnostic, Error)]
pub enum FileWriterError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Hdf5Error(#[from] Hdf5Error),

    #[error(transparent)]
    #[diagnostic(transparent)]
    StreamError(#[from] StreamError),

    #[error("IO error during read/write of '{}'", filename)]
    IoError {
        #[source]
        cause: std::io::Error,
        filename: String,
    },

    #[error("Writer module error in module {} at '{}'", module_name, module_path)]
    WriterModuleError {
        #[source]
        #[diagnostic_source]
        cause: WriterModuleError,
        module_name: String,
        module_path: String,
    },

    #[error(transparent)]
    #[diagnostic(transparent)]
    NexusStructureError(#[from] NexusStructureError),

    #[error("Missing required data '{}' in run start message on topic={} partition={} offset={}", .missing_item, topic, partition, offset)]
    MissingRunStartData {
        missing_item: &'static str,
        topic: String,
        partition: i32,
        offset: i64,
    },

    #[error("Could not find writer module corresponding to id={}", .writer_index)]
    WriterModuleNotFound { writer_index: WriterId },

    #[error("Integer error (context: {context})")]
    IntegerError {
        #[source]
        cause: std::num::TryFromIntError,
        context: &'static str,
    },

    #[cfg(test)]
    #[error("error injected by unit tests")]
    UnitTestError,
}

impl FileWriterError {
    /// Make an error describing missing data in a run start message.
    pub fn from_missing_runstart_data(missing_item: &'static str, msg: &impl Message) -> Self {
        Self::MissingRunStartData {
            missing_item,
            topic: msg.topic().to_owned(),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;

    #[test]
    fn test_from_missing_runstart_data() {
        let e = FileWriterError::from_missing_runstart_data(
            "foo",
            &OwnedMessage::new(
                None,
                None,
                "niceTopic".to_owned(),
                Timestamp::NotAvailable,
                123,
                456,
                None,
            ),
        );

        match e {
            FileWriterError::MissingRunStartData {
                missing_item,
                topic,
                partition,
                offset,
            } => {
                assert_eq!(missing_item, "foo");
                assert_eq!(topic, "niceTopic");
                assert_eq!(partition, 123);
                assert_eq!(offset, 456);
            }
            _ => panic!("expected FileWriterError::MissingRunStartData error"),
        }
    }
}
