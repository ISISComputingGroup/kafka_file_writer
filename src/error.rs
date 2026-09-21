//! Top-level error definitions for runtime errors encountered during file-writing.
use crate::nexus_structure::NexusStructureError;
use crate::run_writer::message_router::WriterId;
use crate::stream::traits::StreamError;
use crate::writer_module::WriterModuleError;
use hdf5::types::StringError;
use miette::Diagnostic;
use rdkafka::Message;
use thiserror::Error;

/// Top-level error type for errors that can be encountered during file writing.
///
/// This type usually wraps a lower-level error type with more context about the location
/// or cause of the underlying error.
#[derive(Debug, Diagnostic, Error)]
pub enum FileWriterError {
    #[error(
        "HDF5 error while writing to file {} dataset {:?}",
        file_name,
        dataset_location
    )]
    HDF5Error {
        #[source]
        cause: hdf5::Error,
        file_name: String,
        dataset_location: String,
    },

    #[error(
        "Stream error (topic={:?}, partition={:?}, offset={:?})",
        topic,
        partition,
        offset
    )]
    StreamError {
        #[source]
        #[diagnostic_source]
        cause: StreamError,
        topic: Option<String>,
        partition: Option<i32>,
        offset: Option<i64>,
    },

    #[error("String encoding error while writing '{}'", .dataset_location)]
    StringError {
        #[source]
        cause: StringError,
        dataset_location: String,
    },

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
    NexusStructureError(NexusStructureError),

    #[error("Missing required data '{}' in run start message on topic={} partition={} offset={}", .missing_item, topic, partition, offset)]
    MissingRunStartData {
        missing_item: &'static str,
        topic: String,
        partition: i32,
        offset: i64,
    },

    #[error("Could not find writer module corresponding to id={}", .writer_index)]
    WriterModuleNotFound { writer_index: WriterId },

    #[cfg(test)]
    #[error("error injected by unit tests")]
    UnitTestError,
}

impl FileWriterError {
    /// Make a StreamError related to a specific topic, but not
    /// a specific message.
    pub fn from_stream_topic(err: StreamError, topic: &str) -> Self {
        Self::StreamError {
            cause: err,
            topic: Some(topic.to_owned()),
            partition: None,
            offset: None,
        }
    }

    /// Make a StreamError related to a specific message.
    pub fn from_stream_message(err: StreamError, msg: &impl Message) -> Self {
        Self::StreamError {
            cause: err,
            topic: Some(msg.topic().to_owned()),
            partition: Some(msg.partition()),
            offset: Some(msg.offset()),
        }
    }

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
