//! Traits and types used by writer modules.
use crate::subscription::Subscription;
use crate::{KafkaMessageMeta, RunStartParameters};
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use miette::Diagnostic;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

/// Error type returned by writer modules at runtime.
#[derive(Error, Debug, Diagnostic)]
pub enum WriterModuleError {
    /// Failure to write to a Nexus file.
    #[error(transparent)]
    FailedFileWrite(#[from] hdf5::Error),

    /// Invalid data which the writer module is unable to
    /// consume. This is non-fatal and the writer module
    /// can accept new, valid, data in subsequent calls.
    #[error("Invalid data passed to writer module: {}", .0)]
    InvalidData(String),

    /// A generic fatal error from which this writer module
    /// will not be able to recover. The writer module is
    /// in a state which can no longer accept new data,
    /// or in an internally-inconsistent state.
    #[error("Fatal error in writer module: {}", .0)]
    Fatal(String),
}

/// Error type returned by writer modules at configuration time.
#[derive(Error, Debug, Diagnostic)]
pub enum WriterModuleCreationError {
    /// The specified writer module does not exist.
    #[error("The writer module name '{}' does not exist", name)]
    WriterModuleNameDoesNotExist { name: String },

    /// The writer-module configuration is semantically invalid
    /// or inconsistent, and cannot be used.
    #[error("Invalid writer module configuration: {}", description)]
    ConfigInvalid { description: String },

    /// The configuration of this writer module failed to parse
    /// into the expected structure from JSON.
    #[error("Writer module config parse of type '{}' failed", config_type_name)]
    ConfigParseError {
        #[source]
        cause: serde_json::Error,

        config_type_name: &'static str,
    },
}

/// Result of calling `create` on a writer module spec.
pub struct WriterModuleCreateResult<T> {
    /// The `Writer` implementation
    pub module: T,
    /// Subscriptions defining which Kafka streams this writer
    /// needs to listen to.
    pub subscriptions: Vec<Subscription>,
}

/// Runtime behaviour definitions for this writer module type.
pub trait WriterModule: Debug {
    /// Called once, when a run start message is received.
    ///
    /// The implementation of this method may write directly to the NeXus file,
    /// or buffer data in-memory.
    fn on_run_start(
        &mut self,
        run_start_parameters: &RunStartParameters,
    ) -> Result<(), WriterModuleError>;

    /// Called for each received message which matched a ``Subscription``
    /// provided by this module.
    ///
    /// The implementation of this method may write directly to the NeXus file,
    /// or buffer data in-memory.
    fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        message: &DeserializedMessage,
    ) -> Result<(), WriterModuleError>;

    /// Called when a run-stop message is received.
    ///
    /// The implementation of this method may write directly to the NeXus file,
    /// or buffer data in-memory.
    fn on_run_stop(
        &mut self,
        meta: &KafkaMessageMeta,
        message: &RunStop,
    ) -> Result<(), WriterModuleError>;

    /// Ensure that any local state is reflected in the NeXus file, in an
    /// internally-consistent way.
    ///
    /// For example, this may write an in-memory histogram to the file,
    /// or write in-memory summary statistics such as min/max/average.
    ///
    /// After this method completes, the file must be in a self-consistent
    /// state and as up-to-date as possible with data received so far.
    ///
    /// This may be implemented as a no-op if this writer module always streams
    /// all data to file directly and does not buffer values in-memory.
    ///
    /// This may be called multiple times during a single run, for example
    /// to generate an autosave/intermediate file and then after run-end.
    fn flush(&mut self) -> Result<(), WriterModuleError>;

    /// Has this writer module finished writing, after a run-stop was received?
    ///
    /// Due to out-of-order Kafka message delivery, a run stop may be received
    /// before messages which occured during the run. This hook allows a writer
    /// module to declare whether it has received all necessary data up to
    /// run end. It should return `true` if all necessary data has been received,
    /// or `false` otherwise.
    fn finished(&self) -> bool;
}

/// Creation parameters for writer modules of this type. This trait
/// should be implemented by all writer modules, and is necessary
/// in order to add the writer module to the ``WriterModuleRegistry``.
pub trait WriterModuleSpec: WriterModule + Sized {
    /// The configuration parameters for this type.
    /// This type will be automatically deserialized from the JSON parameters
    /// specified in `nexus_structure`.
    type Config: DeserializeOwned;

    /// The name of this writer module, as referred to in `nexus_structure`.
    /// This must be unique among all writer modules.
    const NAME: &'static str;

    /// Create the writer module.
    ///
    /// This should return ``Err(WriterModuleFactoryError::ConfigInvalid)``
    /// if the configuration is semantically invalid or inconsistent.
    fn create(
        parent_group: hdf5::Group,
        config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError>;
}
