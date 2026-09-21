//! Representation of a Writer module with a specific NeXus location it is writing to.
use crate::error::FileWriterError;
use crate::nexus_structure::{NexusStructureError, NexusWriterModule};
use crate::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::subscription::Subscription;
use crate::writer_module::{WriterModule, WriterModuleError};
use crate::writer_module_registry::WriterModuleFactories;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::warn;
use std::ops::DerefMut;

#[derive(Debug)]
pub struct PlacedWriter {
    group: hdf5::Group,
    writer: Box<dyn WriterModule>,
    writer_name: String,
    subscriptions: Vec<Subscription>,
}

impl PlacedWriter {
    pub fn new(
        group: hdf5::Group,
        spec: NexusWriterModule,
        factories: &WriterModuleFactories,
    ) -> Result<Self, NexusStructureError> {
        let factory = factories.get(&spec.module).ok_or_else(|| {
            NexusStructureError::UnknownWriterModule {
                location: group.name().to_owned(),
                name: spec.module.to_owned(),
            }
        })?;

        let create_result = factory(group.clone(), spec.config).map_err(|e| {
            NexusStructureError::WriterModuleCreationError {
                cause: e,
                location: group.name().to_owned(),
                module_name: spec.module.to_owned(),
            }
        })?;

        Ok(Self {
            writer_name: spec.module,
            writer: create_result.module,
            group,
            subscriptions: create_result.subscriptions,
        })
    }

    pub fn writer(&mut self) -> &mut dyn WriterModule {
        self.writer.deref_mut()
    }

    pub fn group(&self) -> &hdf5::Group {
        &self.group
    }

    pub fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        msg: &DeserializedMessage,
    ) -> Result<(), FileWriterError> {
        match self.writer.on_message(meta, msg) {
            Ok(()) => Ok(()),
            Err(WriterModuleError::InvalidData(msg)) => {
                warn!(
                    "Writer module '{}' writing to '{}' failed to write message: {}",
                    self.writer_name.to_owned(),
                    self.group.name(),
                    msg
                );
                Ok(())
            }
            Err(e) => Err(FileWriterError::WriterModuleError {
                cause: e,
                module_path: self.group().name().to_owned(),
                module_name: self.writer_name.to_owned(),
            }),
        }
    }

    pub fn on_run_start(&mut self, parameters: &RunStartParameters) -> Result<(), FileWriterError> {
        match self.writer.on_run_start(parameters) {
            Ok(()) => Ok(()),
            Err(e) => Err(FileWriterError::WriterModuleError {
                cause: e,
                module_path: self.group().name().to_owned(),
                module_name: self.writer_name.to_owned(),
            }),
        }
    }

    pub fn on_run_stop(
        &mut self,
        meta: &KafkaMessageMeta,
        msg: &RunStop,
    ) -> Result<(), FileWriterError> {
        match self.writer.on_run_stop(meta, msg) {
            Ok(()) => Ok(()),
            Err(e) => Err(FileWriterError::WriterModuleError {
                cause: e,
                module_path: self.group().name().to_owned(),
                module_name: self.writer_name.to_owned(),
            }),
        }
    }

    pub fn flush(&mut self) -> Result<(), FileWriterError> {
        match self.writer.flush() {
            Ok(()) => Ok(()),
            Err(e) => Err(FileWriterError::WriterModuleError {
                cause: e,
                module_path: self.group().name().to_owned(),
                module_name: self.writer_name.to_owned(),
            }),
        }
    }

    pub fn subscriptions(&self) -> &[Subscription] {
        &self.subscriptions
    }
}
