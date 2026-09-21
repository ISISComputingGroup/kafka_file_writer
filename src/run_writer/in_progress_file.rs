use crate::error::FileWriterError;
use crate::hdf::attr::{add_ascii_string_attribute, add_root_dataset_attributes};
use crate::nexus_structure::{
    NexusFileStructure, NexusGroup, NexusStructureError, NexusStructureItem,
};
use crate::run_start_parameters::RunStartParameters;
use crate::run_writer::message_router::MessageRouter;
use crate::run_writer::placed_writer::PlacedWriter;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module_registry::WriterModuleFactories;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::{error, trace};
use serde_json::Value;

/// Represents a file which is in the process of being written to.
pub struct InProgressFile<'a> {
    file: hdf5::File,
    writers: Vec<PlacedWriter>,
    run_start_parameters: &'a RunStartParameters,
    run_stop_received: bool,
    router: MessageRouter,
}

impl<'a> InProgressFile<'a> {
    pub fn new(
        file: hdf5::File,
        run_start_parameters: &'a RunStartParameters,
        structure: NexusFileStructure,
        factories: &WriterModuleFactories,
    ) -> Result<Self, FileWriterError> {
        let mut result = InProgressFile {
            file,
            writers: vec![],
            run_start_parameters,
            router: MessageRouter::default(),
            run_stop_received: false,
        };

        result.use_structure(structure, factories)?;
        result.on_run_start(run_start_parameters)?;
        Ok(result)
    }

    fn use_structure(
        &mut self,
        structure: NexusFileStructure,
        factories: &WriterModuleFactories,
    ) -> Result<(), FileWriterError> {
        let root = self.file.clone();
        for child in structure.children {
            self.add_child(&root, child, factories)?;
        }
        for (key, json_value) in structure.attributes {
            if let Value::String(value) = json_value {
                add_ascii_string_attribute(&root, &key, &value)?;
            }
        }
        add_root_dataset_attributes(&root)?;
        Ok(())
    }

    fn add_child(
        &mut self,
        parent: &hdf5::Group,
        structure: NexusStructureItem,
        factories: &WriterModuleFactories,
    ) -> Result<(), FileWriterError> {
        match structure {
            NexusStructureItem::Group(group) => {
                self.add_nexus_group(parent.clone(), group, factories)
            }
            NexusStructureItem::WriterModule(module_spec) => {
                self.add_placed_writer(
                    PlacedWriter::new(parent.clone(), module_spec, factories)
                        .map_err(FileWriterError::NexusStructureError)?,
                );
                Ok(())
            }
            NexusStructureItem::Invalid(json) => Err(FileWriterError::NexusStructureError(
                NexusStructureError::UnknownChildType {
                    location: parent.name(),
                    invalid_json: json.to_string(),
                },
            )),
        }
    }

    fn add_nexus_group(
        &mut self,
        parent: hdf5::Group,
        group: NexusGroup,
        writer_module_factories: &WriterModuleFactories,
    ) -> Result<(), FileWriterError> {
        trace!(
            "Creating new group {} with parent {}",
            group.name,
            parent.name()
        );
        let hdf_group =
            parent
                .create_group(&group.name)
                .map_err(|e| FileWriterError::HDF5Error {
                    cause: e,
                    file_name: self.file.filename(),
                    dataset_location: parent.name(),
                })?;

        for (key, json_value) in group.attributes {
            if let Value::String(value) = json_value {
                add_ascii_string_attribute(&hdf_group, &key, &value)?;
            }
        }

        for child in group.children {
            self.add_child(&hdf_group, child, writer_module_factories)?;
        }
        Ok(())
    }

    pub fn add_placed_writer(&mut self, writer: PlacedWriter) {
        let idx = self.writers.len();

        for sub in writer.subscriptions().iter() {
            self.router.subscribe(idx, sub);
        }

        self.writers.push(writer);
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.router.topics()
    }

    pub fn job_id(&self) -> &str {
        &self.run_start_parameters.job_id
    }

    pub fn finished(&mut self) -> bool {
        self.writers.iter_mut().all(|w| w.writer().finished())
    }

    pub fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        msg: &DeserializedMessage,
    ) -> Result<(), FileWriterError> {
        let writer_indices = self.router.writers_for(meta).collect::<Vec<_>>();

        for writer_index in writer_indices {
            if let Some(writer) = self.writers.get_mut(writer_index) {
                writer.on_message(meta, msg)?;
            } else {
                error!(
                    "Writer module indices and subscriptions are out of sync; no writer module for idx={}",
                    writer_index
                );
                return Err(FileWriterError::WriterModuleNotFound { writer_index });
            }
        }
        Ok(())
    }

    pub fn on_run_start(&mut self, parameters: &RunStartParameters) -> Result<(), FileWriterError> {
        for writer in self.writers.iter_mut() {
            writer.on_run_start(parameters)?;
        }
        Ok(())
    }

    pub fn on_run_stop(
        &mut self,
        meta: &KafkaMessageMeta,
        run_stop: &RunStop,
    ) -> Result<(), FileWriterError> {
        for writer in self.writers.iter_mut() {
            writer.on_run_stop(meta, run_stop)?;
        }
        self.run_stop_received = true;
        Ok(())
    }
}
