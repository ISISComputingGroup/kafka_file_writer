//! Representation of a file which is in the process of being written to.
use crate::error::FileWriterError;
use crate::hdf::attr::{add_ascii_string_attribute, add_root_dataset_attributes};
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::message_router::MessageRouter;
use crate::run_writer::nexus_structure::{
    NexusFileStructure, NexusGroup, NexusStructureError, NexusStructureItem,
};
use crate::run_writer::placed_writer::PlacedWriter;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module_factories::WriterModuleFactories;
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
                self.add_placed_writer(PlacedWriter::new(
                    parent.clone(),
                    module_spec,
                    self.run_start_parameters,
                    factories,
                )?);
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
        let hdf_group = parent
            .create_group(&group.name)
            .err_parent_dataset(&parent, &group.name)?;

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

    fn add_placed_writer(&mut self, writer: PlacedWriter) {
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

    pub fn finished(&self) -> bool {
        self.run_stop_received && self.writers.iter().all(|w| w.writer().finished())
    }

    pub fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        msg: &DeserializedMessage,
    ) -> Result<(), FileWriterError> {
        for writer_index in self.router.writers_for(meta) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;
    use crate::hdf::scope::with_nexus_file;
    use crate::run_writer::nexus_structure::NexusWriterModule;
    use crate::subscription::{Subscription, SubscriptionKey};
    use crate::writer_module::{
        WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
        WriterModuleSpec,
    };
    use crate::writer_modules::default_registry;
    use ahash::{HashMap, HashMapExt, HashSet};
    use hdf5::Group;
    use serde_json::json;

    fn empty_structure() -> NexusFileStructure {
        NexusFileStructure {
            children: vec![],
            attributes: HashMap::new(),
        }
    }

    #[derive(Debug)]
    struct AmazingWriter {}

    impl WriterModule for AmazingWriter {
        fn on_message(
            &mut self,
            _: &KafkaMessageMeta,
            _: &DeserializedMessage,
        ) -> Result<(), WriterModuleError> {
            Ok(())
        }

        fn on_run_stop(
            &mut self,
            _: &KafkaMessageMeta,
            _: &RunStop,
        ) -> Result<(), WriterModuleError> {
            Ok(())
        }

        fn flush(&mut self) -> Result<(), WriterModuleError> {
            Ok(())
        }

        fn finished(&self) -> bool {
            false
        }
    }

    impl WriterModuleSpec for AmazingWriter {
        type Config = String; // Our "config" is a single string which is a topic to subscribe to
        const NAME: &'static str = "amazing_writer";

        fn create(
            _: Group,
            _: &RunStartParameters,
            topic: Self::Config,
        ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
            Ok(WriterModuleCreateResult {
                module: AmazingWriter {},
                subscriptions: vec![Subscription::new(topic, SubscriptionKey::All)],
            })
        }
    }

    fn registry_with_amazing_writer() -> WriterModuleFactories {
        let mut res = WriterModuleFactories::empty();
        res.register_module::<AmazingWriter>();
        res
    }

    fn structure_with_amazing_writers() -> NexusFileStructure {
        NexusFileStructure {
            children: vec![
                NexusStructureItem::WriterModule(NexusWriterModule {
                    module: "amazing_writer".to_owned(),
                    config: json!("topic1"),
                }),
                NexusStructureItem::WriterModule(NexusWriterModule {
                    module: "amazing_writer".to_owned(),
                    config: json!("topic1"),
                }),
                NexusStructureItem::WriterModule(NexusWriterModule {
                    module: "amazing_writer".to_owned(),
                    config: json!("topic2"),
                }),
            ],
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn test_in_progress_file_with_empty_structure() {
        with_nexus_file(FileCreator::Memory, "test", |f| {
            let rsp = RunStartParameters::default();
            let ipf = InProgressFile::new(f, &rsp, empty_structure(), &default_registry()).unwrap();
            assert_eq!(ipf.topics().collect::<Vec<&str>>(), Vec::<&str>::new());
            assert_eq!(ipf.job_id(), rsp.job_id);
            assert!(!ipf.run_stop_received);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_in_progress_file_with_structure() {
        with_nexus_file(FileCreator::Memory, "test", |f| {
            let rsp = RunStartParameters::default();
            let ipf = InProgressFile::new(
                f,
                &rsp,
                structure_with_amazing_writers(),
                &registry_with_amazing_writer(),
            )
            .unwrap();

            // topics should be deduplicated, only 2 topics
            let expected_topics = ["topic1", "topic2"].into_iter().collect::<HashSet<_>>();
            assert_eq!(ipf.topics().collect::<HashSet<&str>>(), expected_topics);
            assert_eq!(ipf.writers.len(), 3);
            Ok(())
        })
        .unwrap();
    }
}
