//! Writer module for self-describing the file-writer program.
//!
//! This writer module adds the following  datasets to its parent group:
//! - `program_name`

use crate::hdf::attr::add_simple_string_attribute;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use hdf5::HDF5_VERSION;
use hdf5::types::VarLenAscii;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use serde::Deserialize;
use serde_json::json;

const DATASET_NAME: &str = "program_name";
const PROGRAM_NAME: &str = "kafka_file_writer";

#[derive(Debug)]
pub struct ProgramName {}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ProgramNameConfig {}

impl WriterModule for ProgramName {
    fn on_message(
        &mut self,
        _meta: &KafkaMessageMeta,
        _message: &DeserializedMessage,
    ) -> Result<(), WriterModuleError> {
        Ok(())
    }

    fn on_run_stop(
        &mut self,
        _meta: &KafkaMessageMeta,
        _message: &RunStop,
    ) -> Result<(), WriterModuleError> {
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriterModuleError> {
        Ok(())
    }
}

impl WriterModuleSpec for ProgramName {
    type Config = ProgramNameConfig;

    const NAME: &'static str = "ProgramName";

    fn create(
        group: hdf5::Group,
        run_start_parameters: &RunStartParameters,
        _config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let dataset = group
            .new_dataset::<VarLenAscii>()
            .shape((1,))
            .create(DATASET_NAME)
            .err_parent_dataset(&group, DATASET_NAME)?;

        dataset
            .write_slice(
                &[VarLenAscii::from_ascii(PROGRAM_NAME).err_dataset(&dataset)?],
                ..,
            )
            .err_dataset(&dataset)?;

        add_simple_string_attribute(&dataset, "url", env!("CARGO_PKG_REPOSITORY"))?;
        add_simple_string_attribute(
            &dataset,
            "version",
            &format!("{}+git.{}", env!("CARGO_PKG_VERSION"), env!("GIT_VERSION")),
        )?;

        add_simple_string_attribute(
            &dataset,
            "configuration",
            &json!({
                "build": {
                    "cargo_package_name": env!("CARGO_PKG_NAME"),
                    "host_arch": env!("BUILD_HOST"),
                    "target_arch": env!("BUILD_TARGET"),
                    "opt_level": env!("BUILD_OPT_LEVEL"),
                    "debug": env!("BUILD_DEBUG"),
                    "rustc": env!("BUILD_RUSTC"),
                },
                "file": {
                    "job_id": run_start_parameters.job_id,
                },
                "dependencies": {
                    // Only core input/output dependencies - not every package we depend on
                    "hdf5": format!("{}.{}.{}", HDF5_VERSION.major, HDF5_VERSION.minor, HDF5_VERSION.micro),
                    "rdkafka": rdkafka::util::get_rdkafka_version().1
                }
            })
            .to_string(),
        )?;

        Ok(WriterModuleCreateResult {
            module: ProgramName {},
            subscriptions: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;
    use serde_json::Value;

    #[test]
    fn test_program_name_writer() {
        let file = FileCreator::Memory.create_file("unittest").unwrap();
        let group = file.create_group("group").unwrap();

        let _ = ProgramName::create(
            group.clone(),
            &RunStartParameters::default(),
            Default::default(),
        )
        .unwrap();

        assert_eq!(
            group
                .dataset("program_name")
                .unwrap()
                .read_slice_1d::<VarLenAscii, _>(0..1)
                .unwrap()[0]
                .as_str(),
            PROGRAM_NAME
        );

        assert!(
            group
                .dataset("program_name")
                .unwrap()
                .attr("url")
                .unwrap()
                .read_scalar::<VarLenAscii>()
                .unwrap()
                .starts_with("http")
        );
        assert!(
            !group
                .dataset("program_name")
                .unwrap()
                .attr("url")
                .unwrap()
                .read_scalar::<VarLenAscii>()
                .unwrap()
                .is_empty()
        );

        let configuration = group
            .dataset("program_name")
            .unwrap()
            .attr("configuration")
            .unwrap()
            .read_scalar::<VarLenAscii>()
            .unwrap();

        // Check configuration parses as JSON
        let _: Value = serde_json::from_str(&configuration).unwrap();
    }
}
