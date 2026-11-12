use crate::hdf::attr::add_simple_string_attribute;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use hdf5::types::VarLenAscii;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use serde::Deserialize;

#[derive(Debug)]
pub struct InstrumentName {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentNameConfig {
    dataset_name: String,
}

impl WriterModule for InstrumentName {
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

impl WriterModuleSpec for InstrumentName {
    type Config = InstrumentNameConfig;

    const NAME: &'static str = "InstrumentName";

    fn create(
        group: hdf5::Group,
        run_start_parameters: &RunStartParameters,
        config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let ds = group
            .new_dataset::<VarLenAscii>()
            .shape([1])
            .create(config.dataset_name.as_str())
            .err_parent_dataset(&group, config.dataset_name.as_str())?;

        let inst_name =
            VarLenAscii::from_ascii(&run_start_parameters.instrument_name).err_dataset(&ds)?;

        ds.write(&[inst_name]).err_dataset(&ds)?;

        // TODO: the instrument short name isn't necessarily the first 3 characters of the full name
        // SANDALS -> SLS...
        add_simple_string_attribute(
            &ds,
            "short_name",
            run_start_parameters
                .instrument_name
                .get(0..3)
                .unwrap_or(&run_start_parameters.instrument_name),
        )?;

        let module = InstrumentName {};

        let subscriptions = vec![];

        Ok(WriterModuleCreateResult {
            module,
            subscriptions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;

    #[test]
    fn test_instrument_name() {
        let file = FileCreator::Memory.create_file("unittest").unwrap();
        let group = file.create_group("test_group").unwrap();

        InstrumentName::create(
            group,
            &RunStartParameters {
                instrument_name: "GREATINSTRUMENT".to_string(),
                ..Default::default()
            },
            InstrumentNameConfig {
                dataset_name: "name".to_owned(),
            },
        )
        .unwrap();

        let name_dataset = file.group("test_group").unwrap().dataset("name").unwrap();
        let read_result = name_dataset.read_slice_1d::<VarLenAscii, _>(0..1).unwrap();

        assert_eq!(read_result[0], "GREATINSTRUMENT");
    }

    #[test]
    fn test_instrument_short_name() {
        let file = FileCreator::Memory.create_file("unittest").unwrap();
        let group = file.create_group("test_group").unwrap();

        InstrumentName::create(
            group,
            &RunStartParameters {
                instrument_name: "GREATINSTRUMENT".to_string(),
                ..Default::default()
            },
            InstrumentNameConfig {
                dataset_name: "name".to_owned(),
            },
        )
        .unwrap();

        let short_name_attr = file
            .group("test_group")
            .unwrap()
            .dataset("name")
            .unwrap()
            .attr("short_name")
            .unwrap();
        let read_result: VarLenAscii = short_name_attr.read_scalar().unwrap();

        assert_eq!(read_result, "GRE");
    }
}
