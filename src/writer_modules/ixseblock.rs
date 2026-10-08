use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IXseblockConfig {}

#[derive(Debug)]
pub struct IXseblock {
    _config: IXseblockConfig,
    _group: hdf5::Group,
}

impl WriterModule for IXseblock {
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

    fn finished(&self) -> bool {
        true
    }
}

impl WriterModuleSpec for IXseblock {
    type Config = IXseblockConfig;

    const NAME: &'static str = "IXseblock";

    fn create(
        group: hdf5::Group,
        _run_start_parameters: &RunStartParameters,
        config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let module = IXseblock {
            _config: config,
            _group: group,
        };

        let subscriptions = vec![];

        Ok(WriterModuleCreateResult {
            module,
            subscriptions,
        })
    }
}
