use crate::hdf::attr::add_ascii_string_attribute;
use crate::subscription::{Subscription, SubscriptionKey};
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use crate::{KafkaMessageMeta, RunStartParameters};
use hdf5::SimpleExtents;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NXeventdataConfig {
    topic: String,
}

#[derive(Debug)]
pub struct NXeventdata {
    _config: NXeventdataConfig,
    group: hdf5::Group,
    run_start_time_ms: u64,
    run_stop_time_ms: u64,
    latest_message_time_ms: u64,
}

impl WriterModule for NXeventdata {
    fn on_run_start(
        &mut self,
        run_start_parameters: &RunStartParameters,
    ) -> Result<(), WriterModuleError> {
        self.run_start_time_ms = run_start_parameters.start_time;

        self.group
            .new_dataset::<f64>()
            .shape(SimpleExtents::resizable([0]))
            .chunk([32000]) // TODO: configurable chunk sizes.
            .create("event_time_zero")?;

        self.group
            .new_dataset::<u32>()
            .shape(SimpleExtents::resizable([0]))
            .chunk([32000]) // TODO: configurable chunk sizes.
            .create("event_id")?;

        self.group
            .new_dataset::<f32>()
            .shape(SimpleExtents::resizable([0]))
            .chunk([32000]) // TODO: configurable chunk sizes.
            .create("event_time_offset")?;

        // TODO: handle error better (make an `AttributeError` type, which can be the 'cause'
        // TODO: of a `WriterModuleError`).
        add_ascii_string_attribute(&self.group, "NX_class", "NXevent_data").map_err(|_| {
            WriterModuleError::Fatal("could not create NX_class attribute".to_string())
        })?;
        Ok(())
    }

    fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        message: &DeserializedMessage,
    ) -> Result<(), WriterModuleError> {
        // TODO: the logic for constructing these datasets from pu00 + ev44 is more complicated
        // TODO: than this. Additionally, various parts of this need refactoring into separate
        // TODO: reusable modules.
        // TODO: Currently this is just a proof-of-principle that writer modules can receive and
        // TODO: write data.
        if let Some(timestamp) = meta.timestamp {
            let timestamp = timestamp as u64;
            self.latest_message_time_ms = timestamp;
            if timestamp < self.run_start_time_ms || timestamp > self.run_stop_time_ms {
                // This writer module doesn't write messages before / after run.
                return Ok(());
            }
        }

        match message {
            DeserializedMessage::PulseMetadataPu00(msg) => {
                let event_time_zero = self.group.dataset("event_time_zero")?;
                let original_length = event_time_zero.size();
                event_time_zero.resize(original_length + 1)?;
                event_time_zero.write_slice(&[msg.reference_time() as f64], original_length..)?;
            }
            DeserializedMessage::EventDataEv44(msg) => {
                let pixel_ids = msg.pixel_id().ok_or_else(|| {
                    WriterModuleError::InvalidData("Missing pixel_id".to_string())
                })?;
                let time_of_flight = msg.time_of_flight().ok_or_else(|| {
                    WriterModuleError::InvalidData("Missing time_of_flight".to_string())
                })?;

                let pixel_ids_len = pixel_ids.len();
                let time_of_flight_len = time_of_flight.len();

                if pixel_ids_len != time_of_flight_len {
                    return Err(WriterModuleError::InvalidData(format!(
                        "pixel_id length {} does not match time_of_flight length {}",
                        pixel_ids_len, time_of_flight_len
                    )));
                }

                let event_time_offset = self.group.dataset("event_time_offset")?;
                let event_id = self.group.dataset("event_id")?;
                let original_length = event_time_offset.size();
                event_id.resize(original_length + pixel_ids_len)?;
                event_time_offset.resize(original_length + time_of_flight_len)?;

                event_id.write_slice(
                    &pixel_ids
                        .into_iter()
                        .map(|t| (t as f32) / 1000.)
                        .collect::<Vec<_>>(),
                    original_length..,
                )?;
                event_time_offset.write_slice(
                    &time_of_flight.into_iter().collect::<Vec<_>>(),
                    original_length..,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn on_run_stop(
        &mut self,
        _meta: &KafkaMessageMeta,
        message: &RunStop,
    ) -> Result<(), WriterModuleError> {
        self.run_stop_time_ms = message.stop_time();
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriterModuleError> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.latest_message_time_ms >= self.run_stop_time_ms
    }
}

impl WriterModuleSpec for NXeventdata {
    type Config = NXeventdataConfig;

    const NAME: &'static str = "NXevent_data";

    fn create(
        group: hdf5::Group,
        config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let subscriptions = vec![Subscription::new(
            config.topic.to_owned(),
            SubscriptionKey::All,
        )];

        let module = NXeventdata {
            _config: config,
            group,
            latest_message_time_ms: 0,
            run_start_time_ms: 0,
            run_stop_time_ms: u64::MAX,
        };

        Ok(WriterModuleCreateResult {
            module,
            subscriptions,
        })
    }
}
