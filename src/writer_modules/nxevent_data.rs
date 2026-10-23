use crate::hdf::dataset::{AppendableDataset1D};
use ahash::{HashMap, HashMapExt};
use crate::hdf::attr::add_ascii_string_attribute;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::subscription::{Subscription, SubscriptionKey};
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::debug;
use serde::Deserialize;

#[derive(Debug)]
struct PartialFrame {
    reference_time: i64,
    event_id: Vec<u32>,
    event_time_offset: Vec<f32>,
}

impl PartialFrame {
    fn new(reference_time: i64) -> Self {
        PartialFrame {
            reference_time,
            event_id: vec![],
            event_time_offset: vec![],
        }
    }

    fn add_events(&mut self, event_id: impl IntoIterator<Item = i32>, event_time_offset: impl IntoIterator<Item = i32>) {
        for (id, offset) in event_id.into_iter().zip(event_time_offset.into_iter()) {
            self.event_id.push(id as u32);
            self.event_time_offset.push(offset as f32 / 1000.);
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NXeventdataConfig {
    topic: String,
}

#[derive(Debug)]
pub struct NXeventdata {
    _config: NXeventdataConfig,
    run_start_time_ms: u64,
    run_stop_time_ms: u64,
    latest_message_time_ms: u64,
    partial_frames: HashMap<(String, i32), PartialFrame>,
    event_time_zero: AppendableDataset1D<f64>,
    event_id: AppendableDataset1D<u32>,
    event_time_offset: AppendableDataset1D<f32>,
}

impl WriterModule for NXeventdata {
    fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        message: &DeserializedMessage,
    ) -> Result<(), WriterModuleError> {
        if let Some(timestamp) = meta.timestamp {
            let timestamp = timestamp.try_into().map_err(|_| WriterModuleError::InvalidData (
                "timestamp in ev44 was negative".to_owned()
            ))?;

            self.latest_message_time_ms = timestamp;
            if timestamp < self.run_start_time_ms || timestamp > self.run_stop_time_ms {
                // This writer module doesn't write messages before / after run.
                return Ok(());
            }
        }

        match message {
            DeserializedMessage::PulseMetadataPu00(msg) => {
                self.event_time_zero.append_value(msg.reference_time() as f64)?;

                let new_frame = PartialFrame::new(msg.reference_time());

                if let Some(old_frame) = self.partial_frames.insert((meta.topic.to_owned(), meta.partition), new_frame) {
                    self.event_time_zero.append_value(old_frame.reference_time as f64)?;
                    self.event_id.append_slice(&old_frame.event_id)?;
                    self.event_time_offset.append_slice(&old_frame.event_time_offset)?;
                }
            }
            DeserializedMessage::EventDataEv44(msg) => {
                let pixel_ids = msg
                    .pixel_id()
                    .ok_or_else(|| WriterModuleError::InvalidData("Missing pixel_id".to_owned()))?;

                let time_of_flight = msg.time_of_flight().ok_or_else(|| {
                    WriterModuleError::InvalidData("Missing time_of_flight".to_owned())
                })?;

                let pixel_ids_len = pixel_ids.len();
                let time_of_flight_len = time_of_flight.len();

                if pixel_ids_len != time_of_flight_len {
                    return Err(WriterModuleError::InvalidData(format!(
                        "pixel_id length {} does not match time_of_flight length {}",
                        pixel_ids_len, time_of_flight_len
                    )));
                }

                if let Some(partial_frame) = self.partial_frames.get_mut(&(meta.topic.to_owned(), meta.partition)) {
                    partial_frame.add_events(pixel_ids, time_of_flight);
                } else {
                    return Err(WriterModuleError::InvalidData("Missing partial frame for event data".to_owned()))
                }
            }
            _ => {
                debug!(
                    "nxevent_data writer got message unrecognised schema (topic={}, partition={}, offset={})",
                    meta.topic, meta.partition, meta.offset
                )
            }
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
        for frame in self.partial_frames.values_mut() {
            self.event_time_zero.append_value(frame.reference_time as f64)?;
            self.event_id.append_slice(&frame.event_id)?;
            self.event_time_offset.append_slice(&frame.event_time_offset)?;
        }
        self.partial_frames.clear();
        Ok(())
    }
}

impl WriterModuleSpec for NXeventdata {
    type Config = NXeventdataConfig;

    const NAME: &'static str = "NXevent_data";

    fn create(
        group: hdf5::Group,
        run_start_parameters: &RunStartParameters,
        config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let subscriptions = vec![Subscription::new(
            config.topic.to_owned(),
            SubscriptionKey::All,
        )];

        let event_time_zero = AppendableDataset1D::create_dataset(&group, "event_time_zero", 32000)?;
        let event_id = AppendableDataset1D::create_dataset(&group, "event_id", 32000)?;
        let event_time_offset = AppendableDataset1D::create_dataset(&group, "event_time_offset", 32000)?;

        add_ascii_string_attribute(&group, "NX_class", "NXevent_data")?;

        let module = NXeventdata {
            _config: config,
            latest_message_time_ms: 0,
            run_start_time_ms: run_start_parameters.start_time_ms,
            run_stop_time_ms: run_start_parameters.stop_time_ms,
            partial_frames: HashMap::new(),
            event_id,
            event_time_zero,
            event_time_offset,
        };

        Ok(WriterModuleCreateResult {
            module,
            subscriptions,
        })
    }
}
