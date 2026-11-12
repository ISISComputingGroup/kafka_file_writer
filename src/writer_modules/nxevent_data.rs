//! Writer-module for an `NXevent_data` group.
//!
//! This is the main writer module for event-mode neutron data.
use crate::hdf::attr::add_simple_string_attribute;
use crate::hdf::dataset::AppendableDataset1D;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::subscription::{Subscription, SubscriptionKey};
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use ahash::{HashMap, HashMapExt};
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use log::debug;
use serde::Deserialize;

#[derive(Debug)]
struct PartialFrame {
    reference_time_s_relative_to_run_start: f64,
    event_id: Vec<u32>,
    event_time_offset: Vec<f32>,
}

impl PartialFrame {
    fn new(reference_time_relative_to_run_start: f64) -> Self {
        PartialFrame {
            reference_time_s_relative_to_run_start: reference_time_relative_to_run_start,
            event_id: vec![],
            event_time_offset: vec![],
        }
    }

    fn add_events(
        &mut self,
        event_id: impl IntoIterator<Item = i32>,
        event_time_offset: impl IntoIterator<Item = i32>,
    ) {
        for (id, offset) in event_id.into_iter().zip(event_time_offset) {
            // If we get a negative detector ID, map it to spectrum 0 (junk data)
            self.event_id.push(id.try_into().unwrap_or(0));
            self.event_time_offset.push(offset as f32 / 1000.);
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NXeventdataConfig {
    topic: String,
}

type EventFrameNumberType = i32;
type EventIndexType = u64;
type EventTimeZeroType = f64;
type EventIdType = u32;
type EventTimeOffsetType = f32;
type TotalCountsType = u64;

#[derive(Debug)]
pub struct NXeventdata {
    run_start_time_ms: u64,
    run_stop_time_ms: Option<u64>,
    partial_frames: HashMap<(String, i32), PartialFrame>,
    current_event_index: u64,
    current_event_frame_number: i32,
    event_frame_number: AppendableDataset1D<EventFrameNumberType>,
    event_index: AppendableDataset1D<EventIndexType>,
    event_time_zero: AppendableDataset1D<EventTimeZeroType>,
    event_id: AppendableDataset1D<EventIdType>,
    event_time_offset: AppendableDataset1D<EventTimeOffsetType>,
    total_counts: AppendableDataset1D<TotalCountsType>,
}

impl NXeventdata {
    fn run_start_time_ns(&self) -> i64 {
        (self.run_start_time_ms.try_into().unwrap_or(i64::MAX)).saturating_mul(1_000_000)
    }

    fn run_stop_time_ns(&self) -> i64 {
        (self
            .run_stop_time_ms
            .and_then(|t| t.try_into().ok())
            .unwrap_or(i64::MAX))
        .saturating_mul(1_000_000)
    }
}

impl WriterModule for NXeventdata {
    fn on_message(
        &mut self,
        meta: &KafkaMessageMeta,
        message: &DeserializedMessage,
    ) -> Result<(), WriterModuleError> {
        match message {
            DeserializedMessage::PulseMetadataPu00(msg) => {
                let reference_time_ns = msg.reference_time();
                if reference_time_ns < self.run_start_time_ns()
                    || reference_time_ns > self.run_stop_time_ns()
                {
                    // Don't write frames from before run start time.
                    return Ok(());
                }
                let time_s_relative_to_run_start =
                    (msg.reference_time() - self.run_start_time_ns()) as f64 / 1_000_000_000.;
                let new_frame = PartialFrame::new(time_s_relative_to_run_start);

                if let Some(old_frame) = self
                    .partial_frames
                    .insert((meta.topic.to_owned(), meta.partition), new_frame)
                {
                    self.event_index.append_value(self.current_event_index)?;
                    self.current_event_index += old_frame.event_id.len() as u64;

                    self.event_frame_number
                        .append_value(self.current_event_frame_number)?;
                    self.current_event_frame_number += 1;

                    self.event_time_zero
                        .append_value(old_frame.reference_time_s_relative_to_run_start)?;
                    self.event_id.append_slice(&old_frame.event_id)?;
                    self.event_time_offset
                        .append_slice(&old_frame.event_time_offset)?;
                }
            }
            DeserializedMessage::EventDataEv44(msg) => {
                if msg.reference_time().len() != 1 {
                    return Err(WriterModuleError::InvalidData(
                        "multiple reference times per message not supported".to_owned(),
                    ));
                }
                let reference_time_ns = msg.reference_time().get(0);
                if reference_time_ns < self.run_start_time_ns()
                    || reference_time_ns > self.run_stop_time_ns()
                {
                    // Don't write frames from outside run start/stop times.
                    return Ok(());
                }

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

                if let Some(partial_frame) = self
                    .partial_frames
                    .get_mut(&(meta.topic.to_owned(), meta.partition))
                {
                    partial_frame.add_events(pixel_ids, time_of_flight);
                } else {
                    return Err(WriterModuleError::InvalidData(
                        "Missing partial frame for event data".to_owned(),
                    ));
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
        self.run_stop_time_ms = Some(message.stop_time());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriterModuleError> {
        // TODO: this isn't quite right; if we flush mid-run (e.g. autosave)
        //       we shouldn't be writing/clearing partial frames, but if we
        //       flush at the end of a run we do. Might need to add a flag
        //       to the trait.
        for frame in self.partial_frames.values_mut() {
            self.event_frame_number
                .append_value(self.current_event_frame_number)?;
            self.current_event_frame_number += 1;

            self.event_index.append_value(self.current_event_index)?;
            self.current_event_index += frame.event_id.len() as u64;

            self.event_time_zero
                .append_value(frame.reference_time_s_relative_to_run_start)?;
            self.event_id.append_slice(&frame.event_id)?;
            self.event_time_offset
                .append_slice(&frame.event_time_offset)?;
        }

        self.total_counts
            .overwrite_with_slice(&[self.current_event_index])?;

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

        let event_frame_number =
            AppendableDataset1D::create_dataset(&group, "event_frame_number", 4 * 1024)?;
        let event_index = AppendableDataset1D::create_dataset(&group, "event_index", 4 * 1024)?;
        let event_time_zero =
            AppendableDataset1D::create_dataset(&group, "event_time_zero", 4 * 1024)?;
        let event_id = AppendableDataset1D::create_dataset(&group, "event_id", 4 * 1024 * 1024)?;
        let event_time_offset =
            AppendableDataset1D::create_dataset(&group, "event_time_offset", 4 * 1024 * 1024)?;
        let total_counts = AppendableDataset1D::create_dataset(&group, "total_counts", 1)?;

        add_simple_string_attribute(&group, "NX_class", "NXevent_data")?;
        add_simple_string_attribute(event_time_offset.raw_dataset(), "units", "microsecond")?;
        add_simple_string_attribute(event_time_zero.raw_dataset(), "units", "second")?;

        add_simple_string_attribute(
            event_time_zero.raw_dataset(),
            "offset",
            &run_start_parameters.start_time_as_iso8601(),
        )?;

        let module = NXeventdata {
            run_start_time_ms: run_start_parameters.start_time_ms,
            run_stop_time_ms: run_start_parameters.stop_time_ms,
            partial_frames: HashMap::new(),
            current_event_index: 0,
            current_event_frame_number: 0,
            event_frame_number,
            event_index,
            event_id,
            event_time_zero,
            event_time_offset,
            total_counts,
        };

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
    use crate::messages::fake::{make_ev44, make_pu00};
    use hdf5::H5Type;
    use hdf5::types::VarLenAscii;
    use isis_streaming_data_types::deserialize_message;
    use ndarray::Array1;

    const GROUP: &str = "test_group";
    const TOPIC: &str = "test_topic";

    const RUN_DURATION_MS: u64 = 1_000_000;
    const RUN_START_MS: u64 = 1793876411456;
    const RUN_START_NS: u64 = RUN_START_MS * 1_000_000;
    const RUN_STOP_MS: u64 = RUN_START_MS + RUN_DURATION_MS;
    const RUN_STOP_NS: u64 = RUN_STOP_MS * 1_000_000;

    fn make_basic_module(
        start_time_ms: u64,
        stop_time_ms: u64,
    ) -> (hdf5::File, WriterModuleCreateResult<NXeventdata>) {
        let file = FileCreator::Memory.create_file("unittest").unwrap();
        let group = file.create_group(GROUP).unwrap();

        let create_result = NXeventdata::create(
            group,
            &RunStartParameters {
                instrument_name: "GREATINSTRUMENT".to_string(),
                start_time_ms,
                stop_time_ms: Some(stop_time_ms),
                ..Default::default()
            },
            NXeventdataConfig {
                topic: TOPIC.to_string(),
            },
        )
        .unwrap();

        (file, create_result)
    }

    fn read_total_events(file: &hdf5::File) -> TotalCountsType {
        file.group(GROUP)
            .unwrap()
            .dataset("total_counts")
            .unwrap()
            .read_slice_1d(0..1)
            .unwrap()[0]
    }

    fn read_array<T: H5Type>(file: &hdf5::File, dataset_name: &str) -> Array1<T> {
        file.group(GROUP)
            .unwrap()
            .dataset(dataset_name)
            .unwrap()
            .read_slice_1d::<T, _>(..)
            .unwrap()
    }

    fn read_string_attr(file: &hdf5::File, dataset_name: &str, attr_name: &str) -> String {
        file.group(GROUP)
            .unwrap()
            .dataset(dataset_name)
            .unwrap()
            .attr(attr_name)
            .unwrap()
            .read_scalar::<VarLenAscii>()
            .unwrap()
            .to_string()
    }

    fn send_messages(module: &mut NXeventdata, messages: impl Iterator<Item = (i32, Vec<u8>)>) {
        for (partition, message) in messages.into_iter() {
            module
                .on_message(
                    &KafkaMessageMeta {
                        topic: TOPIC,
                        offset: 0,
                        partition,
                        key: None,
                        timestamp: Some(0),
                    },
                    &deserialize_message(&message).unwrap(),
                )
                .unwrap();
        }
        module.flush().unwrap();
    }

    #[test]
    fn test_no_events_written() {
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        create_result.module.flush().unwrap();

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            0
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 0);
        assert_eq!(read_array::<EventIndexType>(&file, "event_index").len(), 0);
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            0
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            0
        );
        assert_eq!(read_total_events(&file), 0);

        assert_eq!(
            read_string_attr(&file, "event_time_zero", "offset"),
            "2026-11-05T11:00:11.456Z"
        );
        assert_eq!(
            read_string_attr(&file, "event_time_zero", "units"),
            "second"
        );
        assert_eq!(
            read_string_attr(&file, "event_time_offset", "units"),
            "microsecond"
        );
    }

    #[test]
    fn test_one_frame() {
        let messages = [
            (
                0,
                make_pu00(
                    (RUN_START_NS + 10).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                0,
                make_ev44(
                    (RUN_START_NS + 10).try_into().unwrap(),
                    &[50_000, 100_000],
                    &[1, 2],
                ),
            ),
        ];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number")
                .as_slice()
                .unwrap(),
            &[0]
        );
        assert_eq!(
            read_array::<EventIdType>(&file, "event_id")
                .as_slice()
                .unwrap(),
            &[1, 2]
        );
        assert_eq!(
            read_array::<EventIndexType>(&file, "event_index")
                .as_slice()
                .unwrap(),
            &[0]
        );
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset")
                .as_slice()
                .unwrap(),
            &[50., 100.]
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero")
                .as_slice()
                .unwrap(),
            &[10. / 1_000_000_000.]
        );
        assert_eq!(read_total_events(&file), 2);
    }

    #[test]
    fn test_multiple_frames() {
        let messages = [
            (
                0,
                make_pu00(
                    (RUN_START_NS + 10).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                1,
                make_pu00(
                    (RUN_START_NS + 20).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                0,
                make_ev44(
                    (RUN_START_NS + 10).try_into().unwrap(),
                    &[50_000, 100_000],
                    &[1, 2],
                ),
            ),
            (
                1,
                make_ev44(
                    (RUN_START_NS + 20).try_into().unwrap(),
                    &[150_000, 200_000],
                    &[3, 4],
                ),
            ),
        ];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            2
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 4);
        assert_eq!(
            read_array::<EventIndexType>(&file, "event_index")
                .as_slice()
                .unwrap(),
            &[0, 2]
        );
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            4
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            2
        );
        assert_eq!(read_total_events(&file), 4);
    }

    #[test]
    fn test_frame_before_run_not_written() {
        let messages = [
            (
                0,
                make_pu00(
                    (RUN_START_NS - 1).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                0,
                make_ev44(
                    (RUN_START_NS - 1).try_into().unwrap(),
                    &[50_000, 100_000],
                    &[1, 2],
                ),
            ),
        ];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            0
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 0);
        assert_eq!(read_array::<EventIndexType>(&file, "event_index").len(), 0);
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            0
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            0
        );
        assert_eq!(read_total_events(&file), 0);
    }

    #[test]
    fn test_frame_after_run_not_written() {
        let messages = [
            (
                0,
                make_pu00(
                    (RUN_STOP_NS + 1).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                0,
                make_ev44(
                    (RUN_STOP_NS + 1).try_into().unwrap(),
                    &[50_000, 100_000],
                    &[1, 2],
                ),
            ),
        ];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            0
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 0);
        assert_eq!(read_array::<EventIndexType>(&file, "event_index").len(), 0);
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            0
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            0
        );
        assert_eq!(read_total_events(&file), 0);
    }

    #[test]
    fn test_frame_with_no_events() {
        let messages = [(
            0,
            make_pu00(
                (RUN_START_NS + 1).try_into().unwrap(),
                Some(0),
                Some(0xFFFFFFFF),
                Some(0.0),
            ),
        )];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            1
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 0);
        assert_eq!(read_array::<EventIndexType>(&file, "event_index").len(), 1);
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            0
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            1
        );
        assert_eq!(read_total_events(&file), 0);
    }

    #[test]
    fn test_frame_with_empty_ev44() {
        let messages = [
            (
                0,
                make_pu00(
                    (RUN_START_NS + 1).try_into().unwrap(),
                    Some(0),
                    Some(0xFFFFFFFF),
                    Some(0.0),
                ),
            ),
            (
                0,
                make_ev44((RUN_START_NS + 1).try_into().unwrap(), &[], &[]),
            ),
        ];
        let (file, mut create_result) = make_basic_module(RUN_START_MS, RUN_STOP_MS);
        send_messages(&mut create_result.module, messages.into_iter());

        assert_eq!(
            read_array::<EventFrameNumberType>(&file, "event_frame_number").len(),
            1
        );
        assert_eq!(read_array::<EventIdType>(&file, "event_id").len(), 0);
        assert_eq!(read_array::<EventIndexType>(&file, "event_index").len(), 1);
        assert_eq!(
            read_array::<EventTimeOffsetType>(&file, "event_time_offset").len(),
            0
        );
        assert_eq!(
            read_array::<EventTimeZeroType>(&file, "event_time_zero").len(),
            1
        );
        assert_eq!(read_total_events(&file), 0);
    }
}
