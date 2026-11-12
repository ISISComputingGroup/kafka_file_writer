//! Writer module for run duration datasets.
//!
//! This module adds the following datasets to it's parent group:
//! - `start_time`
//! - `end_time`
//! - `collection_time`
//! - `duration`

use crate::hdf::attr::add_simple_string_attribute;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::traits::KafkaMessageMeta;
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleError,
    WriterModuleSpec,
};
use chrono::{DateTime, Utc};
use hdf5::Dataset;
use hdf5::types::VarLenAscii;
use isis_streaming_data_types::DeserializedMessage;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use serde::Deserialize;

type CollectionTimeType = f32;
type DurationType = f32;

#[derive(Debug)]
pub struct RunTimeMetadata {
    start_time_ms: u64,
    stop_time_ms: Option<u64>,
    start_time_dataset: Dataset,
    end_time_dataset: Dataset,
    duration_dataset: Dataset,
    collection_time_dataset: Dataset,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunTimeMetadataConfig {}

impl WriterModule for RunTimeMetadata {
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
        message: &RunStop,
    ) -> Result<(), WriterModuleError> {
        self.stop_time_ms = Some(message.stop_time());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriterModuleError> {
        write_timestamp_to_dataset(&self.start_time_dataset, self.start_time_ms)?;
        write_timestamp_to_dataset(
            &self.end_time_dataset,
            self.stop_time_ms.unwrap_or(u64::MAX),
        )?;

        // TODO: the two datasets below aren't written correctly at the moment: they should
        //       exclude paused and runcontrol'd time, rather than simply the difference between
        //       start and end time.
        let duration =
            (self.stop_time_ms.unwrap_or(u64::MAX) - self.start_time_ms) as DurationType / 1000.;
        self.duration_dataset
            .write_slice(&[duration], ..)
            .err_dataset(&self.duration_dataset)?;

        let collection_time = (self.stop_time_ms.unwrap_or(u64::MAX) - self.start_time_ms)
            as CollectionTimeType
            / 1000.;
        self.collection_time_dataset
            .write_slice(&[collection_time], ..)
            .err_dataset(&self.collection_time_dataset)?;

        Ok(())
    }
}

fn write_timestamp_to_dataset(dataset: &Dataset, timestamp: u64) -> Result<(), WriterModuleError> {
    let timestamp: i64 = timestamp.try_into().unwrap_or(i64::MAX);
    let dt = DateTime::from_timestamp_millis(timestamp).unwrap_or(DateTime::<Utc>::MAX_UTC);
    let iso8601 = format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ"));

    dataset
        .write_slice(
            &[VarLenAscii::from_ascii(&iso8601).err_dataset(&dataset)?],
            ..,
        )
        .err_dataset(&dataset)?;
    Ok(())
}

impl WriterModuleSpec for RunTimeMetadata {
    type Config = RunTimeMetadataConfig;

    const NAME: &'static str = "RunTimeMetadata";

    fn create(
        group: hdf5::Group,
        run_start_parameters: &RunStartParameters,
        _config: Self::Config,
    ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
        let start_time_dataset = group
            .new_dataset::<VarLenAscii>()
            .shape((1,))
            .create("start_time")
            .err_parent_dataset(&group, "start_time")?;

        add_simple_string_attribute(&start_time_dataset, "units", "ISO8601")?;

        let end_time_dataset = group
            .new_dataset::<VarLenAscii>()
            .shape((1,))
            .create("end_time")
            .err_parent_dataset(&group, "end_time")?;

        add_simple_string_attribute(&end_time_dataset, "units", "ISO8601")?;

        write_timestamp_to_dataset(&start_time_dataset, run_start_parameters.start_time_ms)?;
        write_timestamp_to_dataset(
            &end_time_dataset,
            run_start_parameters.stop_time_ms.unwrap_or(u64::MAX),
        )?;

        let duration_dataset = group
            .new_dataset::<DurationType>()
            .shape((1,))
            .create("duration")
            .err_parent_dataset(&group, "duration")?;

        duration_dataset
            .write_slice(&[0.], ..)
            .err_dataset(&duration_dataset)?;

        add_simple_string_attribute(&duration_dataset, "units", "second")?;

        let collection_time_dataset = group
            .new_dataset::<CollectionTimeType>()
            .shape((1,))
            .create("collection_time")
            .err_parent_dataset(&group, "collection_time")?;

        collection_time_dataset
            .write_slice(&[0.], ..)
            .err_dataset(&collection_time_dataset)?;

        add_simple_string_attribute(&collection_time_dataset, "units", "second")?;

        let module = RunTimeMetadata {
            start_time_ms: run_start_parameters.start_time_ms,
            stop_time_ms: run_start_parameters.stop_time_ms,
            start_time_dataset,
            end_time_dataset,
            duration_dataset,
            collection_time_dataset,
        };

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
}
