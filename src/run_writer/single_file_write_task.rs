//! The full write-task lifecycle for a single run.

use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::hdf::file_creator::FileCreator;
use crate::hdf::scope::with_nexus_file;
use crate::metrics::{
    INCOMING_DATA_MESSAGE_SIZE, INCOMING_DATA_MESSAGES, INCOMING_DATA_MESSAGES_FAILED_DESERIALIZE,
    INCOMING_DATA_MESSAGES_NO_PAYLOAD, KAFKA_POLL_ERRORS, SCHEMA_PROCESSING_TIMES,
};
use crate::run_writer::in_progress_file::InProgressFile;
use crate::run_writer::run_end_tracker::RunEndTracker;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::scope::with_assignment;
use crate::stream::traits::{KafkaMessageMeta, Stream};
use crate::writer_module_factories::WriterModuleFactories;
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
use isis_streaming_data_types::{DeserializedMessage, deserialize_message, get_schema_id};
use itertools::Itertools;
use log::{debug, info, warn};
use metrics::{counter, histogram};
use miette::Report;
use rdkafka::Message;
use std::iter::once;
use std::thread;
use std::time::{Duration, Instant};

pub struct SingleFileWriteTask<'a> {
    config: &'a GlobalConfig,
    writer_module_factories: &'a WriterModuleFactories,
    run_start_parameters: &'a RunStartParameters,
    run_end_tracker: RunEndTracker,
}

impl<'a> SingleFileWriteTask<'a> {
    pub fn new(
        config: &'a GlobalConfig,
        writer_module_factories: &'a WriterModuleFactories,
        run_start_parameters: &'a RunStartParameters,
    ) -> Self {
        let run_end_tracker = RunEndTracker::new(
            Duration::from_millis(config.kafka_metadata_timeout_ms),
            Duration::from_millis(config.stream_settle_time_ms),
        );

        SingleFileWriteTask {
            config,
            writer_module_factories,
            run_start_parameters,
            run_end_tracker,
        }
    }

    pub fn write_data_for_run(
        &mut self,
        consumer: &impl Stream,
        file_factory: FileCreator,
    ) -> Result<(), FileWriterError> {
        let resolved_structure = self.run_start_parameters.structure(self.config)?;

        let mut path = self.config.file_output_directory.clone();
        path.push(&self.run_start_parameters.filename);

        with_nexus_file(file_factory, &path, |file| {
            let mut in_progress_file = InProgressFile::new(
                file,
                self.run_start_parameters,
                resolved_structure,
                self.writer_module_factories,
            )?;

            let seek_timestamp: i64 = self
                .run_start_parameters
                .start_time_ms
                .saturating_sub(self.config.stream_seek_back_in_time_ms)
                .try_into()
                .map_err(|e| FileWriterError::IntegerError {
                    cause: e,
                    context: "timestamp greater than i64::MAX in seek timestamp",
                })?;

            let topics = in_progress_file
                .topics()
                .map(|s| s.to_string())
                .chain(once(
                    self.run_start_parameters
                        .control_topic(self.config)
                        .to_owned(),
                ))
                .unique()
                .collect::<Vec<_>>();

            with_assignment(
                consumer,
                &topics.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                seek_timestamp,
                Duration::from_millis(self.config.kafka_assignment_timeout_ms),
                || {
                    self.handle_messages_until_run_end(
                        consumer,
                        seek_timestamp,
                        &mut in_progress_file,
                    )
                },
            )
        })
    }

    fn handle_messages_until_run_end(
        &mut self,
        consumer: &impl Stream,
        seek_timestamp: i64,
        in_progress_file: &mut InProgressFile,
    ) -> Result<(), FileWriterError> {
        // Initialize the offsets we've 'consumed' to the offsets corresponding to our seek time.
        loop {
            match consumer.offsets_for_timestamp(
                seek_timestamp,
                Duration::from_millis(self.config.kafka_metadata_timeout_ms),
            ) {
                Ok(offsets) => {
                    self.run_end_tracker.set_initial_offsets(offsets);
                    break;
                }
                Err(e) => {
                    let report = Report::from(e);
                    warn!(
                        "Error while getting initial offsets (will retry): {:?}",
                        report
                    );
                    thread::sleep(Duration::from_millis(self.config.stream_error_backoff_ms));
                }
            }
        }

        if self.run_start_parameters.stop_time_ms != u64::MAX {
            info!("Run start included an explicit stop time at {} ms since epoch", self.run_start_parameters.stop_time_ms);
            self.run_end_tracker.on_run_stop(self.run_start_parameters.stop_time_ms, consumer)?;
        }

        loop {
            match consumer.poll(Duration::from_millis(
                self.config.data_consumer_poll_time_ms,
            )) {
                Some(Ok(msg)) => {
                    debug!(
                        "Handling message on topic='{}', partition={}, offset={}",
                        msg.topic(),
                        msg.partition(),
                        msg.offset()
                    );

                    self.handle_one_message(&msg, in_progress_file, consumer)?;
                }
                Some(Err(e)) => {
                    let report = Report::from(e);
                    warn!("Error during consumer poll (will retry): {:?}", report);
                    counter!(KAFKA_POLL_ERRORS).increment(1);
                    thread::sleep(Duration::from_millis(self.config.stream_error_backoff_ms));
                }
                None => {}
            }

            match self.run_end_tracker.poll_complete(consumer) {
                Ok(true) => {
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    let report = Report::from(e);
                    warn!(
                        "Error while polling for run completion (will retry): {:?}",
                        report
                    );
                    thread::sleep(Duration::from_millis(self.config.stream_error_backoff_ms));
                }
            }
        }

        in_progress_file.flush()?;

        Ok(())
    }

    fn handle_one_message(
        &mut self,
        msg: &impl Message,
        in_progress_file: &mut InProgressFile,
        consumer: &impl Stream,
    ) -> Result<(), FileWriterError> {
        let meta = KafkaMessageMeta {
            topic: msg.topic(),
            key: msg.key(),
            timestamp: msg.timestamp().to_millis(),
            partition: msg.partition(),
            offset: msg.offset(),
        };

        self.run_end_tracker.message_consumed(&meta);

        if let Some(payload) = msg.payload() {
            counter!(INCOMING_DATA_MESSAGES).increment(1);
            counter!(INCOMING_DATA_MESSAGE_SIZE).increment(payload.len() as u64);

            let start_time = Instant::now();

            let parsed_msg = deserialize_message(payload);
            match parsed_msg {
                Ok(DeserializedMessage::RunStop6s4t(run_stop)) => {
                    self.handle_stop(&meta, &run_stop, in_progress_file, consumer)?;
                }
                Ok(data) => in_progress_file.on_message(&meta, &data)?,
                Err(err) => {
                    warn!(
                        "Ignoring data message on topic '{}' (partition={}, offset={}, length={:?} bytes) as failed to deserialize\nCaused by: {:?}",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        msg.payload().map(|p| p.len()),
                        err
                    );
                    counter!(INCOMING_DATA_MESSAGES_FAILED_DESERIALIZE).increment(1);
                }
            }
            if let Some(schema_bytes) = get_schema_id(payload)
                && let Ok(schema) = str::from_utf8(schema_bytes)
            {
                histogram!(SCHEMA_PROCESSING_TIMES, "schema" => schema.to_owned())
                    .record(start_time.elapsed().as_secs_f64());
            }
        } else {
            warn!(
                "Message on topic='{}', partition={}, offset={} had no payload; ignoring",
                msg.topic(),
                msg.partition(),
                msg.offset()
            );
            counter!(INCOMING_DATA_MESSAGES_NO_PAYLOAD).increment(1);
        }

        Ok(())
    }

    fn handle_stop(
        &mut self,
        meta: &KafkaMessageMeta,
        run_stop: &RunStop,
        in_progress_file: &mut InProgressFile,
        consumer: &impl Stream,
    ) -> Result<(), FileWriterError> {
        if run_stop.job_id() != Some(in_progress_file.job_id()) {
            debug!(
                "Ignoring run stop with mismatched job id (got '{:?}', currently writing '{}')",
                run_stop.job_id(),
                in_progress_file.job_id()
            );
            return Ok(());
        }
        info!(
            "Accepted run-stop for Job ID '{}'",
            in_progress_file.job_id()
        );
        in_progress_file.on_run_stop(meta, run_stop)?;
        self.run_end_tracker
            .on_run_stop(run_stop.stop_time(), consumer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::fake::FakeStream;
    use crate::writer_modules::default_registry;

    #[test]
    fn test_write_data_for_run() {
        let config = GlobalConfig::test_config();

        let rsp = RunStartParameters {
            start_time_ms: 123,
            stop_time_ms: 456,
            job_id: FakeStream::TEST_JOB_ID.to_string(),
            nexus_structure: FakeStream::TEST_STRUCTURE.to_string(),
            control_topic: None,
            filename: FakeStream::TEST_FILENAME.into(),
            n_periods: 1,
            metadata: None,
            instrument_name: "UNITTEST".to_owned(),
        };

        let consumer = FakeStream::default();

        consumer.append_runinfo_message_with_payload(Some(b"nonsense".to_vec()));
        consumer.append_runinfo_message_with_payload(None);
        // Append two runstops; only the first should be consumed, after the two messages
        // above are ignored due to being invalid (these should not cause an `Err`, just
        // a warning).
        consumer.append_runinfo_run_stop_message();
        consumer.append_runinfo_run_stop_message();

        let reg = default_registry();

        let mut task = SingleFileWriteTask::new(&config, &reg, &rsp);

        task.write_data_for_run(&consumer, FileCreator::Memory)
            .unwrap();

        assert_eq!(consumer.messages.borrow().len(), 1);
    }
}
