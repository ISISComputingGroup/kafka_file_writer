//! The full write-task lifecycle for a single run.
use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::hdf::file_factory::FileFactory;
use crate::hdf::scope::with_nexus_file;
use crate::run_writer::in_progress_file::InProgressFile;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::scope::with_assignment;
use crate::stream::traits::{KafkaMessageMeta, Stream};
use crate::writer_module_factories::WriterModuleFactories;
use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
use itertools::Itertools;
use log::{debug, warn};
use miette::Report;
use rdkafka::Message;
use std::iter::once;
use std::thread;
use std::time::Duration;

pub struct Task<'a> {
    pub config: &'a GlobalConfig,
    pub writer_module_factories: &'a WriterModuleFactories,
    pub run_start_parameters: &'a RunStartParameters,
}

impl Task<'_> {
    pub fn write_data_for_run(
        &self,
        consumer: &impl Stream,
        file_factory: FileFactory,
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
                .saturating_sub(self.config.back_in_time_ms)
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
                || self.handle_messages_until_run_stop(consumer, &mut in_progress_file),
            )
        })
    }

    fn handle_messages_until_run_stop(
        &self,
        consumer: &impl Stream,
        in_progress_file: &mut InProgressFile,
    ) -> Result<(), FileWriterError> {
        while !in_progress_file.finished() {
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

                    self.handle_one_message(&msg, in_progress_file)?;
                }
                Some(Err(e)) => {
                    let report = Report::from(e);
                    warn!("Error during consumer poll (will retry): {:?}", report);
                    thread::sleep(Duration::from_millis(self.config.stream_error_backoff_ms));
                }
                None => {}
            }
        }

        Ok(())
    }

    fn handle_one_message(
        &self,
        msg: &impl Message,
        in_progress_file: &mut InProgressFile,
    ) -> Result<(), FileWriterError> {
        if let Some(payload) = msg.payload() {
            let parsed_msg = deserialize_message(payload);

            let meta = KafkaMessageMeta {
                topic: msg.topic(),
                key: msg.key(),
                timestamp: msg.timestamp().to_millis(),
                partition: msg.partition(),
                offset: msg.offset(),
            };

            match parsed_msg {
                Ok(DeserializedMessage::RunStop6s4t(run_stop)) => {
                    if run_stop.job_id() == Some(in_progress_file.job_id()) {
                        warn!("Job stop logic not fully handled"); // TODO
                        in_progress_file.on_run_stop(&meta, &run_stop)?;
                    } else {
                        debug!(
                            "Ignoring run stop with mismatched job id (got '{:?}', currently writing '{}')",
                            run_stop.job_id(),
                            in_progress_file.job_id()
                        );
                    }
                }
                Ok(data) => in_progress_file.on_message(&meta, &data)?,
                Err(err) => {
                    warn!(
                        "Ignoring data message on topic '{}' (key={:?}, partition={}, offset={}, length={:?} bytes) as failed to deserialize\nCaused by: {:?}",
                        msg.topic(),
                        msg.key(),
                        msg.partition(),
                        msg.offset(),
                        msg.payload().map(|p| p.len()),
                        err
                    );
                }
            }
        } else {
            warn!(
                "Message on topic='{}', partition={}, offset={} had no payload; ignoring",
                msg.topic(),
                msg.partition(),
                msg.offset()
            );
        }

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
            filename: FakeStream::TEST_FILENAME.to_string(),
            n_periods: 1,
            metadata: None,
        };

        let consumer = FakeStream::default();

        consumer.append_runinfo_message_with_payload(Some(b"nonsense".to_vec()));
        consumer.append_runinfo_message_with_payload(None);
        // Append two runstops; only the first should be consumed, after the two messages
        // above are ignored due to being invalid (these should not cause an `Err`, just
        // a warning).
        consumer.append_runinfo_run_stop_message();
        consumer.append_runinfo_run_stop_message();

        let task = Task {
            config: &config,
            writer_module_factories: &default_registry(),
            run_start_parameters: &rsp,
        };

        task.write_data_for_run(&consumer, FileFactory::Memory)
            .unwrap();

        assert_eq!(consumer.messages.borrow().len(), 1);
    }
}
