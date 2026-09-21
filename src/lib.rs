use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::hdf::file_factory::FileFactory;
use crate::hdf::scope::with_nexus_file;
use crate::run_start_parameters::RunStartParameters;
use crate::run_writer::in_progress_file::InProgressFile;
use crate::stream::kafka::{
    KafkaStream, make_data_kafka_client_config, make_job_pool_kafka_client_config,
};
use crate::stream::scope::{with_assignment, with_subscription_to_job_pool};
use crate::stream::traits::{KafkaMessageMeta, Stream};
use crate::writer_module_registry::WriterModuleFactories;
use crate::writer_modules::default_registry;
use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
use log::{debug, error, info, warn};
use miette::Report;
use rdkafka::Message;
use rdkafka::consumer::CommitMode;
use std::iter::once;
use std::thread;
use std::time::Duration;

pub mod config;
pub mod error;
pub mod hdf;
pub mod nexus_structure;
pub mod run_start_parameters;
pub mod run_writer;
pub mod stream;
pub mod subscription;
pub mod writer_module;
pub mod writer_module_registry;
pub mod writer_modules;

pub fn write_files_forever(config: &GlobalConfig) -> Result<(), FileWriterError> {
    let job_pool_kafka_config = make_job_pool_kafka_client_config(config);

    let job_pool_consumer = KafkaStream::from_config(&job_pool_kafka_config).map_err(|e| {
        FileWriterError::StreamError {
            cause: e,
            topic: None,
            partition: None,
            offset: None,
        }
    })?;

    let writer_modules = default_registry();

    loop {
        let result = write_one_file(
            config,
            &job_pool_consumer,
            &writer_modules,
            FileFactory::Disk,
        );

        if config.one_file_only {
            info!("Filewriter operating in single-file mode; exiting main loop");
            return result;
        }

        if let Err(err) = result {
            let report: Report = Report::from(err);
            error!(
                "Failed to write file (backing off for {} seconds): {:?}",
                config.error_backoff_s, report
            );
            // Backoff to avoid tight loop if the error was something that will
            // happen again on next file-write attempt, e.g. Kafka unavailable.
            thread::sleep(Duration::from_secs(config.error_backoff_s));
        }
    }
}

fn write_one_file(
    config: &GlobalConfig,
    job_pool_consumer: &impl Stream,
    writer_modules: &WriterModuleFactories,
    file_factory: FileFactory,
) -> Result<(), FileWriterError> {
    info!("Waiting for run start on topic '{}'", config.job_pool_topic);
    let run_start_parameters = wait_for_run_start(config, job_pool_consumer)?;
    info!(
        "Run start job_id='{}' filename='{}' start_time='{}'",
        run_start_parameters.job_id, run_start_parameters.filename, run_start_parameters.start_time
    );

    // Make a fresh data consumer (with a fresh UUID for group.id) for each run.
    let data_kafka_config = make_data_kafka_client_config(config);
    let data_consumer =
        KafkaStream::from_config(&data_kafka_config).map_err(|e| FileWriterError::StreamError {
            cause: e,
            topic: None,
            partition: None,
            offset: None,
        })?;

    write_data_for_run(
        config,
        &data_consumer,
        &run_start_parameters,
        writer_modules,
        file_factory,
    )?;
    info!(
        "Successfully wrote job_id='{}'",
        run_start_parameters.job_id
    );
    Ok(())
}

fn wait_for_run_start(
    config: &GlobalConfig,
    consumer: &impl Stream,
) -> Result<RunStartParameters, FileWriterError> {
    with_subscription_to_job_pool(consumer, &config.job_pool_topic, || {
        // Loop waiting for a new file-writing request to come in.
        loop {
            match consumer.poll(Duration::from_millis(10)) {
                Some(Ok(msg)) => {
                    consumer
                        .commit_message(&msg, CommitMode::Sync)
                        .map_err(|e| FileWriterError::from_stream_message(e, &msg))?;

                    if let Some(payload) = msg.payload() {
                        let parsed_msg = deserialize_message(payload);

                        if let Ok(DeserializedMessage::RunStartPl72(rs)) = parsed_msg {
                            debug!(
                                "Received pl72 (run start) message on topic='{}', partition='{}', offset='{}', job_id='{:?}'",
                                msg.topic(),
                                msg.partition(),
                                msg.offset(),
                                rs.job_id()
                            );
                            return RunStartParameters::from_pl72(&rs, &msg);
                        }
                    } else {
                        warn!(
                            "Message without payload on control topic='{}', partition='{}', offset='{}'",
                            msg.topic(),
                            msg.partition(),
                            msg.offset()
                        )
                    }
                }
                Some(Err(e)) => {
                    let report = Report::from(e);
                    warn!("Error during consumer poll (will retry): {:?}", report);
                    thread::sleep(Duration::from_millis(100)); // TODO: config
                }
                None => {}
            }
        }
    })
}

fn handle_one_message(
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
            Err(_) => {
                warn!(
                    "Ignoring data message on topic '{}' (key={:?}, partition={}, offset={}, length={:?} bytes) as failed to deserialize",
                    msg.topic(),
                    msg.key(),
                    msg.partition(),
                    msg.offset(),
                    msg.payload().map(|p| p.len())
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

fn handle_messages_until_run_stop(
    consumer: &impl Stream,
    in_progress_file: &mut InProgressFile,
) -> Result<(), FileWriterError> {
    while !in_progress_file.finished() {
        match consumer.poll(Duration::from_millis(5000)) {
            // TODO: configurable poll timeout
            Some(Ok(msg)) => {
                debug!(
                    "Handling message on topic='{}', partition={}, offset={}",
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                );

                handle_one_message(&msg, in_progress_file)?;
            }
            Some(Err(e)) => {
                let report = Report::from(e);
                warn!("Error during consumer poll (will retry): {:?}", report);
                thread::sleep(Duration::from_millis(100)); // TODO: config
            }
            None => {}
        }
    }

    Ok(())
}

fn write_data_for_run(
    config: &GlobalConfig,
    consumer: &impl Stream,
    run_start_parameters: &RunStartParameters,
    writer_module_factories: &WriterModuleFactories,
    file_factory: FileFactory,
) -> Result<(), FileWriterError> {
    let resolved_structure = run_start_parameters.structure(config)?;

    let mut path = config.file_output_directory.clone();
    path.push(&run_start_parameters.filename);

    with_nexus_file(file_factory, &path, |file| {
        let mut in_progress_file = InProgressFile::new(
            file,
            run_start_parameters,
            resolved_structure,
            writer_module_factories,
        )?;

        let seek_timestamp: i64 = run_start_parameters
            .start_time
            .saturating_sub(config.back_in_time_ms) as i64;

        let topics = in_progress_file
            .topics()
            .map(|s| s.to_string())
            .chain(once(run_start_parameters.control_topic(config).to_owned()))
            .collect::<Vec<_>>();

        with_assignment(
            consumer,
            &topics.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            seek_timestamp,
            Duration::from_secs(10),
            || handle_messages_until_run_stop(consumer, &mut in_progress_file),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flatbuffers::FlatBufferBuilder;
    use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::{
        RunStart, RunStartArgs, finish_run_start_buffer,
    };
    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;
    use stream::fake::FakeStream;

    fn make_run_start() -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let args = RunStartArgs {
            start_time: 123,
            stop_time: 456,
            run_name: Some(fbb.create_string("a_run_name")),
            instrument_name: Some(fbb.create_string("a_fake_instrument")),
            nexus_structure: Some(fbb.create_string("{}")),
            job_id: Some(fbb.create_string("a_job_id")),
            broker: None,
            service_id: Some(fbb.create_string("some_service_id")),
            filename: Some(fbb.create_string("filename")),
            n_periods: 1,
            detector_spectrum_map: None,
            metadata: None,
            control_topic: None,
        };

        let buf = RunStart::create(&mut fbb, &args);
        finish_run_start_buffer(&mut fbb, buf);
        fbb.finished_data().to_vec()
    }

    #[test]
    fn test_wait_for_run_start() {
        let consumer = FakeStream::default();

        consumer
            .messages
            .borrow_mut()
            .push_back(Ok(OwnedMessage::new(
                Some(b"nonsense".to_vec()),
                None,
                "runInfo".to_owned(),
                Timestamp::now(),
                0,
                0,
                None,
            )));

        consumer
            .messages
            .borrow_mut()
            .push_back(Ok(OwnedMessage::new(
                Some(make_run_start()),
                None,
                "runInfo".to_owned(),
                Timestamp::now(),
                0,
                0,
                None,
            )));

        consumer
            .messages
            .borrow_mut()
            .push_back(Ok(OwnedMessage::new(
                Some(b"a different message".to_vec()),
                None,
                "runInfo".to_owned(),
                Timestamp::now(),
                0,
                0,
                None,
            )));

        let config = GlobalConfig::default();
        let params = wait_for_run_start(&config, &consumer).expect("wait_for_run_start failed");

        // Run start should have been consumed, but "a different message" should remain.
        assert_eq!(consumer.messages.borrow().len(), 1);

        assert_eq!(params.start_time, 123);
        assert_eq!(params.stop_time, 456);
        assert_eq!(params.filename, "filename");
        assert_eq!(params.nexus_structure, "{}");
        assert_eq!(params.metadata, None);
        assert_eq!(params.n_periods, 1);
        assert_eq!(params.control_topic, None);
    }
}
