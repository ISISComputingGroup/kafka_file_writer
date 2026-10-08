//! Interface to the job-pool, for acquiring a run to write.
use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::stream::scope::with_subscription_to_job_pool;
use crate::stream::traits::Stream;
use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
use log::{debug, warn};
use miette::Report;
use rdkafka::Message;
use rdkafka::consumer::CommitMode;
use std::thread;
use std::time::Duration;

pub fn wait_for_run_start(
    config: &GlobalConfig,
    consumer: &impl Stream,
) -> Result<RunStartParameters, FileWriterError> {
    with_subscription_to_job_pool(consumer, &config.job_pool_topic, || {
        // Loop waiting for a new file-writing request to come in.
        loop {
            match consumer.poll(Duration::from_millis(config.job_pool_consumer_poll_time_ms)) {
                Some(Ok(msg)) => {
                    consumer
                        .commit_message(&msg, CommitMode::Sync)
                        .map_err(FileWriterError::StreamError)?;

                    if let Some(payload) = msg.payload() {
                        let parsed_msg = deserialize_message(payload);

                        match parsed_msg {
                            Ok(DeserializedMessage::RunStartPl72(rs)) => {
                                debug!(
                                    "Received pl72 (run start) message on topic='{}', partition='{}', offset='{}', job_id='{:?}'",
                                    msg.topic(),
                                    msg.partition(),
                                    msg.offset(),
                                    rs.job_id()
                                );
                                return RunStartParameters::from_pl72(&rs, &msg);
                            }
                            Ok(DeserializedMessage::RunStop6s4t(_)) => {
                                // Ignore
                            }
                            Ok(_) => {
                                warn!(
                                    "Unexpected message type on control topic ignored: topic='{}', partition='{}', offset='{}'",
                                    msg.topic(),
                                    msg.partition(),
                                    msg.offset()
                                )
                            }
                            Err(_) => {
                                warn!(
                                    "Got message which could not be deserialized on topic='{}', partition='{}', offset='{}'",
                                    msg.topic(),
                                    msg.partition(),
                                    msg.offset()
                                );
                            }
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
                    thread::sleep(Duration::from_millis(config.stream_error_backoff_ms));
                }
                None => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::fake::FakeStream;

    #[test]
    fn test_wait_for_run_start() {
        let consumer = FakeStream::default();

        consumer.append_runinfo_message_with_payload(Some(b"nonsense".to_vec()));
        consumer.append_runinfo_run_start_message();
        consumer.append_runinfo_message_with_payload(Some(b"a different message".to_vec()));

        let config = GlobalConfig::test_config();
        let params = wait_for_run_start(&config, &consumer).expect("wait_for_run_start failed");

        // Run start should have been consumed, but "a different message" should remain.
        assert_eq!(consumer.messages.borrow().len(), 1);

        assert_eq!(params.start_time_ms, FakeStream::TEST_START_TIME);
        assert_eq!(params.stop_time_ms, u64::MAX);
        assert_eq!(
            format!("{}", params.filename.display()),
            FakeStream::TEST_FILENAME
        );
        assert_eq!(params.nexus_structure, FakeStream::TEST_STRUCTURE);
        assert_eq!(params.metadata, None);
        assert_eq!(params.n_periods, 1);
        assert_eq!(params.control_topic, None);
    }
}
