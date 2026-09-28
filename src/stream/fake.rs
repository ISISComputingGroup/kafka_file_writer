//! Definition of a fake Kafka consumer, for use in tests and benchmarks.
use crate::stream::error::StreamError;
use crate::stream::traits::Stream;
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::{
    RunStart, RunStartArgs, finish_run_start_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::{
    RunStop, RunStopArgs, finish_run_stop_buffer,
};
use miette::Diagnostic;
use rdkafka::consumer::CommitMode;
use rdkafka::message::OwnedMessage;
use rdkafka::{Message, Timestamp};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Default)]
pub struct FakeStream {
    pub assign_from_timestamp_causes_error: bool,
    pub subscribe_causes_error: bool,
    pub unassign_causes_error: bool,
    pub commit_causes_error: bool,
    pub messages: RefCell<VecDeque<Result<OwnedMessage, StreamError>>>,
    pub topics: RefCell<Vec<String>>,
    pub is_assigned: RefCell<bool>,
}

#[derive(Debug, Error, Diagnostic, Default)]
#[error("Fake stream error")]
struct FakeStreamError {}

impl Stream for FakeStream {
    type Msg<'a> = OwnedMessage;

    fn assign_from_timestamp(
        &self,
        topics: &[&str],
        _timestamp: i64,
        _timeout: Duration,
    ) -> Result<(), StreamError> {
        if self.assign_from_timestamp_causes_error {
            Err(StreamError::AssignmentError {
                cause: Box::new(FakeStreamError::default()),
                topics: topics.iter().map(|&s| s.to_owned()).collect(),
                context: "faked error",
            })
        } else {
            self.is_assigned.replace(true);
            self.topics
                .replace(topics.iter().map(|&s| s.to_owned()).collect());
            Ok(())
        }
    }

    fn poll(&self, _timeout: Duration) -> Option<Result<Self::Msg<'_>, StreamError>> {
        self.messages.borrow_mut().pop_front()
    }

    fn subscribe(&self, topics: &[&str]) -> Result<(), StreamError> {
        if self.subscribe_causes_error {
            Err(StreamError::AssignmentError {
                cause: Box::new(FakeStreamError::default()),
                context: "FakeKafkaConsumer caused an error in subscribe",
                topics: topics.iter().map(|&s| s.to_owned()).collect(),
            })
        } else {
            self.is_assigned.replace(true);
            self.topics
                .replace(topics.iter().map(|&s| s.to_owned()).collect());
            Ok(())
        }
    }

    fn unsubscribe(&self) {
        self.is_assigned.replace(false);
        self.topics.replace(vec![]);
    }

    fn unassign(&self) -> Result<(), StreamError> {
        if self.unassign_causes_error {
            Err(StreamError::AssignmentError {
                cause: Box::new(FakeStreamError::default()),
                context: "FakeKafkaConsumer caused an error in unassign",
                topics: vec![],
            })
        } else {
            self.is_assigned.replace(false);
            self.topics.replace(vec![]);
            Ok(())
        }
    }

    fn commit_message(
        &self,
        message: &Self::Msg<'_>,
        _mode: CommitMode,
    ) -> Result<(), StreamError> {
        if self.commit_causes_error {
            Err(StreamError::CommitError {
                cause: Box::new(FakeStreamError::default()),
                topic: message.topic().to_owned(),
                partition: message.partition(),
                offset: message.offset(),
            })
        } else {
            Ok(())
        }
    }
}

impl FakeStream {
    pub const TEST_RUN_NAME: &'static str = "a_run_name";
    pub const TEST_INSTRUMENT: &'static str = "a_fake_instrument";
    pub const TEST_JOB_ID: &'static str = "a_job_id";
    pub const TEST_STRUCTURE: &'static str = "{}";
    pub const TEST_FILENAME: &'static str = "some_filename.nxs";
    pub const TEST_SERVICE_ID: &'static str = "some_service_id";
    pub const TEST_START_TIME: u64 = 123;
    pub const TEST_STOP_TIME: u64 = 456;

    fn make_run_start() -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let args = RunStartArgs {
            start_time: Self::TEST_START_TIME,
            stop_time: 0,
            run_name: Some(fbb.create_string(Self::TEST_RUN_NAME)),
            instrument_name: Some(fbb.create_string(Self::TEST_INSTRUMENT)),
            nexus_structure: Some(fbb.create_string(Self::TEST_STRUCTURE)),
            job_id: Some(fbb.create_string(Self::TEST_JOB_ID)),
            broker: None,
            service_id: Some(fbb.create_string(Self::TEST_SERVICE_ID)),
            filename: Some(fbb.create_string(Self::TEST_FILENAME)),
            n_periods: 1,
            detector_spectrum_map: None,
            metadata: None,
            control_topic: None,
        };

        let buf = RunStart::create(&mut fbb, &args);
        finish_run_start_buffer(&mut fbb, buf);
        fbb.finished_data().to_vec()
    }

    fn make_run_stop() -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let args = RunStopArgs {
            stop_time: Self::TEST_STOP_TIME,
            run_name: Some(fbb.create_string(Self::TEST_RUN_NAME)),
            job_id: Some(fbb.create_string(Self::TEST_JOB_ID)),
            service_id: Some(fbb.create_string(Self::TEST_SERVICE_ID)),
            command_id: None,
        };

        let buf = RunStop::create(&mut fbb, &args);
        finish_run_stop_buffer(&mut fbb, buf);
        fbb.finished_data().to_vec()
    }

    pub fn append_runinfo_message_with_payload(&self, payload: Option<Vec<u8>>) {
        self.messages.borrow_mut().push_back(Ok(OwnedMessage::new(
            payload,
            None,
            "runInfo".to_owned(),
            Timestamp::now(),
            0,
            0,
            None,
        )));
    }

    pub fn append_runinfo_run_start_message(&self) {
        self.append_runinfo_message_with_payload(Some(FakeStream::make_run_start()))
    }

    pub fn append_runinfo_run_stop_message(&self) {
        self.append_runinfo_message_with_payload(Some(FakeStream::make_run_stop()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::{Message, Timestamp};

    #[test]
    fn test_poll_messages_from_fake_consumer() {
        let consumer = FakeStream::default();

        consumer
            .messages
            .borrow_mut()
            .push_back(Ok(OwnedMessage::new(
                Some(b"some_payload".to_vec()),
                Some(b"some_key".to_vec()),
                "some_topic".to_owned(),
                Timestamp::now(),
                0,
                0,
                None,
            )));
        consumer
            .messages
            .borrow_mut()
            .push_back(Ok(OwnedMessage::new(
                Some(b"another_payload".to_vec()),
                Some(b"another_key".to_vec()),
                "some_topic".to_owned(),
                Timestamp::now(),
                0,
                0,
                None,
            )));

        consumer
            .messages
            .borrow_mut()
            .push_back(Err(StreamError::PollError {
                cause: Box::new(FakeStreamError::default()),
            }));

        if let Some(Ok(msg1)) = consumer.poll(Duration::from_millis(0)) {
            assert_eq!(msg1.payload(), Some(b"some_payload".as_slice()));
            assert_eq!(msg1.key(), Some(b"some_key".as_slice()));
        } else {
            panic!("msg1 failed")
        }

        if let Some(Ok(msg2)) = consumer.poll(Duration::from_millis(0)) {
            assert_eq!(msg2.payload(), Some(b"another_payload".as_slice()));
            assert_eq!(msg2.key(), Some(b"another_key".as_slice()));
        } else {
            panic!("msg2 failed")
        }

        assert!(consumer.poll(Duration::from_millis(0)).unwrap().is_err());

        // No messages left in consumer
        assert!(consumer.poll(Duration::from_millis(0)).is_none());
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let mut consumer = FakeStream {
            subscribe_causes_error: true,
            ..Default::default()
        };

        assert!(
            consumer
                .subscribe(&["foo", "bar", "baz"])
                .is_err_and(|err| { matches!(err, StreamError::AssignmentError { .. }) })
        );
        assert_eq!(consumer.topics.borrow().len(), 0);

        consumer.subscribe_causes_error = false;
        consumer.subscribe(&["foo", "bar", "baz"]).unwrap();
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unsubscribe();
        assert_eq!(consumer.topics.borrow().len(), 0);
    }

    #[test]
    fn test_assign_unassign() {
        let mut consumer = FakeStream {
            assign_from_timestamp_causes_error: true,
            ..Default::default()
        };

        assert!(
            consumer
                .assign_from_timestamp(&["foo", "bar", "baz"], 0, Duration::from_millis(1))
                .is_err_and(|err| { matches!(err, StreamError::AssignmentError { .. }) })
        );
        assert_eq!(consumer.topics.borrow().len(), 0);

        consumer.assign_from_timestamp_causes_error = false;
        consumer
            .assign_from_timestamp(&["foo", "bar", "baz"], 0, Duration::from_millis(1))
            .unwrap();
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unassign_causes_error = true;
        assert!(
            consumer
                .unassign()
                .is_err_and(|err| { matches!(err, StreamError::AssignmentError { .. }) })
        );
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unassign_causes_error = false;
        consumer.unassign().unwrap();
        assert_eq!(consumer.topics.borrow().len(), 0);
    }
}
