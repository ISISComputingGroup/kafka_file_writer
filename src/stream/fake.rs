//! Definition of a fake Kafka consumer, for use in tests and benchmarks.
use crate::stream::traits::{Stream, StreamError};
use rdkafka::consumer::CommitMode;
use rdkafka::message::OwnedMessage;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct FakeStream {
    pub assign_from_timestamp_causes_error: bool,
    pub subscribe_causes_error: bool,
    pub unassign_causes_error: bool,
    pub commit_causes_error: bool,
    pub messages: RefCell<VecDeque<Result<OwnedMessage, StreamError>>>,
    pub topics: RefCell<Vec<String>>,
}

impl Stream for FakeStream {
    type Msg<'a> = OwnedMessage;

    fn assign_from_timestamp(
        &self,
        topics: &[&str],
        _timestamp: i64,
        _timeout: Duration,
    ) -> Result<(), StreamError> {
        if self.assign_from_timestamp_causes_error {
            Err(StreamError {
                source: None,
                context: "FakeKafkaConsumer caused an error in assign_from_timestamp".to_owned(),
            })
        } else {
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
            Err(StreamError {
                source: None,
                context: "FakeKafkaConsumer caused an error in subscribe".to_owned(),
            })
        } else {
            self.topics
                .replace(topics.iter().map(|&s| s.to_owned()).collect());
            Ok(())
        }
    }

    fn unsubscribe(&self) {
        self.topics.replace(vec![]);
    }

    fn unassign(&self) -> Result<(), StreamError> {
        if self.unassign_causes_error {
            Err(StreamError {
                source: None,
                context: "FakeKafkaConsumer caused an error in unassign".to_owned(),
            })
        } else {
            self.topics.replace(vec![]);
            Ok(())
        }
    }

    fn commit_message(
        &self,
        _message: &Self::Msg<'_>,
        _mode: CommitMode,
    ) -> Result<(), StreamError> {
        if self.commit_causes_error {
            Err(StreamError {
                source: None,
                context: "FakeKafkaConsumer caused an error in commit_message".to_owned(),
            })
        } else {
            Ok(())
        }
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
        consumer.messages.borrow_mut().push_back(Err(StreamError {
            source: None,
            context: "oops".to_owned(),
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
        let mut consumer = FakeStream::default();

        consumer.subscribe_causes_error = true;
        consumer.subscribe(&["foo", "bar", "baz"]).unwrap_err();
        assert_eq!(consumer.topics.borrow().len(), 0);

        consumer.subscribe_causes_error = false;
        consumer.subscribe(&["foo", "bar", "baz"]).unwrap();
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unsubscribe();
        assert_eq!(consumer.topics.borrow().len(), 0);
    }

    #[test]
    fn test_assign_unassign() {
        let mut consumer = FakeStream::default();

        consumer.assign_from_timestamp_causes_error = true;
        consumer
            .assign_from_timestamp(&["foo", "bar", "baz"], 0, Duration::from_millis(1))
            .unwrap_err();
        assert_eq!(consumer.topics.borrow().len(), 0);

        consumer.assign_from_timestamp_causes_error = false;
        consumer
            .assign_from_timestamp(&["foo", "bar", "baz"], 0, Duration::from_millis(1))
            .unwrap();
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unassign_causes_error = true;
        consumer.unassign().unwrap_err();
        assert_eq!(consumer.topics.borrow().len(), 3);

        consumer.unassign_causes_error = false;
        consumer.unassign().unwrap();
        assert_eq!(consumer.topics.borrow().len(), 0);
    }
}
