use crate::stream::error::StreamError;
use crate::stream::traits::{KafkaMessageMeta, Stream, TopicPartitionOffsets};
use ahash::{HashMap, HashMapExt};
use log::debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The current state of this task.
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// The file is open and no stop time is known yet.
    Running,
    /// The file is open, and a stop time is known. The file-writer is draining
    /// messages from Kafka until this stop time is reached.
    Ending {
        /// The system timestamp at which to stop listening for new data.
        /// This may be later than the run-end time, to account for latency
        /// in data reaching Kafka. This is a computer/NTP timestamp, not
        /// a GPS timestamp.
        stop_listening_at: SystemTime,
        /// The Kafka offsets that must have been read up to before a run
        /// can be considered complete.
        end_offsets: TopicPartitionOffsets,
    },
    /// All writing to the file is complete (terminal state).
    Finished,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RunEndTracker {
    state: State,
    consumed: TopicPartitionOffsets,
    kafka_query_timeout: Duration,
    post_run_settle_time: Duration,
}

impl RunEndTracker {
    pub fn new(kafka_query_timeout: Duration, post_run_settle_time: Duration) -> Self {
        RunEndTracker {
            state: State::Running,
            consumed: HashMap::new(),
            kafka_query_timeout,
            post_run_settle_time,
        }
    }

    pub fn set_initial_offsets(&mut self, next_offsets_to_read: TopicPartitionOffsets) {
        for (elem, offset) in next_offsets_to_read {
            self.consumed
                .entry(elem)
                // The -1 here is because we're being passed the *next* offsets that we
                // must read, so the offset that we claim we've read "up to an including"
                // is `offset-1`
                .and_modify(|v| *v = (*v).max(offset - 1))
                .or_insert(offset - 1);
        }
    }

    pub fn message_consumed(&mut self, meta: &KafkaMessageMeta) {
        self.consumed
            .entry((meta.topic.to_owned(), meta.partition))
            .and_modify(|e| *e = (*e).max(meta.offset))
            .or_insert(meta.offset);
    }

    pub fn on_run_stop(
        &mut self,
        run_stop_timestamp_ms: u64,
        consumer: &impl Stream,
    ) -> Result<(), StreamError> {
        let stop_listening_at =
            UNIX_EPOCH + Duration::from_millis(run_stop_timestamp_ms) + self.post_run_settle_time;

        let end_offsets = self.get_end_offsets(stop_listening_at, consumer)?;
        self.state = State::Ending {
            stop_listening_at,
            end_offsets,
        };
        Ok(())
    }

    fn get_end_offsets(
        &self,
        stop_listening_at: SystemTime,
        consumer: &impl Stream,
    ) -> Result<TopicPartitionOffsets, StreamError> {
        let kafka_stop_timestamp = stop_listening_at
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StreamError::OffsetsForTimestampError { cause: Box::new(e) })?
            .as_millis()
            .try_into()
            .map_err(|e| StreamError::OffsetsForTimestampError { cause: Box::new(e) })?;

        let end_offsets =
            consumer.offsets_for_timestamp(kafka_stop_timestamp, self.kafka_query_timeout)?;

        debug!("New run-end target offsets: {:?}", end_offsets);

        Ok(end_offsets)
    }

    fn has_consumed_up_to_offsets(&self, target_offsets: &TopicPartitionOffsets) -> bool {
        target_offsets
            .iter()
            // RHS: Target offsets are the offsets of the message *after* the one we need to
            // read up to, so check against `v-1`.
            //
            // LHS: If we've never consumed anything, only claim we've read up to offset -1.
            // If the *next* offset is zero (i.e. we didn't need to read anything anyway),
            // the condition will evaluate to true.
            .all(|(k, v)| *self.consumed.get(k).unwrap_or(&-1) >= v - 1)
    }

    pub fn poll_complete(&mut self, consumer: &impl Stream) -> Result<bool, StreamError> {
        match &self.state {
            State::Running => {
                // No run-stop has been seen yet. Keep writing.
                Ok(false)
            }
            State::Ending {
                stop_listening_at,
                end_offsets,
            } => {
                // If the "stop listening" time is in the future, we're not "done" yet.
                if *stop_listening_at > SystemTime::now() {
                    return Ok(false);
                }

                if self.has_consumed_up_to_offsets(end_offsets) {
                    // If we've consumed up to the offsets required, update the required
                    // offsets, as new messages may have come in which also need to be read
                    let new_end_offsets = self.get_end_offsets(*stop_listening_at, consumer)?;

                    if self.has_consumed_up_to_offsets(&new_end_offsets) {
                        self.state = State::Finished;
                        Ok(true)
                    } else {
                        self.state = State::Ending {
                            end_offsets: new_end_offsets,
                            stop_listening_at: *stop_listening_at,
                        };
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            }
            State::Finished => {
                // File was previously marked as finished; no need to re-check.
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::fake::FakeStream;

    #[test]
    fn test_when_run_stop_received_then_runendtracker_is_not_complete_until_appropriate_messages_consumed()
     {
        let consumer = FakeStream::default();
        let mut ret = RunEndTracker::new(Duration::from_millis(100), Duration::from_secs(5));

        let stop_timestamp = 123456789;

        let mut end_offsets = HashMap::new();
        end_offsets.insert(("someTopic".to_owned(), 0), 1234);

        *consumer.offsets_for_timestamp.borrow_mut() = end_offsets;

        ret.on_run_stop(stop_timestamp, &consumer).unwrap();

        // Consume a message which doesn't correspond to the end of what we need to consume
        // Not done yet
        ret.message_consumed(&KafkaMessageMeta {
            topic: "someTopic",
            timestamp: Some(0),
            offset: 1232,
            partition: 0,
            key: None,
        });
        assert!(!ret.poll_complete(&consumer).unwrap());

        // Consume the last message required. After consuming this message, we're finished.
        ret.message_consumed(&KafkaMessageMeta {
            topic: "someTopic",
            timestamp: Some(0),
            offset: 1233,
            partition: 0,
            key: None,
        });
        assert!(ret.poll_complete(&consumer).unwrap());
    }

    #[test]
    fn test_stop_time_in_future_is_not_complete() {
        // note: no end offsets, so completion time is the only condition
        let mut ret = RunEndTracker::new(Duration::from_millis(100), Duration::from_secs(5));
        let consumer = FakeStream::default();

        ret.on_run_stop(
            (SystemTime::now() + Duration::from_hours(1))
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
            &consumer,
        )
        .unwrap();

        assert!(!ret.poll_complete(&consumer).unwrap());
    }

    #[test]
    fn test_stop_time_in_past_is_complete() {
        // note: no end offsets, so completion time is the only condition
        let mut ret = RunEndTracker::new(Duration::from_millis(100), Duration::from_secs(5));
        let consumer = FakeStream::default();

        ret.on_run_stop(
            (SystemTime::now() - Duration::from_hours(1))
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
            &consumer,
        )
        .unwrap();

        assert!(ret.poll_complete(&consumer).unwrap());
    }

    #[test]
    fn test_stop_time_in_past_is_not_complete_if_settle_time_has_not_elapsed() {
        // note: no end offsets, so completion time is the only condition
        // 5 minute post-run settle time
        let mut ret = RunEndTracker::new(Duration::from_millis(100), Duration::from_mins(5));
        let consumer = FakeStream::default();

        // Run stop was "2 minutes ago"
        ret.on_run_stop(
            (SystemTime::now() - Duration::from_mins(2))
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
            &consumer,
        )
        .unwrap();

        assert!(!ret.poll_complete(&consumer).unwrap());
    }
}
