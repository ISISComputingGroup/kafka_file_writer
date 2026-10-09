//! Definition of a stream consumer backed by Kafka.
use crate::config::GlobalConfig;
use crate::stream::error::StreamError;
use crate::stream::traits::{Stream, TopicPartitionOffsets};
use ahash::HashMap;
use log::{debug, trace, warn};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;

/// The type of Kafka consumer that we use at runtime.
/// This is a newtype wrapper around an underlying
/// rdkafka type.
pub struct KafkaStream(BaseConsumer<DefaultConsumerContext>);

impl KafkaStream {
    pub fn from_config(config: &ClientConfig) -> Result<KafkaStream, StreamError> {
        config
            .create()
            .map(KafkaStream)
            .map_err(|e| StreamError::ConsumerCreationError { cause: Box::new(e) })
    }
}

/// Make a default Kafka client configuration, from the
/// parameters specified in the `config.toml`.
pub fn make_job_pool_kafka_client_config(config: &GlobalConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    for (k, v) in &config.job_pool_kafka_consumer_settings {
        client_config.set(k, v);
    }
    client_config
}

/// Make a default Kafka client configuration, from the
/// parameters specified in the `config.toml`.
pub fn make_data_kafka_client_config(config: &GlobalConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    for (k, v) in &config.data_kafka_consumer_settings {
        if k == "group.id" {
            warn!(
                "group.id '{}' in config for data consumer will be overridden by a generated UUID",
                v
            );
        }
        client_config.set(k, v);
    }

    let group_id = format!("kafka_file_writer-{}", uuid::Uuid::new_v4());
    client_config.set("group.id", &group_id);
    debug!(
        "Generated Kafka 'group.id' for data consumer is {}",
        group_id
    );

    client_config
}

impl KafkaStream {
    fn partitions_for(&self, topic: &str, timeout: Duration) -> Result<Vec<i32>, StreamError> {
        let metadata = self.0.fetch_metadata(Some(topic), timeout).map_err(|err| {
            StreamError::MissingMetadata {
                cause: Some(Box::new(err)),
                topic: topic.to_owned(),
                context: "fetching metadata",
                err_code: None,
            }
        })?;

        let t = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| StreamError::MissingMetadata {
                cause: None,
                topic: topic.to_owned(),
                context: "finding topic in fetched metadata",
                err_code: None,
            })?;

        if let Some(e) = t.error() {
            return Err(StreamError::MissingMetadata {
                cause: None,
                topic: topic.to_owned(),
                context: "error returned from fetching metadata",
                err_code: Some(e.into()),
            });
        }

        Ok(t.partitions().iter().map(|p| p.id()).collect())
    }
}

impl Stream for KafkaStream {
    type Msg<'a> = BorrowedMessage<'a>;

    fn offsets_for_timestamp(
        &self,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<TopicPartitionOffsets, StreamError> {
        let mut result = HashMap::default();

        let offsets = self
            .0
            .offsets_for_timestamp(timestamp, timeout)
            .map_err(|err| StreamError::OffsetsForTimestampError {
                cause: Box::new(err),
            })?;

        for elem in offsets.elements() {
            let real_offset = match elem.offset() {
                Offset::Offset(o) => o,
                Offset::End => {
                    let (_, high_watermark) = self
                        .0
                        .fetch_watermarks(elem.topic(), elem.partition(), timeout)
                        .map_err(|e| StreamError::OffsetsForTimestampError {
                            cause: Box::new(e),
                        })?;
                    high_watermark
                }
                other => {
                    return Err(StreamError::InvalidKafkaOffsetError {
                        offset: Some(other),
                    });
                }
            };
            result.insert((elem.topic().to_owned(), elem.partition()), real_offset);
        }

        Ok(result)
    }

    fn assign_from_timestamp(
        &self,
        topics: &[&str],
        timestamp: i64,
        timeout: Duration,
    ) -> Result<(), StreamError> {
        if topics.is_empty() {
            self.unassign()?;
            return Ok(());
        }

        let mut timestamps = TopicPartitionList::new();

        for &topic in topics {
            for partition in self.partitions_for(topic, timeout)? {
                timestamps
                    .add_partition_offset(topic, partition, Offset::Offset(timestamp))
                    .map_err(|e| StreamError::AssignmentError {
                        cause: Box::new(e),
                        context: "Unable to assign timestamp",
                        topics: vec![topic.to_owned()],
                    })?;
            }
        }

        let offsets = self
            .0
            .offsets_for_times(timestamps, timeout)
            .map_err(|err| StreamError::AssignmentError {
                cause: Box::new(err),
                context: "Unable to get offsets",
                topics: topics.iter().map(|&t| t.to_owned()).collect(),
            })?;

        trace!("Consumer assignment is to offsets: {:?}", offsets);

        self.0
            .assign(&offsets)
            .map_err(|e| StreamError::AssignmentError {
                cause: Box::new(e),
                context: "Unable to assign",
                topics: topics.iter().map(|&t| t.to_owned()).collect(),
            })?;

        Ok(())
    }

    fn poll(&self, timeout: Duration) -> Option<Result<Self::Msg<'_>, StreamError>> {
        self.0.poll(timeout).map(|res| {
            res.map_err(|err| StreamError::PollError {
                cause: Box::new(err),
            })
        })
    }

    fn subscribe(&self, topics: &[&str]) -> Result<(), StreamError> {
        self.0
            .subscribe(topics)
            .map_err(|err| StreamError::AssignmentError {
                cause: Box::new(err),
                context: "while subscribing",
                topics: topics.iter().map(|&t| t.to_owned()).collect(),
            })
    }

    fn unsubscribe(&self) {
        self.0.unsubscribe()
    }

    fn unassign(&self) -> Result<(), StreamError> {
        self.0
            .unassign()
            .map_err(|err| StreamError::AssignmentError {
                cause: Box::new(err),
                context: "while unassigning",
                topics: vec![],
            })
    }

    fn commit_message(
        &self,
        message: &BorrowedMessage<'_>,
        mode: CommitMode,
    ) -> Result<(), StreamError> {
        self.0
            .commit_message(message, mode)
            .map_err(|err| StreamError::CommitError {
                cause: Box::new(err),
                topic: message.topic().to_owned(),
                partition: message.partition(),
                offset: message.offset(),
            })
    }
}
