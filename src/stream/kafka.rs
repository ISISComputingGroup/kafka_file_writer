//! Definition of a stream consumer backed by Kafka.
use crate::config::GlobalConfig;
use crate::stream::traits::{Stream, StreamError};
use log::{debug, trace, warn};
use miette::Result;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::time::Duration;

/// The type of Kafka consumer that we use at runtime.
/// This is a newtype wrapper around an underlying
/// rdkafka type.
pub struct KafkaStream(BaseConsumer<DefaultConsumerContext>);

impl KafkaStream {
    pub fn from_config(config: &ClientConfig) -> Result<KafkaStream, StreamError> {
        config.create().map(KafkaStream).map_err(|e| StreamError {
            source: Some(Box::new(e)),
            context: "Failed to create Kafka consumer from config".to_string(),
        })
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
                k
            );
        }
        client_config.set(k, v);
    }

    let group_id = format!("kafka_file_writer-{}", uuid::Uuid::new_v4());
    client_config.set("group.id", &group_id);
    debug!(
        "Generated Kafka 'group.id' for data consumer is {}",
        &group_id
    );

    client_config
}

fn partitions_for(
    consumer: &KafkaStream,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, StreamError> {
    let metadata = consumer
        .0
        .fetch_metadata(Some(topic), timeout)
        .map_err(|err| StreamError {
            source: Some(Box::new(err)),
            context: format!("Unable to fetch metadata for '{topic}'"),
        })?;

    let t = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| StreamError {
            source: None,
            context: format!("Topic '{topic}' was not found"),
        })?;

    if let Some(e) = t.error() {
        return Err(StreamError {
            source: None,
            context: format!("Unable to fetch topics (searching for '{topic}'): {e:?}"),
        });
    }

    Ok(t.partitions().iter().map(|p| p.id()).collect())
}

impl Stream for KafkaStream {
    type Msg<'a> = BorrowedMessage<'a>;

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

        for topic in topics {
            for partition in partitions_for(self, topic, timeout)? {
                timestamps
                    .add_partition_offset(topic, partition, Offset::Offset(timestamp))
                    .map_err(|e| StreamError {
                        source: Some(Box::new(e)),
                        context: format!("Unable to assign timestamp for '{topic}'"),
                    })?;
            }
        }

        let offsets = self
            .0
            .offsets_for_times(timestamps, timeout)
            .map_err(|err| StreamError {
                source: Some(Box::new(err)),
                context: format!("Unable to get offsets for '{topics:?}'"),
            })?;

        trace!("Consumer assignment is to offsets: {:?}", offsets);

        self.0.assign(&offsets).map_err(|e| StreamError {
            source: Some(Box::new(e)),
            context: format!("Unable to assign on '{topics:?}'"),
        })?;

        Ok(())
    }

    fn poll(&self, timeout: Duration) -> Option<Result<Self::Msg<'_>, StreamError>> {
        self.0.poll(timeout).map(|res| {
            res.map_err(|err| StreamError {
                source: Some(Box::new(err)),
                context: "during poll".to_string(),
            })
        })
    }

    fn subscribe(&self, topics: &[&str]) -> Result<(), StreamError> {
        self.0.subscribe(topics).map_err(|err| StreamError {
            source: Some(Box::new(err)),
            context: format!("while subscribing to topics {topics:?}"),
        })
    }

    fn unsubscribe(&self) {
        self.0.unsubscribe()
    }

    fn unassign(&self) -> Result<(), StreamError> {
        self.0.unassign().map_err(|err| StreamError {
            source: Some(Box::new(err)),
            context: "while unassigning".to_string(),
        })
    }

    fn commit_message(
        &self,
        message: &BorrowedMessage<'_>,
        mode: CommitMode,
    ) -> Result<(), StreamError> {
        self.0
            .commit_message(message, mode)
            .map_err(|err| StreamError {
                source: Some(Box::new(err)),
                context: "while committing message".to_owned(),
            })
    }
}
