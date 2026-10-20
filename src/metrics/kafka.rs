//! Kafka consumer context which exposes metrics from statistics.

use crate::metrics::{
    KAFKA_APP_OFFSET, KAFKA_COMMITTED_OFFSET, KAFKA_CONSUMER_LAG, KAFKA_FETCHQ, KAFKA_FETCHQ_SIZE,
    KAFKA_HI_OFFSET, KAFKA_LO_OFFSET, KAFKA_MSGS_INFLIGHT, KAFKA_NEXT_OFFSET, KAFKA_QUERY_OFFSET,
    KAFKA_REPLYQ, KAFKA_RX, KAFKA_RX_BYTES, KAFKA_STORED_OFFSET, KAFKA_TX, KAFKA_TX_BYTES,
};
use metrics::gauge;
use rdkafka::consumer::ConsumerContext;
use rdkafka::{ClientContext, Statistics};

/// Kafka context with a named consumer (to disambiguate data
/// and job pool consumers).
pub struct MetricsKafkaContext {
    pub name: &'static str,
}

impl ClientContext for MetricsKafkaContext {
    /// Expose Kafka statistics as prometheus metrics.
    ///
    /// This depends on `statistics.interval.ms` being set in the Kafka
    /// configuration; otherwise, this will never be called.
    fn stats(&self, statistics: Statistics) {
        gauge!(KAFKA_REPLYQ, "consumer" => self.name).set(statistics.replyq as f64);
        gauge!(KAFKA_TX, "consumer" => self.name).set(statistics.tx as f64);
        gauge!(KAFKA_TX_BYTES, "consumer" => self.name).set(statistics.tx_bytes as f64);
        gauge!(KAFKA_RX, "consumer" => self.name).set(statistics.rx as f64);
        gauge!(KAFKA_RX_BYTES, "consumer" => self.name).set(statistics.rx_bytes as f64);

        for (topic_name, topic) in statistics.topics.iter() {
            for (&partition_id, partition) in &topic.partitions {
                if partition_id < 0 {
                    // Kafka summary also gives partition -1 which isn't meaningful
                    continue;
                }
                gauge!(KAFKA_FETCHQ, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.fetchq_cnt as f64);
                gauge!(KAFKA_FETCHQ_SIZE, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.fetchq_size as f64);
                gauge!(KAFKA_QUERY_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.query_offset as f64);
                gauge!(KAFKA_NEXT_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.next_offset as f64);
                gauge!(KAFKA_APP_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.app_offset as f64);
                gauge!(KAFKA_STORED_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.stored_offset as f64);
                gauge!(KAFKA_COMMITTED_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.committed_offset as f64);
                gauge!(KAFKA_CONSUMER_LAG, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.consumer_lag as f64);
                gauge!(KAFKA_MSGS_INFLIGHT, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.msgs_inflight as f64);
                gauge!(KAFKA_LO_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.lo_offset as f64);
                gauge!(KAFKA_HI_OFFSET, "consumer" => self.name, "topic" => topic_name.clone(), "partition" => partition_id.to_string()).set(partition.hi_offset as f64);
            }
        }
    }
}

impl ConsumerContext for MetricsKafkaContext {}
