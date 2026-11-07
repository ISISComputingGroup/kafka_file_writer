//! Metrics-related utilities.

pub mod kafka;
use crate::config::GlobalConfig;
use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

pub const INCOMING_DATA_MESSAGES: &str = "kafka_file_writer_incoming_data_messages";
pub const INCOMING_DATA_MESSAGE_SIZE: &str = "kafka_file_writer_incoming_data_message_size";

pub const INCOMING_DATA_MESSAGES_NO_PAYLOAD: &str =
    "kafka_file_writer_incoming_data_messages_no_payload";
pub const INCOMING_DATA_MESSAGES_FAILED_DESERIALIZE: &str =
    "kafka_file_writer_incoming_data_messages_failed_deserialize";

pub const INCOMING_CONTROL_MESSAGES_UNKNOWN_SCHEMA: &str =
    "kafka_file_writer_incoming_control_messages_unknown_schema";
pub const INCOMING_CONTROL_MESSAGES_NO_PAYLOAD: &str =
    "kafka_file_writer_incoming_control_messages_no_payload";
pub const INCOMING_CONTROL_MESSAGES_FAILED_DESERIALIZE: &str =
    "kafka_file_writer_incoming_control_messages_failed_deserialize";

pub const SCHEMA_PROCESSING_TIMES: &str = "kafka_file_writer_schema_processing_times";
pub const MODULE_PROCESSING_TIMES: &str = "kafka_file_writer_module_processing_times";

pub const FILE_WRITE_FAILURES: &str = "kafka_file_writer_write_failures";
pub const FILES_WRITTEN: &str = "kafka_file_writer_files_written";

pub const WRITING_FILE: &str = "kafka_file_writer_writing_file";

pub const KAFKA_REPLYQ: &str = "kafka_file_writer_kafka_replyq";

pub const KAFKA_TX: &str = "kafka_file_writer_kafka_tx";
pub const KAFKA_TX_BYTES: &str = "kafka_file_writer_kafka_tx_bytes";

pub const KAFKA_RX: &str = "kafka_file_writer_kafka_rx";
pub const KAFKA_RX_BYTES: &str = "kafka_file_writer_kafka_rx_bytes";

pub const KAFKA_CONSUMER_LAG: &str = "kafka_file_writer_kafka_consumer_lag";

pub const KAFKA_FETCHQ: &str = "kafka_file_writer_kafka_fetchq";
pub const KAFKA_FETCHQ_SIZE: &str = "kafka_file_writer_kafka_fetchq_size";

pub const KAFKA_QUERY_OFFSET: &str = "kafka_file_writer_kafka_query_offset";
pub const KAFKA_NEXT_OFFSET: &str = "kafka_file_writer_kafka_next_offset";
pub const KAFKA_APP_OFFSET: &str = "kafka_file_writer_kafka_app_offset";
pub const KAFKA_STORED_OFFSET: &str = "kafka_file_writer_kafka_stored_offset";
pub const KAFKA_COMMITTED_OFFSET: &str = "kafka_file_writer_kafka_committed_offset";
pub const KAFKA_LO_OFFSET: &str = "kafka_file_writer_kafka_lo_offset";
pub const KAFKA_HI_OFFSET: &str = "kafka_file_writer_kafka_hi_offset";
pub const KAFKA_MSGS_INFLIGHT: &str = "kafka_file_writer_kafka_msgs_inflight";

pub const KAFKA_POLL_ERRORS: &str = "kafka_file_writer_kafka_poll_errors";

/// Install and initialize all metrics.
pub fn initialize_metrics(config: &GlobalConfig) -> Result<(), BuildError> {
    let builder = PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(config.metrics_bind_addr);

    builder.install()?;

    describe_counter!(
        INCOMING_DATA_MESSAGES,
        Unit::Count,
        "Total number of incoming Kafka messages processed"
    );
    counter!(INCOMING_DATA_MESSAGES).absolute(0);

    describe_counter!(
        INCOMING_DATA_MESSAGE_SIZE,
        Unit::Bytes,
        "Total size of incoming Kafka messages processed"
    );
    counter!(INCOMING_DATA_MESSAGE_SIZE).absolute(0);

    describe_counter!(
        INCOMING_DATA_MESSAGES_NO_PAYLOAD,
        Unit::Count,
        "Number of Kafka messages received without a payload"
    );
    counter!(INCOMING_DATA_MESSAGES_NO_PAYLOAD).absolute(0);

    describe_counter!(
        INCOMING_DATA_MESSAGES_FAILED_DESERIALIZE,
        Unit::Count,
        "Number of Kafka messages received which could not be deserialized"
    );
    counter!(INCOMING_DATA_MESSAGES_FAILED_DESERIALIZE).absolute(0);

    describe_counter!(
        INCOMING_CONTROL_MESSAGES_NO_PAYLOAD,
        Unit::Count,
        "Number of control Kafka messages received without a payload"
    );
    counter!(INCOMING_CONTROL_MESSAGES_NO_PAYLOAD).absolute(0);

    describe_counter!(
        INCOMING_CONTROL_MESSAGES_FAILED_DESERIALIZE,
        Unit::Count,
        "Number of control Kafka messages received which could not be deserialized"
    );
    counter!(INCOMING_CONTROL_MESSAGES_FAILED_DESERIALIZE).absolute(0);

    describe_counter!(
        INCOMING_CONTROL_MESSAGES_UNKNOWN_SCHEMA,
        Unit::Count,
        "Number of control Kafka messages received with unrecognised schema"
    );
    counter!(INCOMING_CONTROL_MESSAGES_UNKNOWN_SCHEMA).absolute(0);

    describe_gauge!(
        KAFKA_REPLYQ,
        Unit::Count,
        "Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()"
    );

    describe_gauge!(
        KAFKA_TX,
        Unit::Count,
        "Total number of requests sent to Kafka brokers"
    );

    describe_gauge!(
        KAFKA_TX_BYTES,
        Unit::Bytes,
        "Total number of bytes transmitted to Kafka brokers"
    );

    describe_gauge!(
        KAFKA_RX,
        Unit::Count,
        "Total number of responses received from Kafka brokers"
    );

    describe_gauge!(
        KAFKA_RX_BYTES,
        Unit::Bytes,
        "Total number of bytes received from Kafka brokers"
    );

    describe_gauge!(
        KAFKA_FETCHQ,
        Unit::Count,
        "Number of pre-fetched messages in fetch queue"
    );

    describe_gauge!(
        KAFKA_FETCHQ_SIZE,
        Unit::Bytes,
        "Bytes of pre-fetched messages in fetch queue"
    );

    describe_gauge!(
        KAFKA_QUERY_OFFSET,
        Unit::Count,
        "Current/Last logical offset query"
    );

    describe_gauge!(KAFKA_NEXT_OFFSET, Unit::Count, "Next offset to fetch");

    describe_gauge!(
        KAFKA_APP_OFFSET,
        Unit::Count,
        "Offset of last message passed to application + 1"
    );

    describe_gauge!(KAFKA_STORED_OFFSET, Unit::Count, "Offset to be committed");

    describe_gauge!(KAFKA_COMMITTED_OFFSET, Unit::Count, "Last committed offset");

    describe_gauge!(
        KAFKA_LO_OFFSET,
        Unit::Count,
        "Partition's low watermark offset on broker"
    );

    describe_gauge!(
        KAFKA_HI_OFFSET,
        Unit::Count,
        "Partition's high watermark offset on broker"
    );

    describe_gauge!(
        KAFKA_CONSUMER_LAG,
        Unit::Count,
        "Difference between (hi_offset or ls_offset) and committed_offset. hi_offset is used when isolation.level=read_uncommitted, otherwise ls_offset"
    );

    describe_gauge!(
        KAFKA_MSGS_INFLIGHT,
        Unit::Count,
        "Current number of messages in-flight to/from broker"
    );

    describe_histogram!(
        SCHEMA_PROCESSING_TIMES,
        Unit::Seconds,
        "Time spent processing a message with a specific schema"
    );

    describe_histogram!(
        MODULE_PROCESSING_TIMES,
        Unit::Seconds,
        "Time spent processing messages by a specific writer module"
    );

    describe_counter!(
        FILE_WRITE_FAILURES,
        Unit::Count,
        "Number of files which failed to write"
    );
    counter!(FILE_WRITE_FAILURES).absolute(0);

    describe_counter!(
        FILES_WRITTEN,
        Unit::Count,
        "Number of files successfully written"
    );
    counter!(FILES_WRITTEN).absolute(0);

    describe_gauge!(
        WRITING_FILE,
        Unit::Count,
        "Whether a file-write is currently in-progress (1) or not (0)"
    );
    gauge!(WRITING_FILE).set(0);

    describe_counter!(
        KAFKA_POLL_ERRORS,
        Unit::Count,
        "Number of errors from calling Kafka poll()"
    );
    counter!(KAFKA_POLL_ERRORS).absolute(0);

    Ok(())
}
