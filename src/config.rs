//! Global file-writer configuration file support.
use ahash::HashMap;
use miette::Diagnostic;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use thiserror::Error;

fn default_back_in_time_ms() -> u64 {
    60_000
}

fn default_poll_time_ms() -> u64 {
    1000
}

fn default_stream_error_backoff_ms() -> u64 {
    3000
}
fn default_kafka_assignment_timeout_ms() -> u64 {
    10000
}

fn default_exit_after_writing_one_file() -> bool {
    false
}

/// Global configuration parameters which control file-writer behavior for all runs.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    /// How far to seek-back and replay all streams before run start. This is applied
    /// globally to all topics.
    #[serde(default = "default_back_in_time_ms")]
    pub back_in_time_ms: u64,

    /// Kafka topic to listen for jobs on
    pub job_pool_topic: String,

    /// Kafka `poll()` timeout for job pool.
    #[serde(default = "default_poll_time_ms")]
    pub job_pool_consumer_poll_time_ms: u64,

    /// Kafka `poll()` timeout for data consumer.
    #[serde(default = "default_poll_time_ms")]
    pub data_consumer_poll_time_ms: u64,

    /// How long to back off for if we got a stream error from Kafka
    #[serde(default = "default_stream_error_backoff_ms")]
    pub stream_error_backoff_ms: u64,

    #[serde(default = "default_kafka_assignment_timeout_ms")]
    pub kafka_assignment_timeout_ms: u64,

    /// Kafka consumer settings for job pool
    pub job_pool_kafka_consumer_settings: HashMap<String, String>,

    /// Kafka consumer settings for data consumer
    pub data_kafka_consumer_settings: HashMap<String, String>,

    /// Directory into which to write NeXus files
    pub file_output_directory: PathBuf,

    /// Override the `nexus_structure` in the run start message with
    /// the structure specified in a local file
    pub forced_nexus_structure_filepath: Option<PathBuf>,

    #[serde(default = "default_exit_after_writing_one_file")]
    pub exit_after_writing_one_file: bool,
}

/// Errors thrown by `get_config` methods at run time.
#[derive(Debug, Error, Diagnostic)]
pub enum ConfigError {
    #[error("Error reading from config file '{}'", .path)]
    IoError {
        path: String,
        #[source]
        cause: std::io::Error,
    },

    #[error("Error parsing contents of config file '{}'", .path)]
    ParseError {
        path: String,
        #[source]
        cause: toml::de::Error,
        // Don't need source/sourcespan here, `toml::de::Error` already includes this
        // information by default - including it here causes duplicates in error message.
    },
}

impl GlobalConfig {
    /// Load a configuration from TOML at the specified filepath.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let config_str =
            std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::IoError {
                cause: e,
                path: path.as_ref().display().to_string(),
            })?;

        toml::from_str(&config_str).map_err(|e| ConfigError::ParseError {
            path: path.as_ref().display().to_string(),
            cause: e,
        })
    }

    /// Generate a basic valid configuration for unit tests
    #[cfg(test)]
    pub fn test_config() -> Self {
        use ahash::HashMapExt;
        GlobalConfig {
            stream_error_backoff_ms: 0,
            exit_after_writing_one_file: false,
            data_kafka_consumer_settings: HashMap::new(),
            job_pool_kafka_consumer_settings: HashMap::new(),
            file_output_directory: PathBuf::new(),
            job_pool_topic: "unittest_jobPool".to_owned(),
            job_pool_consumer_poll_time_ms: 1,
            data_consumer_poll_time_ms: 1,
            kafka_assignment_timeout_ms: 1,
            back_in_time_ms: 0,
            forced_nexus_structure_filepath: None,
        }
    }
}
