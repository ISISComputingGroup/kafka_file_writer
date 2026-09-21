//! Global file-writer configuration file support.
use ahash::HashMap;
use miette::Diagnostic;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    pub back_in_time_ms: u64,
    pub job_pool_topic: String,
    pub error_backoff_s: u64,
    pub job_pool_kafka_consumer_settings: HashMap<String, String>,
    pub data_kafka_consumer_settings: HashMap<String, String>,
    pub file_output_directory: PathBuf,
    pub forced_nexus_structure_filepath: Option<String>,
    pub one_file_only: bool,
}

/// Errors thrown by `get_config` methods at run time.
#[derive(Debug, Error, Diagnostic)]
pub enum ConfigError {
    #[error("Error reading from config file '{}'", .path)]
    IOError {
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

pub fn get_config_from_path(path: impl AsRef<Path>) -> Result<GlobalConfig, ConfigError> {
    let config_str = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::IOError {
        cause: e,
        path: path.as_ref().display().to_string(),
    })?;

    toml::from_str(&config_str).map_err(|e| ConfigError::ParseError {
        path: path.as_ref().display().to_string(),
        cause: e,
    })
}
