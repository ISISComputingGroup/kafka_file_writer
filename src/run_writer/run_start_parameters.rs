//! Parameters from the start of the run currently being written.

use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::run_writer::nexus_structure::NexusFileStructure;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::RunStart;
use log::{debug, info, trace, warn};
use rdkafka::Message;
use std::path::PathBuf;

/// An owned version of the parts of a pl72 runStart that the filewriter needs to know about.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct RunStartParameters {
    pub start_time_ms: u64,
    pub stop_time_ms: u64,
    pub nexus_structure: String,
    pub job_id: String,
    pub filename: PathBuf,
    pub n_periods: u32,
    pub metadata: Option<String>,
    pub control_topic: Option<String>,
}

impl RunStartParameters {
    /// Convert a flatbuffers RunStart message to an owned `RunStartParameters`
    /// representation.
    pub fn from_pl72(
        rs: &RunStart,
        msg: &impl Message,
    ) -> Result<RunStartParameters, FileWriterError> {
        let stop_time = match rs.stop_time() {
            // If a stop time is not provided, write "forever" until we get a run-stop message.
            0 => u64::MAX,
            t => t,
        };

        Ok(RunStartParameters {
            start_time_ms: rs.start_time(),
            stop_time_ms: stop_time,
            filename: PathBuf::from(
                rs.filename()
                    .ok_or_else(|| FileWriterError::from_missing_runstart_data("filename", msg))?,
            ),
            nexus_structure: rs
                .nexus_structure()
                .ok_or_else(|| FileWriterError::from_missing_runstart_data("nexus_structure", msg))?
                .to_owned(),
            control_topic: rs.control_topic().map(|s| s.to_owned()),
            metadata: rs.metadata().map(|s| s.to_owned()),
            job_id: rs
                .job_id()
                .ok_or_else(|| FileWriterError::from_missing_runstart_data("job_id", msg))?
                .to_owned(),
            n_periods: rs.n_periods(),
        })
    }

    /// Get the NeXus file structure from the run start message, or use
    /// the structure overridden in `GlobalConfig` if present.
    pub fn structure(&self, config: &GlobalConfig) -> Result<NexusFileStructure, FileWriterError> {
        match config.forced_nexus_structure_filepath {
            Some(ref file_path) => {
                warn!(
                    "Using forced nexus_structure from local file at '{}'",
                    file_path.display()
                );
                NexusFileStructure::from_local_file(file_path)
            }
            None => {
                info!("Using NeXus structure from pl72 message");
                trace!("NeXus structure: {:#?}", self.nexus_structure);
                NexusFileStructure::from_run_start(self)
            }
        }
    }

    /// Get the control topic for this message; uses the job-pool topic
    /// if not provided in the runstart message.
    pub fn control_topic<'a>(&'a self, config: &'a GlobalConfig) -> &'a str {
        match self.control_topic {
            Some(ref topic) => {
                debug!("Using control topic from run start '{}'", topic);
                topic
            }
            None => {
                debug!(
                    "No control topic in run start; using job pool topic for control '{}'",
                    config.job_pool_topic
                );
                &config.job_pool_topic
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_topic_from_run_start() {
        let rsp = RunStartParameters {
            control_topic: Some("foo".to_owned()),
            ..Default::default()
        };
        let mut config = GlobalConfig::test_config();
        config.job_pool_topic = "job_pool_topic".to_owned();

        assert_eq!(rsp.control_topic(&config), "foo");
    }

    #[test]
    fn test_control_topic_from_config() {
        let rsp = RunStartParameters {
            control_topic: None,
            ..Default::default()
        };
        let mut config = GlobalConfig::test_config();
        config.job_pool_topic = "job_pool_topic".to_owned();

        assert_eq!(rsp.control_topic(&config), "job_pool_topic");
    }

    #[test]
    fn test_structure_from_run_start() {
        let rsp = RunStartParameters {
            nexus_structure: "{}".to_owned(),
            ..Default::default()
        };
        let mut config = GlobalConfig::test_config();
        config.forced_nexus_structure_filepath = None;

        assert_eq!(rsp.structure(&config).unwrap(), "{}".parse().unwrap());
    }
}
