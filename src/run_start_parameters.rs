use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::nexus_structure::NexusFileStructure;
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::RunStart;
use log::{debug, info, trace};
use rdkafka::Message;

/// An owned version of the parts of a pl72 runStart that the filewriter needs to know about.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct RunStartParameters {
    // ms since epoch
    pub start_time: u64,
    // ms since epoch
    pub stop_time: u64,
    pub nexus_structure: String,
    pub job_id: String,
    pub filename: String,
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
        Ok(RunStartParameters {
            start_time: rs.start_time(),
            stop_time: rs.stop_time(),
            filename: rs
                .filename()
                .ok_or_else(|| FileWriterError::from_missing_runstart_data("filename", msg))?
                .to_owned(),
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

    pub fn structure(&self, config: &GlobalConfig) -> Result<NexusFileStructure, FileWriterError> {
        if let Some(ref file_path) = config.forced_nexus_structure_filepath {
            info!(
                "Using forced nexus_structure from local file at '{}'",
                file_path
            );
            NexusFileStructure::from_local_file(file_path)
        } else {
            info!("Using NeXus structure from pl72 message");
            trace!("NeXus structure: {:#?}", self.nexus_structure);
            NexusFileStructure::from_run_start(self)
        }
    }

    pub fn control_topic<'a>(&'a self, config: &'a GlobalConfig) -> &'a str {
        if let Some(control_topic) = &self.control_topic {
            debug!("Using control topic from run start '{}'", control_topic);
            control_topic
        } else {
            debug!(
                "No control topic in run start; using job pool topic for control '{}'",
                config.job_pool_topic
            );
            &config.job_pool_topic
        }
    }
}
