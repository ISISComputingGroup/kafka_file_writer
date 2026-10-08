//! Top-level file writer.

use crate::config::GlobalConfig;
use crate::error::FileWriterError;
use crate::hdf::file_creator::FileCreator;
use crate::run_writer::job_pool::wait_for_run_start;
use crate::run_writer::single_file_write_task::SingleFileWriteTask;
use crate::stream::error::StreamError;
use crate::stream::traits::Stream;
use crate::writer_module_factories::WriterModuleFactories;
use log::{error, info};
use miette::Report;
use std::time::Duration;

/// Top-level file writer object.
///
/// Type parameters:
/// - `S`: `Stream` type
/// - `JC`: job-pool consumer factory type
/// - `DC`: data consumer factory type
pub struct FileWriter<'a, S, JC, DC>
where
    S: Stream,
    JC: Fn() -> Result<S, StreamError>,
    DC: Fn() -> Result<S, StreamError>,
{
    pub config: &'a GlobalConfig,
    pub registry: WriterModuleFactories,
    pub job_pool_consumer_factory: JC,
    pub data_consumer_factory: DC,
    pub file_factory: FileCreator,
}

impl<S, JC, DC> FileWriter<'_, S, JC, DC>
where
    S: Stream,
    JC: Fn() -> Result<S, StreamError>,
    DC: Fn() -> Result<S, StreamError>,
{
    /// Write multiple files.
    ///
    /// This function will only return if `config.exit_after_writing_one_file` is true,
    /// in which case the returned result will be the result for that one file.
    /// If `config.exit_after_writing_one_file` is false, this function will never return.
    pub fn write_files(&self) -> Result<(), FileWriterError> {
        loop {
            let result = self.write_one_file();

            if self.config.exit_after_writing_one_file {
                info!("Filewriter operating in single-file mode; exiting main loop");
                return result;
            }

            if let Err(err) = result {
                let report: Report = Report::from(err);
                error!("Failed to write file: {:?}", report);

                std::thread::sleep(Duration::from_millis(self.config.stream_error_backoff_ms));
            }
        }
    }

    /// Write a single file.
    ///
    /// This function will wait for a run start on the job-pool topic, wait for that file
    /// to complete writing, and then return.
    fn write_one_file(&self) -> Result<(), FileWriterError> {
        info!(
            "Waiting for run start on topic '{}'",
            self.config.job_pool_topic
        );
        let job_pool_consumer = (self.job_pool_consumer_factory)()?;
        let run_start_parameters = wait_for_run_start(self.config, &job_pool_consumer)?;
        info!(
            "Run start job_id='{}' filename='{}' start_time='{}'",
            run_start_parameters.job_id,
            run_start_parameters.filename.display(),
            run_start_parameters.start_time_ms
        );

        let data_consumer = (self.data_consumer_factory)()?;

        let task = SingleFileWriteTask {
            config: self.config,
            writer_module_factories: &self.registry,
            run_start_parameters: &run_start_parameters,
        };

        task.write_data_for_run(&data_consumer, self.file_factory)?;

        info!(
            "Successfully wrote job_id='{}'",
            run_start_parameters.job_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::error::Hdf5Error;
    use crate::stream::fake::FakeStream;
    use crate::writer_modules::default_registry;

    fn fake_job_pool_stream_with_runstart() -> FakeStream {
        let stream = FakeStream::default();
        stream.append_runinfo_run_start_message();
        stream
    }

    fn fake_data_stream_with_runstop() -> FakeStream {
        let stream = FakeStream::default();
        stream.append_runinfo_run_stop_message();
        stream
    }

    #[test]
    fn test_write_one_file() {
        let fw = FileWriter {
            config: &GlobalConfig::test_config(),
            registry: default_registry(),
            data_consumer_factory: || Ok(fake_data_stream_with_runstop()),
            job_pool_consumer_factory: || Ok(fake_job_pool_stream_with_runstart()),
            file_factory: FileCreator::Memory,
        };

        assert!(fw.write_one_file().is_ok());
    }

    #[test]
    fn test_write_files() {
        let mut config = GlobalConfig::test_config();
        config.exit_after_writing_one_file = true;

        let fw = FileWriter {
            config: &config,
            registry: default_registry(),
            data_consumer_factory: || Ok(fake_data_stream_with_runstop()),
            job_pool_consumer_factory: || Ok(fake_job_pool_stream_with_runstart()),
            file_factory: FileCreator::Memory,
        };

        assert!(fw.write_files().is_ok());
    }

    #[test]
    fn test_write_one_file_with_failed_file_creation() {
        let fw = FileWriter {
            config: &GlobalConfig::test_config(),
            registry: default_registry(),
            data_consumer_factory: || Ok(fake_data_stream_with_runstop()),
            job_pool_consumer_factory: || Ok(fake_job_pool_stream_with_runstart()),
            file_factory: FileCreator::AlwaysFails,
        };

        assert!(fw.write_one_file().is_err_and(|err| {
            matches!(
                err,
                FileWriterError::Hdf5Error(Hdf5Error::CreateError { .. })
            )
        }));
    }

    #[test]
    fn test_write_one_file_with_failed_data_consumer_creation() {
        let fw = FileWriter {
            config: &GlobalConfig::test_config(),
            registry: default_registry(),
            data_consumer_factory: || Err(StreamError::UnitTestError),
            job_pool_consumer_factory: || Ok(fake_job_pool_stream_with_runstart()),
            file_factory: FileCreator::Memory,
        };

        assert!(fw.write_one_file().is_err_and(|err| {
            matches!(
                err,
                FileWriterError::StreamError(StreamError::UnitTestError)
            )
        }));
    }

    #[test]
    fn test_write_one_file_with_message_commit_failure() {
        let fw = FileWriter {
            config: &GlobalConfig::test_config(),
            registry: default_registry(),
            data_consumer_factory: || Ok(fake_data_stream_with_runstop()),
            job_pool_consumer_factory: || {
                let mut stream = fake_job_pool_stream_with_runstart();
                stream.commit_causes_error = true;
                Ok(stream)
            },
            file_factory: FileCreator::Memory,
        };

        assert!(fw.write_one_file().is_err_and(|err| {
            matches!(
                err,
                FileWriterError::StreamError(StreamError::CommitError { .. })
            )
        }));
    }
}
