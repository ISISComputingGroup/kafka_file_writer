//! Rewrite a NeXus file from data in Kafka.
//!
//! This utility writes a single run *as though* a run start with the specified command-line
//! parameters was seen; it does not actually emit a run start to Kafka.
//!
//! Because it does not depend on actually seeing a run start or run-stop in Kafka, this utility
//! is suitable for:
//! - Re-writing data from Kafka when the original run-start or run-stop was missing
//! - Testing writing real data from a beamline without needing that beamline to start and stop data acquisition.
//!
//! A full example command is:
//! ```
//! rewrite_run --config config.toml --start-time-ms 1794307704000 --stop-time-ms 1794307724000 -f hrpd.nxs -i hrpd -s structures\isis.json
//! ```

use clap::Parser;
use kafka_file_writer::config::GlobalConfig;
use kafka_file_writer::hdf::file_creator::FileCreator;
use kafka_file_writer::metrics::initialize_metrics;
use kafka_file_writer::metrics::kafka::MetricsKafkaContext;
use kafka_file_writer::run_writer::run_start_parameters::RunStartParameters;
use kafka_file_writer::run_writer::single_file_write_task::SingleFileWriteTask;
use kafka_file_writer::stream::kafka::{KafkaStream, make_data_kafka_client_config};
use kafka_file_writer::writer_modules::default_registry;
use miette::{IntoDiagnostic, Result};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short = 'c', long)]
    config: PathBuf,

    /// Run start time, as milliseconds since the Unix epoch.
    #[arg(long)]
    start_time_ms: u64,

    /// Run stop time, as milliseconds since the Unix epoch.
    #[arg(long)]
    stop_time_ms: u64,

    /// Path to the NeXus file to be written.
    ///
    /// If relative, the path will be relative to the `file_output_directory` specified in
    /// the config. If absolute, the file will be written in the specified location.
    #[arg(short = 'f', long)]
    filename: PathBuf,

    /// Path to a nexus-structure JSON (for example, one of the structures in the
    /// `structures` directory).
    #[arg(long, short = 's')]
    nexus_structure_file: PathBuf,

    /// Number of periods in this run.
    #[arg(short = 'p', long, default_value = "1")]
    n_periods: u32,

    /// Instrument name for this run. This is written into the resulting NeXus file,
    /// and may be used by analysis programs.
    #[arg(long, short = 'i')]
    instrument_name: String,

    /// Serve metrics for this process if specified
    #[arg(long, action)]
    metrics: bool,

    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

fn main() -> Result<()> {
    let args = Args::try_parse().into_diagnostic()?;

    env_logger::Builder::new()
        .filter_level(args.verbosity.into())
        .format_timestamp_micros()
        .init();

    let config = GlobalConfig::from_path(&args.config)?;

    if args.metrics {
        initialize_metrics(&config).into_diagnostic()?;
    }

    let stream = KafkaStream::from_config(
        &make_data_kafka_client_config(&config),
        MetricsKafkaContext { name: "data" },
    )?;

    let nexus_structure = std::fs::read_to_string(args.nexus_structure_file).into_diagnostic()?;

    let parameters = RunStartParameters {
        filename: args.filename,
        instrument_name: args.instrument_name,
        job_id: Uuid::new_v4().to_string(),
        start_time_ms: args.start_time_ms,
        stop_time_ms: Some(args.stop_time_ms),
        nexus_structure,
        metadata: None,
        n_periods: args.n_periods,
        control_topic: None,
    };

    let writer_module_registry = default_registry();

    let mut task = SingleFileWriteTask::new(&config, &writer_module_registry, &parameters);
    task.write_data_for_run(&stream, FileCreator::Disk)?;

    Ok(())
}
