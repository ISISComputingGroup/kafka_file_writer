//! Rewrite a NeXus file from data in Kafka.

use std::path::PathBuf;
use clap::Parser;
use kafka_file_writer::config::GlobalConfig;
use kafka_file_writer::hdf::file_creator::FileCreator;
use kafka_file_writer::metrics::initialize_metrics;
use kafka_file_writer::metrics::kafka::MetricsKafkaContext;
use kafka_file_writer::stream::kafka::{
    KafkaStream, make_data_kafka_client_config,
};
use kafka_file_writer::writer_modules::default_registry;
use miette::{IntoDiagnostic, Result};
use kafka_file_writer::run_writer::run_start_parameters::RunStartParameters;
use kafka_file_writer::run_writer::single_file_write_task::SingleFileWriteTask;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: String,

    #[arg(long)]
    start_time_ms: u64,

    #[arg(long)]
    stop_time_ms: u64,

    #[arg(long)]
    filename: PathBuf,

    #[arg(long)]
    nexus_structure: PathBuf,

    #[arg(long)]
    n_periods: u32,

    #[arg(long)]
    instrument_name: String,

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

    initialize_metrics(&config).into_diagnostic()?;

    let stream = KafkaStream::from_config(
        &make_data_kafka_client_config(&config),
        MetricsKafkaContext { name: "data" }
    )?;

    let nexus_structure = std::fs::read_to_string(args.nexus_structure).into_diagnostic()?;

    let parameters = RunStartParameters {
        filename: args.filename,
        instrument_name: args.instrument_name,
        job_id: uuid::Uuid::new_v4().to_string(),
        start_time_ms: args.start_time_ms,
        stop_time_ms: args.stop_time_ms,
        nexus_structure,
        metadata: None,
        n_periods: args.n_periods,
        control_topic: None,
    };

    let registry = default_registry();

    let mut task = SingleFileWriteTask::new(&config, &registry, &parameters);
    task.write_data_for_run(&stream, FileCreator::Disk)?;

    Ok(())
}
