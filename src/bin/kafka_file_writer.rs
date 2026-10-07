//! Main file-writer executable.
use clap::Parser;
use kafka_file_writer::config::GlobalConfig;
use kafka_file_writer::file_writer::FileWriter;
use kafka_file_writer::hdf::file_creator::FileCreator;
use kafka_file_writer::stream::kafka::{
    KafkaStream, make_data_kafka_client_config, make_job_pool_kafka_client_config,
};
use kafka_file_writer::writer_modules::default_registry;
use miette::{IntoDiagnostic, Result};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: String,

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
    let registry = default_registry();

    let file_writer = FileWriter {
        config: &config,
        registry,
        job_pool_consumer_factory: || {
            KafkaStream::from_config(&make_job_pool_kafka_client_config(&config))
        },
        data_consumer_factory: || KafkaStream::from_config(&make_data_kafka_client_config(&config)),
        file_factory: FileCreator::Disk,
    };

    file_writer.write_files()?;
    Ok(())
}
