use clap::Parser;
use kafka_file_writer::config::get_config_from_path;
use kafka_file_writer::write_files_forever;
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

    let config = get_config_from_path(&args.config)?;

    write_files_forever(&config)?;
    Ok(())
}
