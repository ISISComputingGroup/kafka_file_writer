//! Verify that a `nexus_structure` can be parsed by the file-writer.
//!
//! The parsed structure will be printed on success.
//!
//! On failure, a non-zero exit code will be returned along with a diagnostic
//! report about which item failed to parse correctly.
use clap::Parser;
use kafka_file_writer::hdf::file_creator::FileCreator;
use kafka_file_writer::hdf::scope::with_nexus_file;
use kafka_file_writer::run_writer::in_progress_file::InProgressFile;
use kafka_file_writer::run_writer::nexus_structure::NexusFileStructure;
use kafka_file_writer::run_writer::run_start_parameters::RunStartParameters;
use kafka_file_writer::writer_modules::default_registry;
use miette::{IntoDiagnostic, Result};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to JSON nexus structure file to verify
    #[arg(short, long)]
    file: String,

    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

fn main() -> Result<()> {
    let args: Args = Args::try_parse().into_diagnostic()?;

    env_logger::Builder::new()
        .filter_level(args.verbosity.into())
        .format_timestamp_micros()
        .init();

    let json = std::fs::read_to_string(&args.file).into_diagnostic()?;
    let structure = json.parse::<NexusFileStructure>()?;

    println!("{:#?}", structure);

    // Create an in-memory file with this structure, to verify it instantiates successfully.
    with_nexus_file(FileCreator::Memory, "structure_verify", |file| {
        InProgressFile::new(
            file,
            &RunStartParameters::default(),
            structure,
            &default_registry(),
        )?;
        Ok(())
    })?;

    Ok(())
}
