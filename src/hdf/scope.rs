use crate::error::FileWriterError;
use crate::hdf::file_factory::FileFactory;
use log::{info, trace, warn};
use std::path::Path;

/// Create a file on disk using the specified factory and path,
/// and then run a closure receiving the opened file.
///
/// On exit, flush and close the file.
pub fn with_nexus_file<F, T>(
    file_factory: FileFactory,
    path: &impl AsRef<Path>,
    func: F,
) -> Result<T, FileWriterError>
where
    F: FnOnce(hdf5::File) -> Result<T, FileWriterError>,
{
    let file = file_factory
        .create_file(&path)
        .map_err(|e| FileWriterError::HDF5Error {
            cause: e,
            file_name: path.as_ref().display().to_string(),
            dataset_location: "/".to_owned(),
        })?;

    info!(
        "Successfully opened NeXus file at '{}' for writing",
        path.as_ref().display()
    );
    let result = func(file.clone());

    if result.is_ok() {
        info!(
            "Finished writing to NeXus file at '{}'",
            path.as_ref().display()
        );
    } else {
        warn!(
            "Finished writing to NeXus file at '{}' with error, cleaning up.",
            path.as_ref().display()
        )
    }

    // Whether writing succeeded or failed, we need to try to clean up anything we *did*
    // manage to write by flushing and closing the file.
    if let Err(e) = file.flush() {
        warn!("Error flushing file: {:?}", e);
    } else {
        trace!("Successfully flushed file at '{}'", path.as_ref().display());
    }
    if let Err(e) = file.close() {
        warn!("Error closing file: {:?}", e);
    } else {
        trace!("Successfully closed file at '{}'", path.as_ref().display());
    }

    result
}
