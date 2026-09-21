use crate::FileWriterError;
use chrono::Utc;
use hdf5::types::VarLenAscii;
use hdf5::{HDF5_VERSION, Location};
use log::trace;

/// Add the default set of attributes present on the root group of a NeXus file.
///
/// ```{note}
/// Intentional difference from ISISICP: we do not write a NeXus_version attribute, which
/// corresponded to the NAPI version in use, as we are not using NAPI.
/// ```
pub fn add_root_dataset_attributes(root: &Location) -> Result<(), FileWriterError> {
    add_ascii_string_attribute(
        root,
        "HDF5_Version",
        &format!(
            "{}.{}.{}",
            HDF5_VERSION.major, HDF5_VERSION.minor, HDF5_VERSION.micro
        ),
    )?;

    add_ascii_string_attribute(root, "file_name", &root.filename())?;

    add_ascii_string_attribute(
        root,
        "file_time",
        &Utc::now().format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
    )?;

    Ok(())
}

pub fn add_ascii_string_attribute(
    location: &Location,
    name: &str,
    value: &str,
) -> Result<(), FileWriterError> {
    trace!(
        "Writing attribute {}={:?} to dataset_location={:?}",
        name, value, location
    );
    let value = VarLenAscii::from_ascii(value).map_err(|e| FileWriterError::StringError {
        cause: e,
        dataset_location: location.name(),
    })?;

    let attr = location
        .new_attr::<VarLenAscii>()
        .create(name)
        .map_err(|e| FileWriterError::HDF5Error {
            cause: e,
            file_name: location.filename(),
            dataset_location: location.name(),
        })?;

    attr.write_scalar(&value)
        .map_err(|e| FileWriterError::HDF5Error {
            cause: e,
            file_name: location.filename(),
            dataset_location: location.name(),
        })?;

    Ok(())
}
