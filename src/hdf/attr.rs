//! Utilities for interacting with HDF5 attributes.
use crate::hdf::error::Hdf5Error;
use crate::hdf::traits::Hdf5ErrorContext;
use chrono::Utc;
use hdf5::types::VarLenAscii;
use hdf5::{HDF5_VERSION, Location};
use log::trace;

/// Add the default set of attributes present on the root group of a NeXus file.
///
/// Intentional difference from ISISICP: we do not write a NeXus_version attribute, which
/// corresponded to the NAPI version in use, as we are not using NAPI.
pub fn add_root_dataset_attributes(root: &Location) -> Result<(), Hdf5Error> {
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
) -> Result<(), Hdf5Error> {
    trace!(
        "Writing attribute {}={:?} to dataset_location={:?}",
        name, value, location
    );
    let value = VarLenAscii::from_ascii(value).map_err(|e| Hdf5Error::StringEncodingError {
        cause: Box::new(e),
        filename: location.filename(),
        location: format!("{} attribute={}", location.name(), name),
    })?;

    let attr = location
        .new_attr::<VarLenAscii>()
        .create(name)
        .err_attribute(location, name)?;

    attr.write_scalar(&value).err_attribute(location, name)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_factory::FileFactory;
    use crate::hdf::scope::with_nexus_file;

    #[test]
    fn test_add_root_dataset_attributes() {
        with_nexus_file(FileFactory::Memory, "unittest", |f| {
            add_root_dataset_attributes(&f).unwrap();

            assert!(
                f.attr("HDF5_Version")
                    .unwrap()
                    .read_scalar::<hdf5::types::VarLenAscii>()
                    .is_ok()
            );
            assert!(
                f.attr("file_name")
                    .unwrap()
                    .read_scalar::<hdf5::types::VarLenAscii>()
                    .is_ok()
            );
            assert!(
                f.attr("file_time")
                    .unwrap()
                    .read_scalar::<hdf5::types::VarLenAscii>()
                    .is_ok()
            );

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_add_ascii_string_attribute() {
        with_nexus_file(FileFactory::Memory, "unittest", |f| {
            add_ascii_string_attribute(&f, "foo", "bar").unwrap();
            assert_eq!(
                f.attr("foo")
                    .unwrap()
                    .read_scalar::<hdf5::types::VarLenAscii>()
                    .unwrap(),
                "bar"
            );
            Ok(())
        })
        .unwrap();
    }
}
