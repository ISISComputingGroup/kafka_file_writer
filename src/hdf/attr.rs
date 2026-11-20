//! Utilities for interacting with HDF5 attributes.
use crate::error::FileWriterError;
use crate::hdf::error::Hdf5Error;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::nexus_structure::{StaticData, StaticDataType};
use chrono::Utc;
use hdf5::types::VarLenAscii;
use hdf5::{H5Type, HDF5_VERSION, Location};
use log::trace;
use ndarray::ArrayD;
use serde::de::DeserializeOwned;

/// Add the default set of attributes present on the root group of a NeXus file.
///
/// Intentional difference from ISISICP: we do not write a NeXus_version attribute, which
/// corresponded to the NAPI version in use, as we are not using NAPI.
pub fn add_root_dataset_attributes(root: &Location) -> Result<(), Hdf5Error> {
    add_simple_string_attribute(
        root,
        "HDF5_Version",
        &format!(
            "{}.{}.{}",
            HDF5_VERSION.major, HDF5_VERSION.minor, HDF5_VERSION.micro
        ),
    )?;

    add_simple_string_attribute(root, "file_name", &root.filename())?;

    add_simple_string_attribute(
        root,
        "file_time",
        &Utc::now().format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
    )?;

    Ok(())
}

pub fn add_simple_string_attribute(
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

fn add_string_attribute(
    location: &Location,
    name: &str,
    value: StaticData,
) -> Result<(), FileWriterError> {
    let data: Vec<String> = serde_json::from_value(value.data)?;
    let data = data
        .into_iter()
        .map(|s| VarLenAscii::from_ascii(s.as_str()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            FileWriterError::Hdf5Error(Hdf5Error::StringEncodingError {
                cause: Box::new(e),
                location: location.name(),
                filename: location.filename(),
            })
        })?;
    let data = ArrayD::from_shape_vec(value.dims, data)?;

    let attr = location
        .new_attr::<VarLenAscii>()
        .create(name)
        .err_attribute(location, name)?;

    attr.write(&data).err_attribute(location, name)?;
    Ok(())
}

fn add_typed_attribute<T: H5Type + DeserializeOwned>(
    location: &Location,
    name: &str,
    value: StaticData,
) -> Result<(), FileWriterError> {
    let data: Vec<T> = serde_json::from_value(value.data)?;
    let data = ArrayD::from_shape_vec(value.dims, data)?;

    let attr = location
        .new_attr::<T>()
        .create(name)
        .err_attribute(location, name)?;

    attr.write(&data).err_attribute(location, name)?;
    Ok(())
}

pub fn add_attribute(
    location: &Location,
    name: &str,
    value: StaticData,
) -> Result<(), FileWriterError> {
    match value.dtype {
        StaticDataType::U8 => add_typed_attribute::<u8>(location, name, value),
        StaticDataType::U16 => add_typed_attribute::<u16>(location, name, value),
        StaticDataType::U32 => add_typed_attribute::<u32>(location, name, value),
        StaticDataType::U64 => add_typed_attribute::<u64>(location, name, value),
        StaticDataType::I8 => add_typed_attribute::<i8>(location, name, value),
        StaticDataType::I16 => add_typed_attribute::<i16>(location, name, value),
        StaticDataType::I32 => add_typed_attribute::<i32>(location, name, value),
        StaticDataType::I64 => add_typed_attribute::<i64>(location, name, value),
        StaticDataType::F32 => add_typed_attribute::<f32>(location, name, value),
        StaticDataType::F64 => add_typed_attribute::<f64>(location, name, value),
        StaticDataType::Ascii => add_string_attribute(location, name, value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;
    use crate::hdf::scope::with_nexus_file;

    #[test]
    fn test_add_root_dataset_attributes() {
        with_nexus_file(FileCreator::Memory, "unittest", |f| {
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
        with_nexus_file(FileCreator::Memory, "unittest", |f| {
            add_simple_string_attribute(&f, "foo", "bar").unwrap();
            assert_eq!(
                f.attr("foo").unwrap().read_scalar::<VarLenAscii>().unwrap(),
                "bar"
            );
            Ok(())
        })
        .unwrap();
    }
}
