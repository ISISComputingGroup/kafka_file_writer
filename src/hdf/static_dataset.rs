use crate::error::FileWriterError;
use crate::hdf::attr::add_attribute;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::nexus_structure::{StaticDataType, StaticDataset};
use hdf5::types::VarLenAscii;
use hdf5::{Group, H5Type};
use ndarray::ArrayD;
use serde::de::DeserializeOwned;

fn create_and_write_string_dataset(
    group: &Group,
    static_dataset: StaticDataset,
) -> Result<(), FileWriterError> {
    let data: Vec<String> = serde_json::from_value(static_dataset.data.data)?;
    let data = data
        .into_iter()
        .map(|s| VarLenAscii::from_ascii(&s))
        .collect::<Result<Vec<_>, _>>()
        .err_parent_dataset(&group, &static_dataset.name)?;

    let data = ArrayD::from_shape_vec(static_dataset.data.dims, data)?;

    let dataset = group
        .new_dataset::<VarLenAscii>()
        .shape(data.shape())
        .create(static_dataset.name.as_str())
        .err_parent_dataset(&group, &static_dataset.name)?;

    dataset.write(&data).err_dataset(&dataset)?;

    for (key, value) in static_dataset.attributes {
        add_attribute(&dataset, &key, value)?;
    }

    Ok(())
}

fn create_and_write_typed_static_dataset<T: H5Type + DeserializeOwned>(
    group: &Group,
    static_dataset: StaticDataset,
) -> Result<(), FileWriterError> {
    let data: Vec<T> = serde_json::from_value(static_dataset.data.data)?;
    let data = ArrayD::from_shape_vec(static_dataset.data.dims, data)?;

    let dataset = group
        .new_dataset::<T>()
        .shape(data.shape())
        .create(static_dataset.name.as_str())
        .err_parent_dataset(&group, static_dataset.name.as_str())?;

    dataset.write(&data).err_dataset(&dataset)?;

    for (key, value) in static_dataset.attributes {
        add_attribute(&dataset, &key, value)?;
    }

    Ok(())
}

pub fn create_and_write_static_dataset(
    group: &Group,
    static_dataset: StaticDataset,
) -> Result<(), FileWriterError> {
    match static_dataset.data.dtype {
        StaticDataType::U8 => create_and_write_typed_static_dataset::<u8>(group, static_dataset),
        StaticDataType::U16 => create_and_write_typed_static_dataset::<u16>(group, static_dataset),
        StaticDataType::U32 => create_and_write_typed_static_dataset::<u32>(group, static_dataset),
        StaticDataType::U64 => create_and_write_typed_static_dataset::<u64>(group, static_dataset),
        StaticDataType::I8 => create_and_write_typed_static_dataset::<i8>(group, static_dataset),
        StaticDataType::I16 => create_and_write_typed_static_dataset::<i16>(group, static_dataset),
        StaticDataType::I32 => create_and_write_typed_static_dataset::<i32>(group, static_dataset),
        StaticDataType::I64 => create_and_write_typed_static_dataset::<i64>(group, static_dataset),
        StaticDataType::F32 => create_and_write_typed_static_dataset::<f32>(group, static_dataset),
        StaticDataType::F64 => create_and_write_typed_static_dataset::<f64>(group, static_dataset),
        StaticDataType::Ascii => create_and_write_string_dataset(group, static_dataset),
    }
}
