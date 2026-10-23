use std::marker::PhantomData;
use hdf5::{Dataset, Group, H5Type, SimpleExtents};
use crate::hdf::error::Hdf5Error;
use crate::hdf::traits::Hdf5ErrorContext;

#[derive(Debug)]
pub struct AppendableDataset1D<T: H5Type> {
    dataset: Dataset,
    datatype: PhantomData<T>,
}

impl<T: H5Type> AppendableDataset1D<T> {
    pub fn create_dataset(parent: &Group, name: impl AsRef<str>, chunk_size: usize) -> Result<Self, Hdf5Error> {
        let dataset = parent
            .new_dataset::<T>()
            .shape(SimpleExtents::resizable([0]))
            .chunk([chunk_size])
            .deflate(1)
            .create(name.as_ref())
            .err_parent_dataset(&parent, name.as_ref())?;

        Ok(AppendableDataset1D {
            dataset,
            datatype: PhantomData,
        })
    }

    pub fn append_slice(&self, value: &[T]) -> Result<(), Hdf5Error> {
        let original_length = self.dataset.shape()[0];
        self.dataset
            .resize(original_length + value.len())
            .err_dataset(&self.dataset)?;
        self.dataset
            .write_slice(value, original_length..original_length + value.len())
            .err_dataset(&self.dataset)?;

        Ok(())
    }

    pub fn append_value(&self, value: T) -> Result<(), Hdf5Error> {
        self.append_slice(&[value])
    }
}
