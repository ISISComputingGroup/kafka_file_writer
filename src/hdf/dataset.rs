use crate::hdf::error::Hdf5Error;
use crate::hdf::traits::Hdf5ErrorContext;
use hdf5::{Dataset, Group, H5Type, SimpleExtents};
use std::marker::PhantomData;

/// Appendable 1-dimensional typed HDF5 dataset.
#[derive(Debug)]
pub struct AppendableDataset1D<T: H5Type> {
    dataset: Dataset,
    datatype: PhantomData<T>,
}

impl<T: H5Type> AppendableDataset1D<T> {
    pub fn create_dataset(
        parent: &Group,
        name: impl AsRef<str>,
        chunk_size: usize,
    ) -> Result<Self, Hdf5Error> {
        let dataset = parent
            .new_dataset::<T>()
            .shape(SimpleExtents::resizable([0]))
            .chunk([chunk_size])
            .chunk_cache(127, 4 * chunk_size * size_of::<T>(), 1.0)
            .deflate(1)
            .create(name.as_ref())
            .err_parent_dataset(parent, name.as_ref())?;

        Ok(AppendableDataset1D {
            dataset,
            datatype: PhantomData,
        })
    }

    /// Append a slice of values to this dataset
    pub fn append_slice(&self, value: &[T]) -> Result<(), Hdf5Error> {
        if value.is_empty() {
            return Ok(()); // nothing to do
        }
        let original_length =
            *self
                .dataset
                .shape()
                .first()
                .ok_or_else(|| Hdf5Error::ShapeError {
                    filename: self.dataset.filename(),
                    location: self.dataset.name(),
                    actual_ndim: self.dataset.shape().len(),
                    expected_ndim: 1,
                })?;
        self.dataset
            .resize(original_length + value.len())
            .err_dataset(&self.dataset)?;
        self.dataset
            .write_slice(value, original_length..original_length + value.len())
            .err_dataset(&self.dataset)?;

        Ok(())
    }

    /// Append a single value to this dataset.
    pub fn append_value(&self, value: T) -> Result<(), Hdf5Error> {
        self.append_slice(&[value])
    }

    /// Overwrite this entire dataset with a new slice.
    pub fn overwrite_with_slice(&self, values: &[T]) -> Result<(), Hdf5Error> {
        self.dataset
            .resize(values.len())
            .err_dataset(&self.dataset)?;
        self.dataset
            .write_slice(values, 0..values.len())
            .err_dataset(&self.dataset)?;
        Ok(())
    }

    /// Get the underlying HDF5 dataset handle
    pub fn raw_dataset(&self) -> &Dataset {
        &self.dataset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;

    #[test]
    fn test_write_to_appendable_dataset() {
        let file = FileCreator::Memory.create_file("unittest").unwrap();
        let group = file.create_group("group").unwrap();
        let ds = AppendableDataset1D::create_dataset(&group, "test_dataset", 1024).unwrap();

        ds.append_slice(&[]).unwrap();
        ds.append_slice(&[1, 2, 3]).unwrap();
        ds.append_value(4).unwrap();
        ds.append_slice(&[5]).unwrap();
        ds.append_slice(&[6]).unwrap();
        ds.append_slice(&[]).unwrap();

        assert_eq!(
            file.group("group")
                .unwrap()
                .dataset("test_dataset")
                .unwrap()
                .read_slice_1d(..)
                .unwrap()
                .as_slice(),
            Some([1, 2, 3, 4, 5, 6].as_slice())
        );

        ds.overwrite_with_slice(&[33, 22, 11]).unwrap();
        ds.append_slice(&[44]).unwrap();

        assert_eq!(
            file.group("group")
                .unwrap()
                .dataset("test_dataset")
                .unwrap()
                .read_slice_1d(..)
                .unwrap()
                .as_slice(),
            Some([33, 22, 11, 44].as_slice())
        );
    }
}
