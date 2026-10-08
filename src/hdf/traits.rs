//! Traits related to HDF5 types.
use crate::hdf::error::Hdf5Error;
use hdf5::Location;

/// Add context to a HDF5 error (dataset and filename being written to)
pub trait Hdf5ErrorContext<T> {
    fn err_dataset(self, dataset: &Location) -> Result<T, Hdf5Error>;

    fn err_parent_dataset(self, parent: &Location, dataset: &str) -> Result<T, Hdf5Error>;

    fn err_attribute(self, parent: &Location, name: &str) -> Result<T, Hdf5Error>;
}

impl<T> Hdf5ErrorContext<T> for Result<T, hdf5::Error> {
    fn err_dataset(self, dataset: &Location) -> Result<T, Hdf5Error> {
        self.map_err(|e| Hdf5Error::WriteError {
            cause: Box::new(e),
            location: dataset.name(),
            filename: dataset.filename(),
        })
    }

    fn err_parent_dataset(self, parent: &Location, dataset: &str) -> Result<T, Hdf5Error> {
        self.map_err(|e| Hdf5Error::WriteError {
            cause: Box::new(e),
            location: format!("{}/{}", parent.name(), dataset),
            filename: parent.filename(),
        })
    }

    fn err_attribute(self, parent: &Location, name: &str) -> Result<T, Hdf5Error> {
        self.map_err(|e| Hdf5Error::WriteError {
            cause: Box::new(e),
            location: format!("{} attribute='{}'", parent.name(), name),
            filename: parent.filename(),
        })
    }
}
