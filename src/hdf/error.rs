//! HDF5 Error wrapper types.
use hdf5::types::StringError;
use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum Hdf5Error {
    #[error("Failed to write string to {} ({})", location, filename)]
    StringEncodingError {
        #[source]
        cause: Box<StringError>,
        filename: String,
        location: String,
    },

    #[error(
        "Invalid/unexpected shape of dataset at {} ({}) - got ndim={}, expected ndim={}",
        location,
        filename,
        actual_ndim,
        expected_ndim
    )]
    ShapeError {
        filename: String,
        location: String,
        expected_ndim: usize,
        actual_ndim: usize,
    },

    #[error("Failed to write to {} ({})", location, filename)]
    WriteError {
        #[source]
        cause: Box<hdf5::Error>,
        filename: String,
        location: String,
    },

    #[error("Failed to create file at {}", filename)]
    CreateError {
        #[source]
        cause: Box<hdf5::Error>,
        filename: String,
    },
}
