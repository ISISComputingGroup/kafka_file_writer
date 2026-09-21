//! Utilities for creating HDF5 files.
use std::path::Path;

/// Implementations of opening a NeXus file.
pub enum FileFactory {
    /// On-disk - used at runtime
    Disk,
    /// In-memory file - used for tests, and for structure_verify.
    Memory,
    /// A file factory that unconditionally fails to create a file
    AlwaysFails,
}

impl FileFactory {
    pub fn create_file(&self, name: &impl AsRef<Path>) -> hdf5::Result<hdf5::File> {
        match self {
            Self::Disk => hdf5::File::create_excl(name),

            Self::Memory => {
                // Create with 64MB hdf5 increment (amount in-memory file grows by when
                // it runs out of alloc'd memory)
                hdf5::File::with_options()
                    .with_fapl(|props| props.core_options(64 * 1024 * 1024, false))
                    // Sometimes fails to create if name is not unique...
                    .create_excl(format!("name-{}", uuid::Uuid::new_v4()))
            }

            Self::AlwaysFails => Err(hdf5::Error::Internal(
                format!(
                    "{}::AlwaysFails injected a failure",
                    std::any::type_name::<Self>()
                )
                .to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FileWriterError;
    use crate::hdf::scope::with_nexus_file;
    use std::path::PathBuf;

    #[test]
    fn test_create_in_memory_file() {
        let f = FileFactory::Memory.create_file(&"foo").unwrap();
        f.close().unwrap();
        let f = FileFactory::Memory.create_file(&"bar").unwrap();
        f.close().unwrap();

        // Check we can make the same filename again
        let f = FileFactory::Memory.create_file(&"bar").unwrap();
        f.close().unwrap();
    }

    #[test]
    fn test_with_nexus_file_success() {
        let factory = FileFactory::Memory;
        let path = PathBuf::from("test");

        let result = with_nexus_file(factory, &path, |f| {
            f.create_group("some_test_group")
                .map_err(|e| FileWriterError::HDF5Error {
                    file_name: path.display().to_string(),
                    cause: e,
                    dataset_location: "/".to_owned(),
                })?;

            assert!(f.group("some_test_group").is_ok());

            Ok("result of file-writing")
        });
        println!("{:?}", result);
        assert_eq!(result.ok(), Some("result of file-writing"));
    }

    #[test]
    fn test_with_nexus_file_failure() {
        let factory = FileFactory::Memory;
        let path = PathBuf::from("test");

        let result: Result<(), _> =
            with_nexus_file(factory, &path, |_| Err(FileWriterError::UnitTestError));

        assert!(result.is_err_and(|err| match err {
            FileWriterError::UnitTestError => true,
            _ => false,
        }));
    }

    #[test]
    fn test_with_nexus_file_creation_failure() {
        let factory = FileFactory::AlwaysFails;
        let path = PathBuf::from("test");
        let mut function_ran = false;

        let result = with_nexus_file(factory, &path, |_| {
            function_ran = true;
            Ok("it worked".to_owned())
        });

        assert_eq!(function_ran, false);
        assert!(result.is_err());
    }
}
