//! Utilities for creating HDF5 files.
use std::path::Path;

/// Implementations of opening a NeXus file.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum FileCreator {
    /// On-disk - used at runtime
    Disk,
    /// In-memory file - used for tests, and for structure_verify.
    Memory,
    /// A file factory that unconditionally fails to create a file
    #[cfg(test)]
    AlwaysFails,
}

impl FileCreator {
    pub fn create_file(&self, name: impl AsRef<Path>) -> hdf5::Result<hdf5::File> {
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

            #[cfg(test)]
            Self::AlwaysFails => Err(hdf5::Error::Internal(format!(
                "{}::AlwaysFails injected a failure",
                std::any::type_name::<Self>()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FileWriterError;
    use crate::hdf::scope::with_nexus_file;
    use crate::hdf::traits::Hdf5ErrorContext;
    use std::path::PathBuf;

    #[test]
    fn test_create_in_memory_file() {
        let f = FileCreator::Memory.create_file("foo").unwrap();
        f.close().unwrap();
        let f = FileCreator::Memory.create_file("bar").unwrap();
        f.close().unwrap();

        // Check we can make the same filename again
        let f = FileCreator::Memory.create_file("bar").unwrap();
        f.close().unwrap();
    }

    #[test]
    fn test_with_nexus_file_success() {
        let factory = FileCreator::Memory;
        let path = PathBuf::from("test");

        let result = with_nexus_file(factory, &path, |f| {
            f.create_group("some_test_group")
                .err_parent_dataset(&f, "some_test_group")?;

            assert!(f.group("some_test_group").is_ok());

            Ok("result of file-writing")
        });
        println!("{:?}", result);
        assert_eq!(result.ok(), Some("result of file-writing"));
    }

    #[test]
    fn test_with_nexus_file_failure() {
        let factory = FileCreator::Memory;
        let path = PathBuf::from("test");

        let result: Result<(), _> =
            with_nexus_file(factory, &path, |_| Err(FileWriterError::UnitTestError));

        assert!(result.is_err_and(|err| matches!(err, FileWriterError::UnitTestError)));
    }

    #[test]
    fn test_with_nexus_file_creation_failure() {
        let factory = FileCreator::AlwaysFails;
        let path = PathBuf::from("test");
        let mut function_ran = false;

        let result = with_nexus_file(factory, &path, |_| {
            function_ran = true;
            Ok("it worked".to_owned())
        });

        assert!(!function_ran);
        assert!(result.is_err());
    }
}
