//! Contains the definitions of all writer modules implemented by this file-writer.
use crate::writer_module_registry::WriterModuleFactories;
use ixseblock::IXseblock;
use nxevent_data::NXeventdata;

mod ixseblock;
mod nxevent_data;

/// The set of writer-modules available in this file writer.
///
/// New writer modules must be added to this list in order to be usable.
pub fn default_registry() -> WriterModuleFactories {
    let mut registry = WriterModuleFactories::empty();
    registry.register_module::<IXseblock>();
    registry.register_module::<NXeventdata>();
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_registry_does_not_have_duplicate_names() {
        assert!(
            !default_registry().has_duplicated_module_names(),
            "Default registry has modules with duplicate names"
        );
    }
}
