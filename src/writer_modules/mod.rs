//! Contains the definitions of all writer modules implemented by this file-writer.
use crate::writer_module_factories::WriterModuleFactories;
use crate::writer_modules::run_time_metadata::RunTimeMetadata;
use instrument_name::InstrumentName;
use ixseblock::IXseblock;
use nxevent_data::NXeventdata;

mod instrument_name;
mod ixseblock;
mod nxevent_data;
mod run_time_metadata;

/// The set of writer-modules available in this file writer.
///
/// New writer modules must be added to this list in order to be usable.
pub fn default_registry() -> WriterModuleFactories {
    let mut registry = WriterModuleFactories::empty();
    registry.register_module::<IXseblock>();
    registry.register_module::<NXeventdata>();
    registry.register_module::<InstrumentName>();
    registry.register_module::<RunTimeMetadata>();
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
