//! Registry of writer modules usable by the filewriter at runtime.
use crate::run_writer::run_start_parameters::RunStartParameters;
use crate::writer_module::{
    WriterModule, WriterModuleCreateResult, WriterModuleCreationError, WriterModuleSpec,
};
use ahash::{HashMap, HashMapExt};
use log::warn;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The `WriterModuleRegistry` is, fundamentally, a Map of writer names to creation functions.
/// This is the expected signature of one such creation function.
type WriterModuleCreateFn =
    fn(
        hdf5::Group,
        &RunStartParameters,
        Value,
    ) -> Result<WriterModuleCreateResult<Box<dyn WriterModule>>, WriterModuleCreationError>;

/// Create a type-erased writer module of type `M` from a runtime configuration, `writer_config`.
///
/// `writer_config` is deserialized from the JSON `nexus_structure` in a run start message.
fn create_writer_module<M>(
    group: hdf5::Group,
    run_start_parameters: &RunStartParameters,
    writer_config: Value,
) -> Result<WriterModuleCreateResult<Box<dyn WriterModule>>, WriterModuleCreationError>
where
    M: WriterModuleSpec + 'static,
{
    let parsed_config: M::Config = parse_config(writer_config)?;
    let result = M::create(group, run_start_parameters, parsed_config)?;
    Ok(WriterModuleCreateResult {
        module: Box::new(result.module),
        subscriptions: result.subscriptions,
    })
}

/// Holder struct for writer module creation functions.
pub struct WriterModuleFactories {
    modules: HashMap<&'static str, WriterModuleCreateFn>,
    has_duplicated_module_names: bool,
}

impl WriterModuleFactories {
    /// Create an empty set of writer module factories.
    pub fn empty() -> WriterModuleFactories {
        WriterModuleFactories {
            modules: HashMap::new(),
            has_duplicated_module_names: false,
        }
    }

    /// Register a new writer-module, of type `M`.
    pub fn register_module<M>(&mut self)
    where
        M: WriterModuleSpec + 'static,
    {
        if self
            .modules
            .insert(M::NAME, create_writer_module::<M>)
            .is_some()
        {
            warn!(
                "Duplicated module name in WriterModuleFactories: {}. Using most-recently inserted module.",
                M::NAME
            );
            self.has_duplicated_module_names = true;
        }
    }

    pub fn get(&self, name: &str) -> Option<WriterModuleCreateFn> {
        self.modules.get(name).copied()
    }

    pub fn has_duplicated_module_names(&self) -> bool {
        self.has_duplicated_module_names
    }
}

/// Helper to parse a writer-module specific config.
///
/// On success, return ``Ok(T)``.
///
/// On failure, return ``Err(WriterModuleFactoryError::ConfigParseError)`` containing
/// context.
fn parse_config<T>(json: Value) -> Result<T, WriterModuleCreationError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(json).map_err(|cause| WriterModuleCreationError::ConfigParseError {
        cause,
        config_type_name: std::any::type_name::<T>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_factory::FileFactory;
    use crate::hdf::scope::with_nexus_file;
    use crate::stream::traits::KafkaMessageMeta;
    use crate::writer_module::WriterModuleError;
    use hdf5::Group;
    use isis_streaming_data_types::DeserializedMessage;
    use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::RunStop;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize, Debug)]
    struct TestConfig {
        foo: i32,
        bar: Vec<i32>,
    }

    #[derive(Debug)]
    struct TestModule {}

    impl WriterModule for TestModule {
        fn on_message(
            &mut self,
            _: &KafkaMessageMeta,
            _: &DeserializedMessage,
        ) -> Result<(), WriterModuleError> {
            Ok(())
        }
        fn on_run_stop(
            &mut self,
            _: &KafkaMessageMeta,
            _: &RunStop,
        ) -> Result<(), WriterModuleError> {
            Ok(())
        }
        fn flush(&mut self) -> Result<(), WriterModuleError> {
            Ok(())
        }
        fn finished(&self) -> bool {
            true
        }
    }

    impl WriterModuleSpec for TestModule {
        type Config = TestConfig;
        const NAME: &'static str = "TEST_WRITER";
        fn create(
            _: Group,
            _: &RunStartParameters,
            _: Self::Config,
        ) -> Result<WriterModuleCreateResult<Self>, WriterModuleCreationError> {
            Ok(WriterModuleCreateResult {
                module: TestModule {},
                subscriptions: vec![],
            })
        }
    }

    #[test]
    fn test_parse_config() {
        let val: TestConfig = parse_config(json!({"foo": 1, "bar": [1, 2, 3]})).unwrap();
        assert_eq!(val.foo, 1);
        assert_eq!(val.bar, [1, 2, 3]);
    }

    #[test]
    fn test_parse_config_fail() {
        let json = json!({"foo": 1});
        assert!(parse_config::<TestConfig>(json).is_err_and(|err| {
            matches!(
                err,
                WriterModuleCreationError::ConfigParseError {
                    cause: serde_json::error::Error { .. },
                    ..
                }
            )
        }))
    }

    #[test]
    fn test_factories_detects_duplicated_module_names() {
        let mut factories = WriterModuleFactories::empty();
        factories.register_module::<TestModule>();
        assert!(!factories.has_duplicated_module_names());
        factories.register_module::<TestModule>();
        assert!(factories.has_duplicated_module_names());
    }

    #[test]
    fn test_writer_module_creation() {
        let mut factories = WriterModuleFactories::empty();
        factories.register_module::<TestModule>();

        let create_fn = factories.get("TEST_WRITER").unwrap();
        let rsp = RunStartParameters::default();
        let config = json!({
            "foo": 1,
            "bar": [2, 3, 4],
        });

        with_nexus_file(FileFactory::Memory, "test", |f| {
            let group = f.create_group("test").unwrap();
            create_fn(group, &rsp, config).unwrap();
            Ok(())
        })
        .unwrap();
    }
}
