//! Support for parsing a `nexus_structure` from a run start message.
use crate::error::FileWriterError;
use crate::run_start_parameters::RunStartParameters;
use crate::subscription::Subscription;
use crate::writer_module::{WriterModule, WriterModuleCreationError};
use ahash::HashMap;
use miette::{Diagnostic, SourceOffset};
use serde::Deserialize;
use serde_json::{Value, json};
use std::iter::Iterator;
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum NexusStructureError {
    #[error("Error parsing NeXus structure")]
    FailedParse {
        #[source]
        cause: serde_json::Error,

        #[source_code]
        source_code: String,

        #[label("{cause}")]
        location: SourceOffset,
    },

    #[error("Unknown/invalid child type in nexus_structure at '{}': JSON for invalid child was:\n{}", .location, .invalid_json)]
    UnknownChildType {
        invalid_json: String,
        location: String,
    },

    #[error("Failed creating writer module '{}' at '{}'", .module_name, .location)]
    WriterModuleCreationError {
        #[source]
        #[diagnostic_source]
        cause: WriterModuleCreationError,

        module_name: String,
        location: String,
    },

    #[error("Failed creating writer module at '{}': '{}' is not a valid writer module name", .location, .name)]
    UnknownWriterModule { name: String, location: String },
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NexusFileStructure {
    #[serde(default)]
    pub children: Vec<NexusStructureItem>,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

/// This enum with only one variant exists so that
/// we can reject:
/// ```
/// "type": "something_invalid"
/// ```
/// In something that otherwise looks like the structure
/// of a ``nexus_group``.
#[derive(Deserialize, Debug, PartialEq, Eq)]
enum NexusGroupType {
    #[serde(rename = "group")]
    Group,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NexusGroup {
    #[serde(rename = "type")]
    _type: NexusGroupType,

    pub name: String,

    #[serde(default)]
    pub children: Vec<NexusStructureItem>,

    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

fn empty_dict() -> Value {
    json!({})
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NexusWriterModule {
    pub module: String,

    #[serde(default = "empty_dict")]
    pub config: Value,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum NexusStructureItem {
    Group(NexusGroup),
    WriterModule(NexusWriterModule),
    Invalid(Value), // Note: must be last - this matches *anything* with serde(untagged)
}

impl FromStr for NexusFileStructure {
    type Err = NexusStructureError;

    fn from_str(json: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(json).map_err(|cause| {
            let location = SourceOffset::from_location(json, cause.line(), cause.column());
            NexusStructureError::FailedParse {
                cause,
                source_code: json.to_string(),
                location,
            }
        })
    }
}

#[derive(Debug)]
pub enum ResolvedItem {
    Group {
        name: String,
        attributes: HashMap<String, Value>,
        children: Vec<ResolvedItem>,
    },
    Writer {
        module: Box<dyn WriterModule>,
        subscriptions: Vec<Subscription>,
    },
}

impl NexusFileStructure {
    pub fn from_local_file(file_path: &impl AsRef<Path>) -> Result<Self, FileWriterError> {
        let structure: String =
            std::fs::read_to_string(file_path.as_ref()).map_err(|e| FileWriterError::IoError {
                cause: e,
                filename: file_path.as_ref().display().to_string(),
            })?;

        structure
            .parse()
            .map_err(FileWriterError::NexusStructureError)
    }

    pub fn from_run_start(run_start: &RunStartParameters) -> Result<Self, FileWriterError> {
        run_start
            .nexus_structure
            .parse()
            .map_err(FileWriterError::NexusStructureError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_nexus_structure() {
        let json = r#"
{
    "children": []
}
"#;

        let structure: NexusFileStructure = json.parse().unwrap();
        assert_eq!(structure.children.len(), 0)
    }

    #[test]
    fn test_parse_nexus_structure_with_groups() {
        let json = r#"
{
    "children": [
      {
        "type": "group",
        "name": "foo"
      },
      {
        "type": "group",
        "name": "bar"
      }
    ]
}
"#;

        let structure: NexusFileStructure = json.parse().unwrap();
        assert_eq!(structure.children.len(), 2);
        assert!(structure.children.iter().any(|x| match x {
            NexusStructureItem::Group(g) => g.name == "foo",
            _ => false,
        }));
        assert!(structure.children.iter().any(|x| match x {
            NexusStructureItem::Group(g) => g.name == "bar",
            _ => false,
        }));
    }

    #[test]
    fn test_parse_nexus_structure_with_invalid_group_type() {
        let json = r#"
{
    "children": [
      {
        "type": "group_blah_blah_blah",
        "name": "foo"
      }
    ]
}
"#;

        let structure = json.parse::<NexusFileStructure>().unwrap();
        assert_eq!(
            structure.children[0],
            NexusStructureItem::Invalid(json!({"type": "group_blah_blah_blah", "name": "foo"}))
        );
    }

    #[test]
    fn test_parse_nexus_structure_with_writer_module() {
        let json = r#"
{
    "children": [
      {
        "module": "amazing_writer",
        "config": {
          "special_parameter": "very_special",
          "pi": 3,
          "extremely_special": [
            ["wow", "I", "am"],
            {"so": "special"}
          ]
        }
      }
    ]
}
"#;

        let structure: NexusFileStructure = json.parse().unwrap();
        match &structure.children[0] {
            NexusStructureItem::WriterModule(m) => {
                assert_eq!(m.module, "amazing_writer");
            }
            _ => panic!("should have been a writer"),
        }
    }

    #[test]
    fn test_parse_nexus_structure_with_writer_module_no_config() {
        let json = r#"
{
    "children": [
      {
        "module": "amazing_writer"
      }
    ]
}
"#;

        let structure: NexusFileStructure = json.parse().unwrap();
        match &structure.children[0] {
            NexusStructureItem::WriterModule(m) => {
                assert_eq!(m.module, "amazing_writer");
            }
            _ => panic!("should have been a writer"),
        }
    }

    #[test]
    fn test_parse_nexus_structure_with_writer_module_empty_config() {
        let json = r#"
{
    "children": [
      {
        "module": "amazing_writer",
        "config": {}
      }
    ]
}
"#;

        let structure: NexusFileStructure = json.parse().unwrap();
        match &structure.children[0] {
            NexusStructureItem::WriterModule(m) => {
                assert_eq!(m.module, "amazing_writer");
            }
            _ => panic!("should have been a writer"),
        }
    }
}
