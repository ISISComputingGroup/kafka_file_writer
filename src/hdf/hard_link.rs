use crate::error::FileWriterError;
use crate::hdf::attr::add_simple_string_attribute;
use crate::hdf::traits::Hdf5ErrorContext;
use crate::run_writer::nexus_structure::HardLink;
use hdf5::Group;
use log::{debug, warn};

pub fn create_hard_link(parent: &Group, link: &HardLink) -> Result<(), FileWriterError> {
    if parent.link_exists(&link.name) {
        // Link already created; nothing to do
        debug!(
            "Link from {}/{} already exists - skipping creating link",
            parent.name(),
            link.name
        );
        return Ok(());
    }

    if let Ok(target) = parent.dataset(link.target.as_str()) {
        // Add the target attribute as the "canonical" location of the dataset.
        match add_simple_string_attribute(&target, "target", &target.name()) {
            Ok(_) => {}
            Err(_) => {
                debug!(
                    "Could not write 'target' attribute to {} - already exists? Skipping.",
                    target.name()
                );
            }
        }
        parent
            .link_hard(&link.target, &link.name)
            .err_parent_dataset(parent, &link.name)?;
    } else {
        warn!(
            "Cannot create link from {}/{} to {} - target dataset does not exist",
            parent.name(),
            link.name,
            link.target
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hdf::file_creator::FileCreator;
    use hdf5::types::VarLenAscii;

    #[test]
    fn test_create_hard_link_to_nonexistent_dataset() {
        for target in ["/group/my_link_target", "my_link_target"] {
            let file = FileCreator::Memory.create_file("unittest").unwrap();

            let group = file.create_group("group").unwrap();

            create_hard_link(
                &group,
                &HardLink {
                    name: "my_link_name".to_string(),
                    target: target.to_string(),
                },
            )
            .unwrap();

            // Should not exist - target didn't exist
            assert!(!group.link_exists("my_link_name"));
        }
    }

    #[test]
    fn test_create_hard_link() {
        for target in &["/group/my_link_target", "my_link_target"] {
            let file = FileCreator::Memory.create_file("unittest").unwrap();

            let group = file.create_group("group").unwrap();
            let target_dataset = group.new_dataset::<i64>().create("my_link_target").unwrap();
            target_dataset.write_scalar(&123).unwrap();

            create_hard_link(
                &group,
                &HardLink {
                    name: "my_link_name".to_string(),
                    target: target.to_string(),
                },
            )
            .unwrap();

            assert!(group.link_exists("my_link_name"));
            assert_eq!(
                group
                    .dataset("my_link_name")
                    .unwrap()
                    .read_scalar::<i64>()
                    .unwrap(),
                123
            );

            // creating the link again should not error
            create_hard_link(
                &group,
                &HardLink {
                    name: "my_link_name".to_string(),
                    target: "my_link_target".to_string(),
                },
            )
            .unwrap();

            // Target attribute should always be absolute
            assert_eq!(
                target_dataset
                    .attr("target")
                    .unwrap()
                    .read_scalar::<VarLenAscii>()
                    .unwrap()
                    .as_str(),
                "/group/my_link_target"
            );
        }
    }
}
