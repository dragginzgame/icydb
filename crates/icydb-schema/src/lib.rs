pub mod build;
pub mod error;
pub mod node;
pub mod types;
pub mod validate;
pub mod visit;

/// Maximum length for entity schema identifiers.
pub const MAX_ENTITY_NAME_LEN: usize = 64;

/// Maximum length for field schema identifiers.
pub const MAX_FIELD_NAME_LEN: usize = 64;

/// Maximum number of fields allowed in a derived index.
pub const MAX_INDEX_FIELDS: usize = 4;

/// Maximum length for derived index identifiers.
pub const MAX_INDEX_NAME_LEN: usize =
    MAX_ENTITY_NAME_LEN + (MAX_INDEX_FIELDS * (1 + MAX_FIELD_NAME_LEN));

use crate::{build::BuildError, node::NodeError};
use thiserror::Error as ThisError;

///
/// Prelude
///

pub mod prelude {
    pub(crate) use crate::build::schema_read;
    pub use crate::{
        err,
        error::ErrorTree,
        node::*,
        types::{Cardinality, Primitive},
        visit::Visitor,
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
}

///
/// Error
///

#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    BuildError(#[from] BuildError),

    #[error(transparent)]
    NodeError(#[from] NodeError),
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{Error, build::BuildError, error::ErrorTree, node::NodeError};

    #[test]
    fn build_errors_remain_in_build_boundary() {
        let schema_error = Error::from(BuildError::Validation(ErrorTree::from(
            "missing schema relation target",
        )));

        match schema_error {
            Error::BuildError(BuildError::Validation(tree)) => {
                assert!(
                    tree.messages()
                        .iter()
                        .any(|message| message == "missing schema relation target"),
                    "build validation errors must remain wrapped as build-boundary failures",
                );
            }
            Error::NodeError(_) => {
                panic!("build validation failures must not be remapped into node-boundary errors");
            }
        }
    }

    #[test]
    fn node_errors_remain_in_node_boundary() {
        let schema_error = Error::from(NodeError::PathNotFound("entity.user_id".to_string()));

        match schema_error {
            Error::NodeError(NodeError::PathNotFound(path)) => {
                assert_eq!(path, "entity.user_id");
            }
            Error::NodeError(NodeError::IncorrectNodeType(path)) => {
                panic!("unexpected node error kind after conversion for path {path}");
            }
            Error::BuildError(_) => {
                panic!("node errors must not be remapped into build-boundary failures");
            }
        }
    }
}
