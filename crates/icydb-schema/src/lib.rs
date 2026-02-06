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
