pub mod build;
pub mod node;
pub mod types;
pub mod visit;

use crate::{build::BuildError, node::NodeError};
use thiserror::Error as ThisError;

///
/// Prelude
///

pub mod prelude {
    pub(crate) use crate::build::{schema_read, validate::validate_ident};
    pub use crate::{
        node::*,
        types::{Cardinality, Primitive, StoreType},
        visit::Visitor,
    };
    pub use candid::CandidType;
    pub use icydb_error::{ErrorTree, err};
    pub use icydb_paths::paths;
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
