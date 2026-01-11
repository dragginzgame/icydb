//! Core runtime for IcyDB: entity traits, values, executors, visitors, and
//! the ergonomics exported via the `prelude`.
pub mod db;
pub mod hash;
pub mod index;
pub mod interface;
pub mod key;
pub mod macros;
pub mod obs;
pub mod runtime_error;
pub mod serialize;
pub mod traits;
pub mod types;
pub mod value;
pub mod view;
pub mod visitor;

pub(crate) use runtime_error::RuntimeError;

pub use index::IndexSpec;
pub use key::Key;
pub use serialize::{deserialize, serialize};
pub use value::Value;

///
/// CONSTANTS
///

/// Maximum number of indexed fields allowed on an entity.
///
/// This limit keeps hashed index keys within bounded, storable sizes and
/// simplifies sizing tests in the stores.
pub const MAX_INDEX_FIELDS: usize = 4;

use candid::CandidType;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;
use traits::Visitable;
use visitor::VisitorIssues;

///
/// Error
///
/// top level error should handle all sub-errors, but not expose the candid types
/// as that would be a lot of them
///

#[derive(CandidType, Debug, Deserialize, Serialize, ThisError)]
#[error("{0}")]
pub struct Error(pub String);

impl From<VisitorIssues> for runtime_error::RuntimeError {
    fn from(err: VisitorIssues) -> Self {
        Self::new(
            runtime_error::ErrorClass::Unsupported,
            runtime_error::ErrorOrigin::Executor,
            err.to_string(),
        )
    }
}

impl From<RuntimeError> for Error {
    fn from(err: runtime_error::RuntimeError) -> Self {
        Self(err.display_with_class())
    }
}

/// sanitize
pub fn sanitize(node: &mut dyn Visitable) -> Result<(), runtime_error::RuntimeError> {
    visitor::sanitize(node).map_err(runtime_error::RuntimeError::from)
}

/// validate
pub fn validate(node: &dyn Visitable) -> Result<(), runtime_error::RuntimeError> {
    visitor::validate(node).map_err(runtime_error::RuntimeError::from)
}
