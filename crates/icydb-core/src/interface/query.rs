use crate::{
    db::query::{SaveQuery, v2::plan::LogicalPlan},
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::*,
};
use thiserror::Error as ThisError;

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("entity not found: {0}")]
    EntityNotFound(String),
}

impl From<QueryError> for InternalError {
    fn from(err: QueryError) -> Self {
        Self::new(err.class(), ErrorOrigin::Interface, err.to_string())
    }
}

impl QueryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::EntityNotFound(_) => ErrorClass::Unsupported,
        }
    }
}

/// Function pointer that executes a load query for a specific entity type.
pub type LoadHandler = fn(LogicalPlan) -> Result<Vec<Key>, InternalError>;

/// Function pointer that executes a save query for a specific entity type.
pub type SaveHandler = fn(SaveQuery) -> Result<Key, InternalError>;

/// Function pointer that executes a delete query for a specific entity type.
pub type DeleteHandler = fn(LogicalPlan) -> Result<Vec<Key>, InternalError>;

/// Metadata and typed handlers for a single entity path.
///
/// Generated actor code exposes a `dispatch_entity(path)` function that returns this,
/// letting you authorize per-entity before invoking the handlers. No canister
/// endpoints are generated automatically.
#[derive(Clone, Copy)]
pub struct EntityDispatch {
    pub entity_name: &'static str,
    pub path: &'static str,
    pub load_keys: LoadHandler,
    pub save_key: SaveHandler,
    pub delete_keys: DeleteHandler,
}
