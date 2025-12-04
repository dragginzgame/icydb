use crate::{
    Error, Key,
    db::query::{DeleteQuery, LoadQuery, SaveQuery},
    interface::InterfaceError,
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

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        InterfaceError::from(err).into()
    }
}

/// Function pointer that executes a load query for a specific entity type.
pub type LoadHandler = fn(LoadQuery) -> Result<Vec<Key>, Error>;

/// Function pointer that executes a save query for a specific entity type.
pub type SaveHandler = fn(SaveQuery) -> Result<Key, Error>;

/// Function pointer that executes a delete query for a specific entity type.
pub type DeleteHandler = fn(DeleteQuery) -> Result<Vec<Key>, Error>;

/// Metadata and typed handlers for a single entity path.
///
/// Generated actor code exposes a `dispatch_entity(path)` function that returns this,
/// letting you authorize per-entity before invoking the handlers. No canister
/// endpoints are generated automatically.
#[derive(Clone, Copy)]
pub struct EntityDispatch {
    pub entity_id: u64,
    pub path: &'static str,
    pub load_keys: LoadHandler,
    pub save_key: SaveHandler,
    pub delete_keys: DeleteHandler,
}
