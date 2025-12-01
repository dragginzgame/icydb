pub mod query;

use thiserror::Error as ThisError;

///
/// InterfaceError
///

#[derive(Debug, ThisError)]
pub enum InterfaceError {
    #[error(transparent)]
    QueryError(#[from] query::QueryError),
}
