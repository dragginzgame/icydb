use crate::db::identity::{EntityNameError, IndexName, IndexNameError};
use derive_more::Display;
use thiserror::Error as ThisError;

///
/// IndexId
///
/// Logical identifier for an index.
/// Combines entity identity and indexed field set into a stable, ordered name.
/// Used as the prefix component of all index keys.
///

#[derive(Clone, Copy, Debug, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexId(pub IndexName);

impl IndexId {
    /// Maximum sentinel value for stable-memory bounds.
    /// Used for upper-bound scans and fuzz validation.
    #[must_use]
    pub const fn max_storable() -> Self {
        Self(IndexName::max_storable())
    }
}

///
/// IndexIdError
/// Errors returned when constructing an [`IndexId`].
/// This surfaces identity validation failures.
///

#[derive(Debug, ThisError)]
pub enum IndexIdError {
    #[error("entity name invalid: {0}")]
    EntityName(#[from] EntityNameError),

    #[error("index name invalid: {0}")]
    IndexName(#[from] IndexNameError),
}
