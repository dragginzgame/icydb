use crate::{
    db::{
        data::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError},
        index::IndexKey,
    },
    value::Value,
};
use thiserror::Error as ThisError;

///
/// PrimaryKeyEquivalenceError
///
/// Index-layer primary-key equivalence failures when comparing an index-key
/// anchor against a semantic boundary value.
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum PrimaryKeyEquivalenceError {
    #[error("index anchor primary key decode failed: {source}")]
    AnchorDecode {
        #[source]
        source: StorageKeyDecodeError,
    },

    #[error("boundary primary key is not storage-key encodable: {source}")]
    BoundaryEncode {
        #[source]
        source: StorageKeyEncodeError,
    },
}

/// Compare an index-key primary-key payload with a semantic boundary key value.
pub(in crate::db) fn primary_key_matches_value(
    index_key: &IndexKey,
    boundary_key_value: &Value,
) -> Result<bool, PrimaryKeyEquivalenceError> {
    let anchor_key = index_key
        .primary_storage_key()
        .map_err(|source| PrimaryKeyEquivalenceError::AnchorDecode { source })?;
    let boundary_key = StorageKey::try_from_value(boundary_key_value)
        .map_err(|source| PrimaryKeyEquivalenceError::BoundaryEncode { source })?;

    Ok(anchor_key == boundary_key)
}
