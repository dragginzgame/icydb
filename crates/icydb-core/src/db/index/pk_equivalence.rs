//! Module: index::pk_equivalence
//! Responsibility: boundary contracts for index-key/semantic-value equivalence checks.
//! Does not own: key encoding rules or predicate planning.
//! Boundary: used by range/cursor validation paths.

use crate::{
    db::{
        data::{StorageKeyDecodeError, StorageKeyEncodeError},
        index::IndexKey,
    },
    value::{Value, storage_key_from_runtime_value},
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
    // Phase 1: decode the persisted primary-key anchor from the index key.
    let anchor_key = index_key
        .primary_storage_key()
        .map_err(|source| PrimaryKeyEquivalenceError::AnchorDecode { source })?;

    // Phase 2: encode the semantic boundary value to comparable storage form.
    let boundary_key = storage_key_from_runtime_value(boundary_key_value)
        .map_err(|source| PrimaryKeyEquivalenceError::BoundaryEncode { source })?;

    Ok(anchor_key == boundary_key)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::index::{IndexId, IndexKeyKind, RawIndexKey},
        traits::Storable,
        types::{Principal, Subaccount, Timestamp, Ulid},
        value::StorageKey,
    };
    use std::borrow::Cow;

    fn index_key_with_primary_storage_key(primary_key: StorageKey) -> IndexKey {
        let primary_key_bytes = primary_key
            .to_bytes()
            .expect("test primary key should encode");
        let index_id = IndexId::new(crate::types::EntityTag::new(7), 0);
        let mut bytes = Vec::new();
        bytes.push(IndexKeyKind::User as u8);
        bytes.extend_from_slice(&index_id.to_bytes());
        bytes.push(0);
        bytes.extend_from_slice(
            &u16::try_from(StorageKey::STORED_SIZE_USIZE)
                .expect("storage-key size should fit in one index segment length")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&primary_key_bytes);

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));

        IndexKey::try_from_raw(&raw).expect("test raw index key should decode")
    }

    #[test]
    fn pk_equivalence_matches_when_anchor_and_boundary_share_storage_key() {
        let cases = [
            (
                index_key_with_primary_storage_key(StorageKey::Int(-7)),
                Value::Int(-7),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Nat(42)),
                Value::Nat(42),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Principal(Principal::dummy(9))),
                Value::Principal(Principal::dummy(9)),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Subaccount(Subaccount::new(
                    [7; 32],
                ))),
                Value::Subaccount(Subaccount::new([7; 32])),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Timestamp(Timestamp::from_secs(17))),
                Value::Timestamp(Timestamp::from_secs(17)),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Ulid(Ulid::from_u128(91))),
                Value::Ulid(Ulid::from_u128(91)),
            ),
            (
                index_key_with_primary_storage_key(StorageKey::Unit),
                Value::Unit,
            ),
        ];

        for (index_key, boundary) in cases {
            assert!(
                primary_key_matches_value(&index_key, &boundary)
                    .expect("matching boundary should compare cleanly"),
                "expected anchor and boundary to match for {boundary:?}",
            );
        }
    }

    #[test]
    fn pk_equivalence_rejects_non_storage_key_boundary_values() {
        let index_key = index_key_with_primary_storage_key(StorageKey::Nat(42));
        let err = primary_key_matches_value(&index_key, &Value::Text("broken".to_string()))
            .expect_err("non-storage-key runtime value must be rejected");

        assert!(matches!(
            err,
            PrimaryKeyEquivalenceError::BoundaryEncode { .. }
        ));
    }

    #[test]
    fn pk_equivalence_reports_false_for_distinct_storage_keys() {
        let index_key = index_key_with_primary_storage_key(StorageKey::Nat(42));

        assert!(
            !primary_key_matches_value(&index_key, &Value::Nat(99))
                .expect("distinct storage keys should still compare cleanly"),
        );
    }
}
