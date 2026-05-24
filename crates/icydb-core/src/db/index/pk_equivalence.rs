//! Module: index::pk_equivalence
//! Responsibility: boundary contracts for index-key/semantic-value equivalence checks.
//! Does not own: key encoding rules or predicate planning.
//! Boundary: used by range/cursor validation paths.

use crate::db::{
    index::IndexKey,
    key_taxonomy::{CompactPrimaryKeyDecodeError, PrimaryKeyValue},
};
#[cfg(test)]
use crate::{
    db::data::primary_key_value_from_structural_value, error::InternalError, value::Value,
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
        source: CompactPrimaryKeyDecodeError,
    },

    #[cfg(test)]
    #[error("boundary primary key is not admitted: {source}")]
    BoundaryDecode {
        #[source]
        source: InternalError,
    },
}

/// Compare an index-key primary-key payload with a semantic boundary key value.
#[cfg(test)]
pub(in crate::db) fn primary_key_matches_value(
    index_key: &IndexKey,
    boundary_key_value: &Value,
) -> Result<bool, PrimaryKeyEquivalenceError> {
    // Normalize the semantic boundary value into the same scalar-or-composite
    // primary-key taxonomy used by the raw index key.
    let boundary_key = primary_key_value_from_structural_value(boundary_key_value)
        .map_err(|source| PrimaryKeyEquivalenceError::BoundaryDecode { source })?;

    primary_key_matches_primary_key_value(index_key, &boundary_key)
}

/// Compare an index-key primary-key payload with an already-normalized
/// scalar-or-composite boundary key.
pub(in crate::db) fn primary_key_matches_primary_key_value(
    index_key: &IndexKey,
    boundary_key: &PrimaryKeyValue,
) -> Result<bool, PrimaryKeyEquivalenceError> {
    let anchor_key = index_key
        .primary_key_value()
        .map_err(|source| PrimaryKeyEquivalenceError::AnchorDecode { source })?;

    Ok(&anchor_key == boundary_key)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            index::{IndexId, IndexKeyKind, RawIndexStoreKey},
            key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        },
        traits::Storable,
        types::{Principal, Subaccount, Timestamp, Ulid},
        value::StorageKey,
    };
    use std::borrow::Cow;

    fn index_key_with_primary_key_value(primary_key: &PrimaryKeyValue) -> IndexKey {
        let primary_key_bytes = IndexKey::compact_primary_key_value_bytes(primary_key);
        let index_id = IndexId::new(crate::types::EntityTag::new(7), 0);
        let mut bytes = Vec::new();
        bytes.push(IndexKeyKind::User as u8);
        bytes.extend_from_slice(&index_id.to_bytes());
        bytes.push(0);
        bytes.extend_from_slice(
            &u16::try_from(primary_key_bytes.len())
                .expect("compact primary-key size should fit in one index segment length")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&primary_key_bytes);

        let raw = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(bytes));

        IndexKey::try_from_raw(&raw).expect("test raw index key should decode")
    }

    #[test]
    fn pk_equivalence_matches_when_anchor_and_boundary_share_storage_key() {
        let cases = [
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Int(-7))),
                Value::Int(-7),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Nat(42))),
                Value::Nat(42),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Principal(
                    Principal::from_slice(&[9]),
                ))),
                Value::Principal(Principal::from_slice(&[9])),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Subaccount(
                    Subaccount::new([7; 32]),
                ))),
                Value::Subaccount(Subaccount::new([7; 32])),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Timestamp(
                    Timestamp::from_secs(17),
                ))),
                Value::Timestamp(Timestamp::from_secs(17)),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Ulid(
                    Ulid::from_u128(91),
                ))),
                Value::Ulid(Ulid::from_u128(91)),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Unit)),
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
    fn pk_equivalence_matches_composite_anchor_and_boundary_values() {
        let composite = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(42),
            PrimaryKeyComponent::Int(-7),
        ])
        .expect("test composite primary key should encode");
        let index_key = index_key_with_primary_key_value(&PrimaryKeyValue::Composite(composite));
        let boundary = Value::List(vec![Value::Nat(42), Value::Int(-7)]);

        assert!(
            primary_key_matches_value(&index_key, &boundary)
                .expect("composite primary-key comparison should succeed")
        );
    }

    #[test]
    fn pk_equivalence_rejects_non_storage_key_boundary_values() {
        let index_key =
            index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Nat(42)));
        let err = primary_key_matches_value(&index_key, &Value::Text("broken".to_string()))
            .expect_err("non-storage-key runtime value must be rejected");

        assert!(matches!(
            err,
            PrimaryKeyEquivalenceError::BoundaryDecode { .. }
        ));
    }

    #[test]
    fn pk_equivalence_reports_false_for_distinct_primary_key_values() {
        let index_key =
            index_key_with_primary_key_value(&PrimaryKeyValue::from(StorageKey::Nat(42)));

        assert!(
            !primary_key_matches_value(&index_key, &Value::Nat(99))
                .expect("distinct primary keys should still compare cleanly"),
        );
    }
}
