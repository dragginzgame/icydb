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

///
/// PrimaryKeyEquivalenceError
///
/// Index-layer primary-key equivalence failures when comparing an index-key
/// anchor against a semantic boundary value.
///

#[derive(Debug)]
pub(in crate::db) enum PrimaryKeyEquivalenceError {
    AnchorDecode {
        source: CompactPrimaryKeyDecodeError,
    },

    #[cfg(test)]
    BoundaryDecode { source: InternalError },
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
        types::{Principal, Subaccount, Timestamp, Ulid},
    };
    use ic_memory::stable_structures::Storable;
    use std::borrow::Cow;

    fn index_key_with_primary_key_value(primary_key: &PrimaryKeyValue) -> IndexKey {
        let primary_key_bytes = IndexKey::compact_primary_key_value_bytes(primary_key)
            .expect("test primary key should encode");
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
    fn pk_equivalence_matches_when_anchor_and_boundary_share_primary_key_component() {
        let cases = [
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Int64(-7),
                )),
                Value::Int64(-7),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Nat64(42),
                )),
                Value::Nat64(42),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Principal(Principal::from_slice(&[9])),
                )),
                Value::Principal(Principal::from_slice(&[9])),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
                )),
                Value::Subaccount(Subaccount::from_array([7; 32])),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Timestamp(Timestamp::from_secs(17)),
                )),
                Value::Timestamp(Timestamp::from_secs(17)),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(
                    PrimaryKeyComponent::Ulid(Ulid::from_u128(91)),
                )),
                Value::Ulid(Ulid::from_u128(91)),
            ),
            (
                index_key_with_primary_key_value(&PrimaryKeyValue::from(PrimaryKeyComponent::Unit)),
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
            PrimaryKeyComponent::Nat64(42),
            PrimaryKeyComponent::Int64(-7),
        ])
        .expect("test composite primary key should encode");
        let index_key = index_key_with_primary_key_value(&PrimaryKeyValue::Composite(composite));
        let boundary = Value::List(vec![Value::Nat64(42), Value::Int64(-7)]);

        assert!(
            primary_key_matches_value(&index_key, &boundary)
                .expect("composite primary-key comparison should succeed")
        );
    }

    #[test]
    fn pk_equivalence_rejects_non_primary_key_component_boundary_values() {
        let index_key = index_key_with_primary_key_value(&PrimaryKeyValue::from(
            PrimaryKeyComponent::Nat64(42),
        ));
        let err = primary_key_matches_value(&index_key, &Value::Text("broken".to_string()))
            .expect_err("non-storage-key runtime value must be rejected");

        std::assert_matches!(err, PrimaryKeyEquivalenceError::BoundaryDecode { .. });
    }

    #[test]
    fn pk_equivalence_reports_false_for_distinct_primary_key_values() {
        let index_key = index_key_with_primary_key_value(&PrimaryKeyValue::from(
            PrimaryKeyComponent::Nat64(42),
        ));

        assert!(
            !primary_key_matches_value(&index_key, &Value::Nat64(99))
                .expect("distinct primary keys should still compare cleanly"),
        );
    }
}
