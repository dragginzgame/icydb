//! Resume anchors retain canonical bytes without materializing their prefix.

use super::*;
use crate::{db::index::IndexKeyEncodeError, error::InternalError};

#[test]
fn prefix_resume_anchors_match_owned_keys_across_identity_and_suffix_shapes() {
    let composite = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Int64(i64::MIN),
        ])
        .unwrap(),
    );
    for primary in [
        PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(u64::MAX)),
        PrimaryKeyValue::from(PrimaryKeyComponent::Nat128(u128::MAX)),
        composite,
    ] {
        let values = match primary.as_runtime_value() {
            Value::List(values) => values,
            value => vec![value],
        };
        for suffix in [&[][..], values.as_slice()] {
            for prefix_len in 0..=MAX_INDEX_FIELDS - suffix.len() {
                let prefix = (0..prefix_len)
                    .map(|position| encode_component(&Value::Text(format!("prefix\0{position}"))))
                    .collect::<Vec<_>>();
                let components = prefix
                    .iter()
                    .cloned()
                    .chain(suffix.iter().map(encode_component))
                    .collect::<Vec<_>>();
                for kind in [IndexKeyKind::User, IndexKeyKind::System] {
                    for generation in [0, 17] {
                        let id = IndexId::new_with_generation(EntityTag::new(42), 7, generation);
                        let expected = IndexKey::new_from_components_with_primary_key_value(
                            &id,
                            kind,
                            &components,
                            &primary,
                        )
                        .unwrap()
                        .to_raw()
                        .unwrap();
                        let anchor =
                            IndexKey::raw_from_prefix_and_suffix_values_with_primary_key_value(
                                id, kind, &prefix, suffix, &primary,
                            )
                            .unwrap();
                        assert_eq!(anchor.as_bytes(), expected.as_bytes());
                        assert_eq!(
                            IndexKey::try_from_raw(&anchor)
                                .unwrap()
                                .primary_key_value()
                                .unwrap(),
                            primary,
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn prefix_resume_anchors_preserve_typed_component_and_frame_refusals() {
    let primary = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7));
    for (prefix, suffix, expected) in [
        (
            vec![Vec::new()],
            vec![],
            InternalError::from(IndexKeyEncodeError::EmptySegment),
        ),
        (
            vec![vec![1; IndexKey::MAX_COMPONENT_SIZE + 1]],
            vec![],
            InternalError::index_component_exceeds_max_size(),
        ),
        (
            vec![vec![1; IndexKey::MAX_COMPONENT_SIZE + 1]; MAX_INDEX_FIELDS + 1],
            vec![],
            InternalError::index_component_exceeds_max_size(),
        ),
        (
            vec![vec![1]; MAX_INDEX_FIELDS + 1],
            vec![],
            IndexKeyEncodeError::TooManyComponents.into(),
        ),
        (
            vec![vec![1]; MAX_INDEX_FIELDS],
            vec![Value::Nat64(8)],
            IndexKeyEncodeError::TooManyComponents.into(),
        ),
        (
            vec![vec![1]],
            vec![Value::Null],
            InternalError::query_executor_invariant(),
        ),
        (
            vec![vec![1]; MAX_INDEX_FIELDS + 1],
            vec![Value::Null],
            InternalError::query_executor_invariant(),
        ),
        (
            vec![vec![1]],
            vec![Value::Text("x".repeat(IndexKey::MAX_COMPONENT_SIZE + 1))],
            InternalError::index_component_exceeds_max_size(),
        ),
        (
            vec![vec![1; IndexKey::MAX_COMPONENT_SIZE]; MAX_INDEX_FIELDS],
            vec![],
            IndexKeyEncodeError::SegmentTooLarge.into(),
        ),
    ] {
        let error = IndexKey::raw_from_prefix_and_suffix_values_with_primary_key_value(
            index_id(),
            IndexKeyKind::User,
            &prefix,
            &suffix,
            &primary,
        )
        .unwrap_err();
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.origin(), expected.origin());
        assert_eq!(error.diagnostic(), expected.diagnostic());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }
}
