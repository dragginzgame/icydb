use super::{
    IndexPrefixCardinality, IndexPrefixCardinalityDelta, IndexPrefixCardinalityFirstKey,
    IndexPrefixCardinalityKey, PrefixCardinalityDelta, apply_signed_count_delta,
    checked_metadata_count_increment,
};
use crate::{
    MAX_INDEX_FIELDS,
    db::{
        index::{
            EncodedValue, IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
            UserIndexPrefixCardinalityKey,
        },
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
    },
    types::EntityTag,
    value::Value,
};
use std::{collections::BTreeMap, ops::Bound};

#[test]
fn retained_prefix_limit_releases_optional_counts_without_rejecting_writes() {
    let id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let present = IndexEntryValue::presence();
    for width in [1, 2] {
        let key = |value: usize| {
            let components = vec![
                EncodedValue::try_new(&Value::Nat64(value as u64))
                    .unwrap()
                    .into_bytes();
                width
            ];
            let raw = IndexKey::new_from_components_with_primary_key_value(
                &id,
                IndexKeyKind::User,
                &components,
                &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value as u64)),
            )
            .unwrap()
            .to_raw()
            .unwrap();
            (raw, components)
        };
        let mut counts = IndexPrefixCardinality::synchronized_empty();
        let rows = IndexPrefixCardinality::MAX_RETAINED_PREFIXES / width;
        for value in 0..rows {
            counts.apply_insert(&key(value).0, None, &present);
        }
        counts.mark_synchronized(7);
        let (first, components) = key(0);
        assert_eq!(
            counts.first_component_counts.len() + counts.counts.len(),
            IndexPrefixCardinality::MAX_RETAINED_PREFIXES
        );
        assert_eq!(
            counts.exact_count(7, IndexKeyKind::User, id, &components),
            Some(1)
        );

        // Multiplicity changes at the cap retain exact evidence; the limit
        // applies to distinct prefixes across both maps, not row count.
        counts.apply_insert(&first, None, &present);
        counts.mark_synchronized(8);
        assert_eq!(
            counts.exact_count(8, IndexKeyKind::User, id, &components),
            Some(2)
        );
        counts.apply_remove(&first, Some(&present));
        counts.mark_synchronized(9);
        assert_eq!(
            counts.exact_count(9, IndexKeyKind::User, id, &components),
            Some(1)
        );

        counts.apply_insert(&key(rows).0, None, &present);
        counts.mark_synchronized(10);
        assert!(counts.first_component_counts.is_empty());
        assert!(counts.counts.is_empty());
        assert_eq!(
            counts.exact_count(10, IndexKeyKind::User, id, &components),
            None
        );

        // A later commit/fold stamp cannot resurrect a partial projection.
        counts.apply_remove(&first, Some(&present));
        counts.apply_insert(&first, None, &present);
        counts.mark_synchronized(11);
        assert_eq!(
            counts.exact_count(11, IndexKeyKind::User, id, &components),
            None
        );
        assert!(counts.first_component_counts.is_empty());
        assert!(counts.counts.is_empty());
    }
}

#[test]
fn prefix_transitions_preserve_counts_sync_and_overlay_contributions() {
    let composite = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Nat64(u64::MAX),
        ])
        .unwrap(),
    );
    let present = IndexEntryValue::presence();
    for kind in [IndexKeyKind::User, IndexKeyKind::System] {
        for generation in [0, 17] {
            let id = IndexId::new_with_generation(EntityTag::new(42), 7, generation);
            for primary in [PrimaryKeyComponent::Nat64(7).into(), composite] {
                for width in 0..=MAX_INDEX_FIELDS {
                    let components = (0..width)
                        .map(|slot| {
                            EncodedValue::try_new(&Value::Text(format!("a\0{slot}")))
                                .unwrap()
                                .into_bytes()
                        })
                        .collect::<Vec<_>>();
                    let raw = IndexKey::new_from_components_with_primary_key_value(
                        &id,
                        kind,
                        &components,
                        &primary,
                    )
                    .unwrap()
                    .to_raw()
                    .unwrap();
                    for previous in [None, Some(&present)] {
                        for next in [None, Some(&present)] {
                            let before = previous == Some(&present);
                            let after = next == Some(&present);
                            let counted = kind == IndexKeyKind::User && width > 0;
                            let mut counts = IndexPrefixCardinality::synchronized_empty();
                            if let Some(value) = previous {
                                counts.apply_insert(&raw, None, value);
                            }
                            counts.mark_synchronized(7);
                            if let Some(value) = next {
                                counts.apply_insert(&raw, previous, value);
                            } else {
                                counts.apply_remove(&raw, previous);
                            }
                            assert!(counts.decodable);
                            assert_eq!(
                                counts.data_generation,
                                (!(counted && before != after)).then_some(7)
                            );
                            counts.mark_synchronized(8);
                            let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
                            overlay.apply_transition(&raw, previous, next);
                            assert!(overlay.decodable);
                            for length in 1..=width {
                                let expected = u64::from(kind == IndexKeyKind::User && after);
                                assert_eq!(
                                    counts.exact_count(8, kind, id, &components[..length]),
                                    Some(expected)
                                );
                                let expected_delta = if counted {
                                    i64::from(after) - i64::from(before)
                                } else {
                                    0
                                };
                                assert_eq!(
                                    overlay.exact_delta(kind, id, &components[..length]),
                                    Some(expected_delta)
                                );
                            }
                            assert_eq!(
                                counts.first_component_counts.len(),
                                usize::from(counted && after)
                            );
                            assert_eq!(
                                counts.counts.len(),
                                if counted && after { width - 1 } else { 0 }
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn first_component_count_updates_transfer_owned_component_backing() {
    let id = IndexId::new(EntityTag::new(42), 7);
    for signed in [false, true] {
        let component = vec![7; 32];
        let pointer = component.as_ptr();
        let prefix = IndexPrefixCardinalityKey {
            key_kind: IndexKeyKind::User,
            index_id: id,
            components: vec![component],
        };
        if signed {
            let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
            overlay.apply_prefixes(vec![prefix], 1);
            assert_eq!(
                overlay
                    .first_component_counts
                    .first_key_value()
                    .unwrap()
                    .0
                    .component
                    .as_ptr(),
                pointer
            );
        } else {
            let mut counts = IndexPrefixCardinality::synchronized_empty();
            counts.apply_delta(vec![prefix], PrefixCardinalityDelta::Increment);
            assert_eq!(
                counts
                    .first_component_counts
                    .first_key_value()
                    .unwrap()
                    .0
                    .component
                    .as_ptr(),
                pointer
            );
        }
    }
}

#[test]
fn prefix_changes_preserve_corruption_and_ignored_namespace_semantics() {
    let primary = PrimaryKeyComponent::Nat64(7).into();
    let id = IndexId::new(EntityTag::new(42), 7);
    let present = IndexEntryValue::presence();
    for kind in [IndexKeyKind::User, IndexKeyKind::System] {
        let raw =
            IndexKey::new_from_components_with_primary_key_value(&id, kind, &[vec![7]], &primary)
                .unwrap()
                .to_raw()
                .unwrap();
        let mut truncated = raw.as_bytes().to_vec();
        truncated.pop();
        let mut trailing = raw.as_bytes().to_vec();
        trailing.push(0);
        let mut bad_primary = raw.as_bytes().to_vec();
        let offset = bad_primary.len()
            - IndexKey::compact_primary_key_value_bytes(&primary)
                .unwrap()
                .len();
        bad_primary[offset] = 0xff;
        let user = kind == IndexKeyKind::User;
        let cases = [
            (raw.clone(), present.clone(), false),
            (
                RawIndexStoreKey::from_persisted_bytes(truncated),
                present.clone(),
                true,
            ),
            (
                RawIndexStoreKey::from_persisted_bytes(trailing),
                present.clone(),
                true,
            ),
            (
                RawIndexStoreKey::from_persisted_bytes(bad_primary),
                present.clone(),
                user,
            ),
            (
                raw.clone(),
                IndexEntryValue::from_persisted_bytes(vec![]),
                user,
            ),
            (
                raw.clone(),
                IndexEntryValue::from_persisted_bytes(vec![1]),
                user,
            ),
            (
                raw.clone(),
                IndexEntryValue::from_persisted_bytes(vec![9]),
                user,
            ),
            (
                raw.clone(),
                IndexEntryValue::from_persisted_bytes(vec![0, 0]),
                user,
            ),
        ];
        for (observed_key, next, invalid) in cases {
            let mut counts = IndexPrefixCardinality::synchronized_empty();
            counts.apply_insert(&raw, None, &present);
            counts.mark_synchronized(7);
            // Even equal malformed user entries must invalidate the absolute
            // projection; equal valid entries retain its synchronization.
            counts.apply_insert(&observed_key, Some(&next), &next);
            let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
            overlay.apply_transition(&observed_key, None, Some(&next));
            assert_eq!(counts.decodable, !invalid);
            assert_eq!(overlay.decodable, !invalid);
            if invalid {
                assert_eq!(counts.data_generation, None);
                assert!(counts.first_component_counts.is_empty());
                assert!(counts.counts.is_empty());
                assert!(overlay.first_component_counts.is_empty());
                assert!(overlay.counts.is_empty());
            } else {
                assert_eq!(counts.data_generation, Some(7));
                assert_eq!(counts.first_component_counts.len(), usize::from(user));
                assert_eq!(overlay.first_component_counts.len(), usize::from(user));
            }
        }
    }
}

#[test]
fn signed_count_deltas_cancel_and_preserve_counts_on_overflow() {
    for delta in [0, 1, -1] {
        let mut counts = BTreeMap::new();
        assert!(apply_signed_count_delta(&mut counts, 7, delta));
        assert!(apply_signed_count_delta(&mut counts, 7, delta));
        assert_eq!(counts.get(&7).copied(), (delta != 0).then_some(2 * delta));
        assert!(apply_signed_count_delta(&mut counts, 7, -2 * delta));
        assert!(counts.is_empty());
    }

    for (initial, delta) in [(i64::MAX, 1), (i64::MIN, -1)] {
        let mut counts = BTreeMap::new();
        assert!(apply_signed_count_delta(&mut counts, 7, initial));
        assert!(!apply_signed_count_delta(&mut counts, 7, delta));
        assert_eq!(counts.get(&7), Some(&initial));
    }
}

#[test]
fn user_prefix_lookup_key_preserves_physical_generation_and_encoded_components() {
    let entity_tag = EntityTag::new(0xCA7D);
    let components = vec![b"collection-a".to_vec(), b"Draft".to_vec()];
    let current_index = IndexId::new_with_generation(entity_tag, 2, 7);
    let next_index = IndexId::new_with_generation(entity_tag, 2, 8);

    let current = UserIndexPrefixCardinalityKey::new(current_index, components.clone());
    let next = UserIndexPrefixCardinalityKey::new(next_index, components.clone());

    assert_eq!(current.index_id(), current_index);
    assert_eq!(current.prefix_components(), components.as_slice());
    assert_ne!(current, next);
}

#[test]
fn impossible_zero_multiplicity_is_a_store_invariant_failure() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let mut cardinality = IndexPrefixCardinality::synchronized_empty();
    cardinality.first_component_counts.insert(
        IndexPrefixCardinalityFirstKey {
            key_kind: IndexKeyKind::User,
            index_id,
            component: b"invalid-zero".to_vec(),
        },
        0,
    );

    assert!(
        cardinality
            .exact_first_component_range_count(
                0,
                index_id,
                &Bound::Unbounded,
                &Bound::Unbounded,
                2,
            )
            .is_err(),
    );
}

#[test]
fn impossible_metadata_count_overflow_is_a_store_invariant_failure() {
    assert!(checked_metadata_count_increment(u64::MAX).is_err());
}

#[test]
fn first_component_range_cardinality_preserves_bounds_and_work_cap() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let mut cardinality = IndexPrefixCardinality::synchronized_empty();
    for (component, multiplicity) in [(b"a".as_slice(), 1), (b"b", 2), (b"c", 3)] {
        cardinality.first_component_counts.insert(
            IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, component),
            multiplicity,
        );
    }
    cardinality.mark_synchronized(7);
    let lower = Bound::Included(b"b".to_vec());
    let upper = Bound::Excluded(b"d".to_vec());

    assert_eq!(
        cardinality
            .exact_first_component_range_count(7, index_id, &lower, &upper, 3)
            .expect("synchronized range metadata should be valid"),
        Some((5, 2, true)),
    );
    assert_eq!(
        cardinality
            .exact_first_component_range_count(7, index_id, &lower, &upper, 1)
            .expect("bounded range metadata should be valid"),
        Some((2, 1, false)),
    );
    assert_eq!(
        cardinality
            .exact_first_component_range_count(8, index_id, &lower, &upper, 3)
            .expect("stale range metadata should fail closed"),
        None,
    );

    let empty_lower = Bound::Excluded(b"z".to_vec());
    assert_eq!(
        cardinality
            .exact_first_component_range_count(7, index_id, &empty_lower, &Bound::Unbounded, 1,)
            .expect("empty synchronized range should be valid"),
        Some((0, 0, true)),
    );
}

#[test]
fn first_component_numeric_fold_is_exact_bounded_and_generation_matched() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let mut cardinality = IndexPrefixCardinality::synchronized_empty();
    for (value, multiplicity) in [
        (i64::from(i32::MIN), 1_u64),
        (-7, 2),
        (0, 3),
        (11, 4),
        (i64::from(i32::MAX), 1),
    ] {
        let component = EncodedValue::try_new(&Value::Int64(value))
            .expect("test Int64 component should encode")
            .into_bytes();
        cardinality.first_component_counts.insert(
            IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, &component),
            multiplicity,
        );
    }
    cardinality.mark_synchronized(9);

    let exact = cardinality
        .exact_first_component_numeric_fold(9, index_id, 6)
        .expect("synchronized numeric metadata should be valid")
        .expect("synchronized numeric metadata should be available");
    assert_eq!(exact, (11, 29, 5, true));

    let bounded = cardinality
        .exact_first_component_numeric_fold(9, index_id, 2)
        .expect("bounded numeric metadata should be valid")
        .expect("bounded numeric metadata should be available");
    assert_eq!(bounded, (3, -2_147_483_662, 2, false));
    assert!(
        cardinality
            .exact_first_component_numeric_fold(10, index_id, 6)
            .expect("stale numeric metadata should fail closed")
            .is_none(),
    );
}

#[test]
fn first_component_numeric_fold_rejects_impossible_metadata() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    for value in [
        Value::Text("wrong-kind".to_string()),
        Value::Int64(i64::MAX),
    ] {
        let component = EncodedValue::try_new(&value)
            .expect("test component should encode")
            .into_bytes();
        let mut cardinality = IndexPrefixCardinality::synchronized_empty();
        cardinality.first_component_counts.insert(
            IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, &component),
            1,
        );

        assert!(
            cardinality
                .exact_first_component_numeric_fold(0, index_id, 1)
                .is_err(),
        );
    }
}
