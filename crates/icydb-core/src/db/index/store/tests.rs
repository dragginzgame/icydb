use super::*;
use crate::{
    db::{
        direction::Direction,
        index::{IndexId, IndexKey, IndexKeyKind},
        journal::JournalSequence,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        positioned_overlay::{JournalOverlayPosition, PositionedOverlayRetirement},
        registry::StoreAllocationIdentity,
    },
    testing::test_memory,
    types::EntityTag,
};
use ic_storage::adapter::Storable;
use std::borrow::Cow;

#[test]
fn retained_store_corruption_does_not_initialize_a_replacement() {
    crate::testing::assert_retained_store_header_preserved(IndexStore::init_journaled);
}

fn raw_key(value: u8) -> RawIndexStoreKey {
    <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(vec![value]))
        .expect("storage operation should succeed")
}

fn overlay_position(sequence: u64) -> JournalOverlayPosition {
    JournalOverlayPosition::new(
        StoreAllocationIdentity::new(231, "test::index"),
        JournalSequence::new(sequence),
    )
}

fn indexed_raw_key(
    index_id: &IndexId,
    components: Vec<Vec<u8>>,
    primary_key: u64,
) -> RawIndexStoreKey {
    indexed_raw_key_with_kind(index_id, IndexKeyKind::User, components, primary_key)
}

fn indexed_raw_key_with_kind(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    components: Vec<Vec<u8>>,
    primary_key: u64,
) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        index_id,
        key_kind,
        components.as_slice(),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(primary_key)),
    )
    .expect("test index key should build")
    .to_raw()
    .expect("test index key should encode")
}

fn malformed_index_entry_value() -> IndexEntryValue {
    <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![0xFF]))
        .expect("storage operation should succeed")
}

fn invalid_index_entry_value() -> IndexEntryValue {
    <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![1]))
        .expect("storage operation should succeed")
}

#[test]
fn index_rows_survive_optional_prefix_metadata_eviction() {
    let id = IndexId::new(EntityTag::new(42), 7);
    for mut store in [
        IndexStore::init_heap(),
        IndexStore::init_journaled(test_memory(94)).expect("fixture storage should initialize"),
    ] {
        for value in 0_u64..5_000 {
            let key = indexed_raw_key(&id, vec![value.to_be_bytes().to_vec()], value);
            store.insert(key, IndexEntryValue::presence());
        }
        store.mark_prefix_cardinality_data_generation(7);
        let components = vec![0_u64.to_be_bytes().to_vec()];
        assert_eq!(
            store.exact_prefix_cardinality(7, IndexKeyKind::User, id, &components),
            None
        );
        for value in [0_u64, 4_095, 4_096, 4_999] {
            let key = indexed_raw_key(&id, vec![value.to_be_bytes().to_vec()], value);
            assert_eq!((store.get(&key)), Some(IndexEntryValue::presence()));
        }
    }
}

#[test]
fn index_insert_paths_preserve_previous_values_and_prefix_counts() {
    let id = IndexId::new(EntityTag::new(42), 7);
    let components = vec![b"collection".to_vec(), b"draft".to_vec()];
    let key = indexed_raw_key(&id, components.clone(), 1);
    for mut store in [
        IndexStore::init_heap(),
        IndexStore::init_journaled(test_memory(94)).expect("fixture storage should initialize"),
    ] {
        let journaled = matches!(store.backend, IndexStoreBackend::Journaled { .. });
        let mut previous = None;
        for next in [
            Some(IndexEntryValue::presence()),
            Some(IndexEntryValue::presence()),
            Some(IndexEntryValue::presence()),
            None,
            None,
        ] {
            let returned = if let Some(value) = next.as_ref() {
                store.insert(key.clone(), value.clone())
            } else {
                store.remove(&key)
            };
            assert_eq!((returned), previous);
            assert_eq!((store.get(&key)), next);
            let count = u64::from(next == Some(IndexEntryValue::presence()));
            store.mark_prefix_cardinality_data_generation(7);
            assert_eq!(
                store.exact_prefix_cardinality(7, IndexKeyKind::User, id, &components),
                Some(count)
            );
            assert_eq!(
                store.exact_prefix_cardinality_delta(IndexKeyKind::User, id, &components),
                Some(if journaled {
                    i64::try_from(count).unwrap()
                } else {
                    0
                })
            );
            previous = next;
        }
        store.insert_preflighted_absent(key.clone(), IndexEntryValue::presence());
        store.mark_prefix_cardinality_data_generation(8);
        assert_eq!(
            store.exact_prefix_cardinality(8, IndexKeyKind::User, id, &components),
            Some(1)
        );
        if journaled {
            for (sequence, next) in [Some(IndexEntryValue::presence()), None]
                .into_iter()
                .enumerate()
            {
                store
                    .publish_positioned_journal_entry(
                        key.clone(),
                        next.clone(),
                        overlay_position(sequence as u64 + 1),
                    )
                    .unwrap();
                assert_eq!((store.get(&key)), next);
                let count = u64::from(next == Some(IndexEntryValue::presence()));
                store.mark_prefix_cardinality_data_generation(9);
                assert_eq!(
                    store.exact_prefix_cardinality(9, IndexKeyKind::User, id, &components),
                    Some(count)
                );
                assert_eq!(
                    store.exact_prefix_cardinality_delta(IndexKeyKind::User, id, &components),
                    Some(i64::try_from(count).unwrap())
                );
            }
        }
    }
}

#[test]
fn index_prefix_cardinality_requires_explicit_data_generation_sync() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let draft = b"Draft".to_vec();
    let review = b"Review".to_vec();
    let mut store = IndexStore::init_heap();

    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 2),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), review.clone()], 3),
        IndexEntryValue::presence(),
    );

    assert_eq!(
        store.exact_prefix_cardinality(
            0,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        None,
        "raw index mutations must not be trusted until row generation sync is stamped",
    );

    store.mark_prefix_cardinality_data_generation(7);

    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(3),
    );
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            index_id,
            &[collection.clone(), draft],
        ),
        Some(2),
    );
    assert_eq!(
        store.exact_prefix_cardinality(8, IndexKeyKind::User, index_id, &[collection, review],),
        None,
        "row generation drift should force the caller to use the existing-row fallback",
    );
}

#[test]
fn first_component_range_cardinality_is_exact_bounded_and_generation_matched() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let alpha = b"alpha".to_vec();
    let beta = b"beta".to_vec();
    let alpha_one = indexed_raw_key(&index_id, vec![alpha.clone()], 1);
    let alpha_two = indexed_raw_key(&index_id, vec![alpha], 2);
    let beta_one = indexed_raw_key(&index_id, vec![beta], 3);
    let mut store = IndexStore::init_heap();
    let bound = Bound::Unbounded;

    assert_eq!(
        store
            .exact_first_component_range_cardinality(0, index_id, &bound, &bound, 1)
            .expect("initialized metadata should be structurally valid"),
        Some((0, 0, true)),
        "initialized empty metadata must positively prove exact zero",
    );
    store.insert(alpha_one.clone(), IndexEntryValue::presence());
    store.insert(alpha_two.clone(), IndexEntryValue::presence());
    store.insert(beta_one.clone(), IndexEntryValue::presence());
    assert_eq!(
        store
            .exact_first_component_range_cardinality(0, index_id, &bound, &bound, 3)
            .expect("invalidated metadata should remain structurally valid"),
        None,
        "an unstamped mutation must make optional metadata unavailable",
    );

    store.mark_prefix_cardinality_data_generation(7);
    assert_eq!(
        store
            .exact_first_component_range_cardinality(7, index_id, &bound, &bound, 3)
            .expect("synchronized metadata should be structurally valid"),
        Some((3, 2, true)),
        "duplicate physical entries contribute multiplicity but one leading-component visit",
    );
    assert_eq!(
        store
            .exact_first_component_range_cardinality(7, index_id, &bound, &bound, 1)
            .expect("bounded metadata should be structurally valid"),
        Some((2, 1, false)),
        "stop-after bounds leading-component visits while retaining their multiplicities",
    );
    assert_eq!(
        store
            .exact_first_component_range_cardinality(8, index_id, &bound, &bound, 3)
            .expect("stale metadata should remain structurally valid"),
        None,
        "row-generation drift must fail closed",
    );

    store.remove(&alpha_one);
    store.remove(&alpha_two);
    store.remove(&beta_one);
    store.mark_prefix_cardinality_data_generation(8);
    assert_eq!(
        store
            .exact_first_component_range_cardinality(8, index_id, &bound, &bound, 1)
            .expect("empty metadata should be structurally valid"),
        Some((0, 0, true)),
        "deleting every value must restore a positive exact-zero proof",
    );
}

#[test]
fn index_prefix_cardinality_seeks_parents_and_charges_examined_metadata() {
    use crate::db::resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        current_execution_budget_usage, with_execution_budget_for_tests,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    };

    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let parent = vec![vec![128]];
    let mut store = IndexStore::init_heap();
    for prefix in 0..=255_u8 {
        for child in 0..2_u8 {
            store.insert(
                indexed_raw_key(
                    &index_id,
                    vec![vec![prefix], vec![child], vec![0]],
                    u64::from(prefix) * 2 + u64::from(child),
                ),
                IndexEntryValue::presence(),
            );
        }
    }
    store.mark_prefix_cardinality_data_generation(7);

    for limit in [5, 4] {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            limit,
        );
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0,
        );
        let result = with_execution_budget_for_tests(
            budget,
            context,
            || {
                let children = store.exact_child_prefixes_for_parent_set(
                    7,
                    IndexKeyKind::User,
                    index_id,
                    [parent.as_slice(), parent.as_slice()],
                    2,
                )?;
                assert_eq!(
                    children,
                    Some(vec![vec![vec![128], vec![0]], vec![vec![128], vec![1]]])
                );
                // Two children, their deeper descendants and one range boundary;
                // unrelated prefixes and duplicate parents do not multiply work.
                assert_eq!(
                    current_execution_budget_usage()?
                        .observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
                    5
                );
                Ok::<_, InternalError>(())
            },
            std::convert::identity,
        );
        assert_eq!(result.is_ok(), limit == 5);
        if let Err(error) = result {
            assert!(matches!(
                error.diagnostic().detail(),
                Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
                })
            ));
        }
    }
}

#[test]
fn index_prefix_cardinality_enumerates_bounded_child_prefixes() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let other_collection = b"collection-b".to_vec();
    let draft = b"Draft".to_vec();
    let review = b"Review".to_vec();
    let published = b"Published".to_vec();
    let mut store = IndexStore::init_heap();

    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 2),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), review.clone()], 3),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(
            &index_id,
            vec![other_collection.clone(), published.clone()],
            4,
        ),
        IndexEntryValue::presence(),
    );
    store.mark_prefix_cardinality_data_generation(7);

    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            )
            .unwrap(),
        Some(vec![
            vec![collection.clone(), draft],
            vec![collection.clone(), review],
        ]),
        "child-prefix enumeration should return deterministic unique children under the requested parent",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&other_collection)],
                4,
            )
            .unwrap(),
        Some(vec![vec![other_collection, published]]),
        "child-prefix enumeration must stay scoped to the requested parent prefix",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                8,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            )
            .unwrap(),
        None,
        "row generation drift should keep child-prefix expansion fail-closed",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                1,
            )
            .unwrap(),
        None,
        "over-cap child-prefix expansion should fall back to the existing route",
    );
}

#[test]
fn index_prefix_cardinality_batches_sparse_child_prefixes() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let other_collection = b"collection-b".to_vec();
    let missing_a = b"missing-a".to_vec();
    let missing_b = b"missing-b".to_vec();
    let draft = b"Draft".to_vec();
    let review = b"Review".to_vec();
    let published = b"Published".to_vec();
    let mut store = IndexStore::init_heap();

    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), review.clone()], 2),
        IndexEntryValue::presence(),
    );
    store.insert(
        indexed_raw_key(
            &index_id,
            vec![other_collection.clone(), published.clone()],
            3,
        ),
        IndexEntryValue::presence(),
    );
    store.mark_prefix_cardinality_data_generation(7);

    let parents = [
        std::slice::from_ref(&missing_a),
        std::slice::from_ref(&collection),
        std::slice::from_ref(&missing_b),
        std::slice::from_ref(&other_collection),
        std::slice::from_ref(&collection),
    ];
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(7, IndexKeyKind::User, index_id, parents, 3,)
            .unwrap(),
        Some(vec![
            vec![collection.clone(), draft],
            vec![collection.clone(), review],
            vec![other_collection.clone(), published],
        ]),
        "batched child-prefix enumeration should skip missing sparse parents and return deterministic real children",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [
                    std::slice::from_ref(&missing_a),
                    std::slice::from_ref(&missing_b)
                ],
                4,
            )
            .unwrap(),
        Some(Vec::new()),
        "missing-only sparse parent sets should be proven empty when cardinality is synchronized",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                7,
                IndexKeyKind::User,
                index_id,
                [
                    std::slice::from_ref(&collection),
                    std::slice::from_ref(&other_collection)
                ],
                2,
            )
            .unwrap(),
        None,
        "over-cap sparse parent-set expansion should fail closed",
    );
    assert_eq!(
        store
            .exact_child_prefixes_for_parent_set(
                8,
                IndexKeyKind::User,
                index_id,
                [std::slice::from_ref(&collection)],
                4,
            )
            .unwrap(),
        None,
        "generation drift should keep batched child-prefix expansion fail-closed",
    );
}

#[test]
fn index_prefix_cardinality_ignores_system_index_mutations() {
    let user_index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let system_index_id = IndexId::new(EntityTag::new(0xCA7D), 2);
    let collection = b"collection-a".to_vec();
    let draft = b"Draft".to_vec();
    let system_component = b"reverse-edge".to_vec();
    let mut store = IndexStore::init_heap();

    store.insert(
        indexed_raw_key(&user_index_id, vec![collection.clone(), draft.clone()], 1),
        IndexEntryValue::presence(),
    );
    store.mark_prefix_cardinality_data_generation(7);

    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            user_index_id,
            &[collection.clone(), draft.clone()],
        ),
        Some(1),
    );

    let system_key = indexed_raw_key_with_kind(
        &system_index_id,
        IndexKeyKind::System,
        vec![system_component],
        1,
    );
    store.insert(system_key.clone(), IndexEntryValue::presence());
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            user_index_id,
            &[collection.clone(), draft.clone()],
        ),
        Some(1),
        "system index writes must not invalidate synchronized user-prefix cardinality",
    );

    store.remove(&system_key);
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            user_index_id,
            &[collection.clone(), draft.clone()],
        ),
        Some(1),
        "system index removals must not invalidate synchronized user-prefix cardinality",
    );

    let malformed_system_key = indexed_raw_key_with_kind(
        &system_index_id,
        IndexKeyKind::System,
        vec![b"malformed-reverse-edge".to_vec()],
        2,
    );
    store.insert(malformed_system_key.clone(), malformed_index_entry_value());
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            user_index_id,
            &[collection.clone(), draft.clone()],
        ),
        Some(1),
        "malformed system index payloads must not invalidate user-prefix cardinality",
    );

    store.remove(&malformed_system_key);
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            user_index_id,
            &[collection.clone(), draft],
        ),
        Some(1),
        "malformed system index removals must not invalidate user-prefix cardinality",
    );

    let review = b"Review".to_vec();
    store.insert(
        indexed_raw_key(&user_index_id, vec![collection.clone(), review.clone()], 2),
        IndexEntryValue::presence(),
    );
    assert_eq!(
        store.exact_prefix_cardinality(7, IndexKeyKind::User, user_index_id, &[collection, review]),
        None,
        "user-prefix count changes must still require a fresh row-generation stamp",
    );
}

#[test]
fn index_prefix_cardinality_refuses_invalid_user_index_mutations() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let draft = b"Draft".to_vec();
    let mut store = IndexStore::init_heap();

    store.insert(
        indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 1),
        IndexEntryValue::presence(),
    );
    store.mark_prefix_cardinality_data_generation(7);

    let stale_key = indexed_raw_key(&index_id, vec![collection.clone(), draft.clone()], 2);
    store.insert(stale_key.clone(), invalid_index_entry_value());
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            index_id,
            &[collection.clone(), draft.clone()],
        ),
        None,
        "invalid user index entries invalidate synchronized prefix cardinality",
    );

    store.remove(&stale_key);
    assert_eq!(
        store.exact_prefix_cardinality(7, IndexKeyKind::User, index_id, &[collection, draft],),
        None,
        "removing invalid bytes cannot restore a complete cardinality proof",
    );
}

#[test]
fn journaled_mixed_index_range_traversal_preserves_order_and_early_stop() {
    let mut store =
        IndexStore::init_journaled(test_memory(93)).expect("fixture storage should initialize");
    for value in [1_u8, 3, 5] {
        store
            .apply_canonical_entry(raw_key(value), Some(IndexEntryValue::presence()))
            .expect("canonical index seed should apply");
    }

    store.insert(raw_key(0), IndexEntryValue::presence());
    store.insert(raw_key(4), IndexEntryValue::presence());
    store.insert(raw_key(5), IndexEntryValue::presence());
    store.remove(&raw_key(1));

    let lower = Bound::Included(raw_key(0));
    let upper = Bound::Included(raw_key(5));

    let mut asc = Vec::new();
    store
        .visit_journaled_entries_in_range(
            (lower.as_ref(), upper.as_ref()),
            Direction::Asc,
            &mut |key, _value| {
                asc.push(key.as_bytes()[0]);
                Ok::<_, InternalError>(asc.len() == 2)
            },
        )
        .expect("asc journaled index range traversal should succeed");
    assert_eq!(asc, vec![0, 3]);

    let mut desc = Vec::new();
    store
        .visit_journaled_entries_in_range(
            (lower.as_ref(), upper.as_ref()),
            Direction::Desc,
            &mut |key, _value| {
                desc.push(key.as_bytes()[0]);
                Ok::<_, InternalError>(desc.len() == 2)
            },
        )
        .expect("desc journaled index range traversal should succeed");
    assert_eq!(desc, vec![5, 4]);
}

#[test]
fn journaled_index_range_preserves_typed_failure_and_stops_immediately() {
    // Exercise canonical-only, live-only and mixed/tombstoned fast paths.
    for backend in 0..3 {
        let mut store =
            IndexStore::init_journaled(test_memory(93)).expect("fixture storage should initialize");
        if backend != 1 {
            for value in [1, 3, 5] {
                store
                    .apply_canonical_entry(raw_key(value), Some(IndexEntryValue::presence()))
                    .expect("canonical seed should apply");
            }
        }
        if backend != 0 {
            for value in [0, 4, 5] {
                store.insert(raw_key(value), IndexEntryValue::presence());
            }
            store.remove(&raw_key(1));
        }
        let lower = Bound::Included(raw_key(0));
        let upper = Bound::Included(raw_key(5));
        for direction in [Direction::Asc, Direction::Desc] {
            let mut visited = Vec::new();
            let result = store.visit_journaled_entries_in_range(
                (lower.as_ref(), upper.as_ref()),
                direction,
                &mut |key, _| {
                    visited.push(key.as_bytes()[0]);
                    Err::<bool, crate::testing::VisitorTestError>(
                        crate::testing::VisitorTestError::Callback(7),
                    )
                },
            );
            assert_eq!(result, Err(crate::testing::VisitorTestError::Callback(7)));
            let first = match direction {
                Direction::Asc if backend == 0 => 1,
                Direction::Asc => 0,
                Direction::Desc => 5,
            };
            assert_eq!(visited, vec![first]);
        }
    }
}

#[test]
fn journaled_index_store_reopens_without_materializing_prefix_cardinality() {
    let memory = crate::testing::test_memory_owner(94);
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let mut store =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    let key = indexed_raw_key(&index_id, vec![collection.clone()], 1);
    store
        .apply_canonical_entry(key.clone(), Some(IndexEntryValue::presence()))
        .expect("canonical index seed should apply");
    drop(store);

    let mut reopened =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    let bound = Bound::Unbounded;

    assert_eq!((reopened.get(&key)), Some(IndexEntryValue::presence()));
    assert_eq!(
        reopened.exact_prefix_cardinality(
            0,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        None,
        "startup must leave optional prefix cardinality unavailable without scanning stable entries",
    );
    assert_eq!(
        reopened
            .exact_first_component_range_cardinality(0, index_id, &bound, &bound, 2)
            .expect("unmaterialized metadata should remain structurally valid"),
        None,
        "startup must not treat an absent materialized leading-component map as exact evidence",
    );
    assert_eq!(
        reopened.exact_prefix_cardinality_delta(
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
    );
    let second = indexed_raw_key(&index_id, vec![collection.clone()], 2);
    reopened.insert(second.clone(), IndexEntryValue::presence());
    assert_eq!(
        reopened.exact_prefix_cardinality_delta(
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(1),
    );
    reopened
        .apply_canonical_entry(second, Some(IndexEntryValue::presence()))
        .expect("matching canonical index entry should fold");
    assert_eq!(
        reopened.exact_prefix_cardinality_delta(
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
        "canonical fold must consume only its exact overlay prefix contribution",
    );
}

#[test]
fn journaled_reset_preserves_only_live_source_system_entries() {
    let memory = crate::testing::test_memory_owner(97);
    let mut store =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    let live_source = EntityTag::new(101);
    let durable_source = EntityTag::new(102);
    let key = |source, kind, pk| {
        indexed_raw_key_with_kind(&IndexId::new(source, u16::MAX), kind, vec![vec![1]], pk)
    };
    let retained = key(live_source, IndexKeyKind::System, 1);
    let removed = key(live_source, IndexKeyKind::System, 2);
    let user = key(live_source, IndexKeyKind::User, 3);
    let durable = key(durable_source, IndexKeyKind::System, 4);
    for key in [&retained, &removed, &user, &durable] {
        store.insert(key.clone(), IndexEntryValue::presence());
    }
    store.remove(&removed);
    let owners = BTreeSet::from([live_source]);
    for _ in 0..2 {
        store
            .reset_journaled_live_projection(0, FoldWatermark::initial(), &owners)
            .unwrap();
        assert!(store.get(&retained).is_some());
        for key in [&removed, &user, &durable] {
            assert!(store.get(key).is_none());
        }
    }
    drop(store);
    let reopened =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    assert!(
        reopened.get(&retained).is_none(),
        "live references must not become durable"
    );
}

#[test]
fn journaled_reset_refuses_unclassifiable_live_keys_before_removal() {
    let mut store =
        IndexStore::init_journaled(test_memory(97)).expect("fixture storage should initialize");
    let source = EntityTag::new(101);
    let valid = indexed_raw_key_with_kind(
        &IndexId::new(source, u16::MAX),
        IndexKeyKind::System,
        vec![vec![1]],
        1,
    );
    let invalid = RawIndexStoreKey::from_persisted_bytes(vec![0xff]);
    store.insert(valid.clone(), IndexEntryValue::presence());
    store.insert(invalid.clone(), IndexEntryValue::presence());
    let error = store
        .reset_journaled_live_projection(0, FoldWatermark::initial(), &BTreeSet::from([source]))
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::index_corruption().diagnostic_code()
    );
    assert!(store.get(&valid).is_some());
    assert!(store.get(&invalid).is_some());
}

#[test]
fn empty_journaled_index_store_retains_exact_prefix_cardinality_without_scanning() {
    let memory = crate::testing::test_memory_owner(95);
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let mut store =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");

    assert_eq!(
        store.exact_prefix_cardinality(
            0,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
    );
    store
        .reset_journaled_live_projection(
            7,
            FoldWatermark::initial(),
            &std::collections::BTreeSet::new(),
        )
        .expect("empty projection reset should succeed");
    assert_eq!(
        store.exact_prefix_cardinality(
            7,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
    );
    drop(store);

    let reopened =
        IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    assert_eq!(
        reopened.exact_prefix_cardinality(
            0,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
    );
}

#[test]
fn journaled_prefix_delta_binds_and_advances_its_canonical_watermark() {
    let mut store =
        IndexStore::init_journaled(test_memory(99)).expect("fixture storage should initialize");
    assert_eq!(store.exact_prefix_cardinality_delta_watermark(), None);

    let current = FoldWatermark::new(JournalSequence::new(7), 3);
    let next = FoldWatermark::new(JournalSequence::new(8), 4);
    store
        .reset_journaled_live_projection(0, current, &std::collections::BTreeSet::new())
        .expect("delta reset should bind the current canonical watermark");
    assert_eq!(
        store.exact_prefix_cardinality_delta_watermark(),
        Some(current)
    );
    store.apply_prefix_cardinality_delta_watermark(current, next);
    assert_eq!(store.exact_prefix_cardinality_delta_watermark(), Some(next));

    store.apply_prefix_cardinality_delta_watermark(current, next);
    assert_eq!(store.exact_prefix_cardinality_delta_watermark(), None);
    store.apply_prefix_cardinality_delta_watermark(next, next);
    assert_eq!(store.exact_prefix_cardinality_delta_watermark(), None);
}

#[test]
fn recovered_index_fold_maintains_available_prefix_cardinality() {
    let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
    let collection = b"collection-a".to_vec();
    let key = indexed_raw_key(&index_id, vec![collection.clone()], 1);
    let mut store =
        IndexStore::init_journaled(test_memory(96)).expect("fixture storage should initialize");

    store
        .apply_canonical_entry(key.clone(), Some(IndexEntryValue::presence()))
        .expect("recovered index put should fold");
    store.mark_prefix_cardinality_data_generation(1);
    assert_eq!(
        store.exact_prefix_cardinality(
            1,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(1),
    );

    store
        .apply_canonical_entry(key, None)
        .expect("recovered index delete should fold");
    store.mark_prefix_cardinality_data_generation(2);
    assert_eq!(
        store.exact_prefix_cardinality(
            2,
            IndexKeyKind::User,
            index_id,
            std::slice::from_ref(&collection),
        ),
        Some(0),
    );
}

#[test]
fn positioned_index_overlay_preserves_later_membership_until_exact_retirement() {
    let key = raw_key(7);
    let mut store =
        IndexStore::init_journaled(test_memory(97)).expect("fixture storage should initialize");
    store
        .apply_canonical_entry(key.clone(), Some(IndexEntryValue::presence()))
        .expect("canonical membership should seed");

    store
        .publish_positioned_journal_entry(key.clone(), None, overlay_position(1))
        .expect("positioned tombstone should publish");
    store
        .publish_positioned_journal_entry(
            key.clone(),
            Some(IndexEntryValue::presence()),
            overlay_position(2),
        )
        .expect("later membership should supersede the tombstone");
    let prepared = store
        .prepare_position_retirement([key.clone()], overlay_position(1))
        .expect("older retirement should preserve later membership");
    assert_eq!(
        prepared.entries,
        [(key.clone(), PositionedOverlayRetirement::Superseded)]
    );
    store
        .apply_canonical_entry(key.clone(), None)
        .expect("tombstone batch should become canonical");
    store.apply_prepared_position_retirement(prepared);
    assert_eq!((store.get(&key)), Some(IndexEntryValue::presence()));

    let prepared = store
        .prepare_position_retirement([key.clone()], overlay_position(2))
        .expect("latest retirement should be exact");
    assert_eq!(
        prepared.entries,
        [(key.clone(), PositionedOverlayRetirement::Exact)]
    );
    store
        .apply_canonical_entry(key.clone(), Some(IndexEntryValue::presence()))
        .expect("membership batch should become canonical");
    store.apply_prepared_position_retirement(prepared);
    assert_eq!((store.get(&key)), Some(IndexEntryValue::presence()));

    let mut visible = Vec::new();
    store
        .visit_entries(|visited_key, value| {
            visible.push((visited_key.clone(), value.clone()));
            Ok::<_, InternalError>(IndexStoreVisit::Continue)
        })
        .expect("positioned index should remain range-visible");
    assert_eq!(visible, vec![(key, IndexEntryValue::presence())]);
}

#[test]
fn positioned_index_overlay_coalesces_repeated_same_batch_target() {
    let key = raw_key(8);
    let position = overlay_position(3);
    let mut store =
        IndexStore::init_journaled(test_memory(98)).expect("fixture storage should initialize");

    store
        .publish_positioned_journal_entry(key.clone(), Some(IndexEntryValue::presence()), position)
        .expect("first same-batch effect should publish");
    store
        .publish_positioned_journal_entry(key.clone(), None, position)
        .expect("final same-batch effect should coalesce by logical target");
    let prepared = store
        .prepare_position_retirement([key.clone()], position)
        .expect("coalesced target should retire once");
    assert_eq!(
        prepared.entries,
        [(key.clone(), PositionedOverlayRetirement::Exact)]
    );
    store
        .apply_canonical_entry(key.clone(), None)
        .expect("coalesced final effect should become canonical");
    store.apply_prepared_position_retirement(prepared);
    assert!(store.get(&key).is_none());
}
