use super::*;
use crate::{
    db::{
        QueryError,
        data::{DataStore, RawRow},
        executor::{
            PageWorkEnvelope, begin_production_scalar_page_unit,
            finish_production_scalar_page_unit, with_production_scalar_page_work,
        },
        index::{IndexEntryValue, IndexId, IndexStore},
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreAllocationIdentities, StoreRuntimeStorageCapabilities},
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
        schema::SchemaStore,
        tests::support::index::nat64_index_key,
    },
    types::EntityTag,
};
use ic_storage::adapter::Storable;
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, DiagnosticFactTag,
    RuntimeBoundaryCode,
};
use std::{borrow::Cow, cell::RefCell};

const ENTITY: EntityTag = EntityTag::new(0x222);

thread_local! {
    static DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
}

const STORE: StoreHandle = StoreHandle::new(
    &DATA,
    &INDEX,
    &SCHEMA,
    StoreAllocationIdentities::absent(),
    StoreRuntimeStorageCapabilities::heap(),
);

fn data_key(value: u64) -> DecodedDataStoreKey {
    DecodedDataStoreKey::new(
        ENTITY,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
}

fn index_key(index_id: &IndexId, component: &[u8], value: u64) -> IndexKey {
    nat64_index_key(index_id, component, value)
}

fn reset_heap_stores() {
    DATA.with_borrow_mut(|store| *store = DataStore::init_heap());
    INDEX.with_borrow_mut(|store| *store = IndexStore::init_heap());
}

fn load_primary_keys() {
    DATA.with_borrow_mut(|store| {
        for value in 1..=100 {
            store.insert_raw_for_test(
                data_key(value)
                    .to_raw()
                    .expect("test data key should encode"),
                RawRow::try_new(vec![0]).expect("test row should be bounded"),
            );
        }
    });
}

fn load_index_keys(index_id: &IndexId, component: &[u8]) {
    INDEX.with_borrow_mut(|store| {
        for value in 1..=100 {
            store.insert(
                index_key(index_id, component, value)
                    .to_raw()
                    .expect("test index key should encode"),
                IndexEntryValue::presence(),
            );
        }
    });
}

fn ordered_index_stream(
    direction: Direction,
    limit: Option<usize>,
    anchor: Option<RawIndexStoreKey>,
) -> IndexRangeKeyStream {
    let index_id = IndexId::new(ENTITY, 1);
    IndexRangeKeyStream::new(
        STORE,
        ENTITY,
        (
            Bound::Included(index_key(&index_id, b"lane", 0).to_raw().unwrap()),
            Bound::Included(index_key(&index_id, b"lane", 101).to_raw().unwrap()),
        ),
        direction,
        anchor,
        limit,
        ACCESS_SCAN_CHUNK_ENTRIES,
    )
}

fn collect_keys(stream: &mut impl OrderedKeyStream) -> Vec<DecodedDataStoreKey> {
    let mut keys = Vec::new();
    while let Some(key) = stream.next_key().unwrap() {
        keys.push(key);
    }
    assert_eq!(stream.next_key().unwrap(), None);
    keys
}

#[test]
fn primary_boundaries_preserve_key_bytes_and_typed_refusals() {
    use crate::types::{Account, Principal, Subaccount};

    let account = Value::Account(Account::from_owner_and_subaccount(
        Principal::MAX,
        Some(Subaccount::MAX),
    ));
    let cases = [
        vec![],
        vec![Value::Unit],
        vec![account.clone()],
        vec![Value::Nat64(7), account.clone()],
        vec![account; 4],
        vec![Value::List(vec![Value::Nat64(1), Value::Nat64(2)])],
        vec![Value::Null],
        vec![Value::Text("unsupported".into())],
        vec![Value::Nat64(1), Value::Unit],
        vec![Value::List(Vec::new())],
        vec![Value::Null; 5],
    ];
    for values in cases {
        let boundary = CursorBoundary {
            slots: values
                .iter()
                .cloned()
                .map(CursorBoundarySlot::Present)
                .collect(),
        };
        let structural = match values.as_slice() {
            [value] => value.clone(),
            _ => Value::List(values),
        };
        let expected = DecodedDataStoreKey::try_from_structural_key(ENTITY, &structural);
        let actual = primary_key_boundary_data_key(ENTITY, &boundary);
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(actual, expected);
                assert_eq!(actual.to_raw().unwrap(), expected.to_raw().unwrap());
            }
            (Err(expected), Err(actual)) => {
                assert_eq!(actual.diagnostic(), expected.diagnostic());
                assert_eq!(actual.diagnostic_facts(), expected.diagnostic_facts());
            }
            results => panic!("boundary and structural key results differ: {results:?}"),
        }
        for position in 0..=boundary.slots.len() {
            let mut missing = boundary.clone();
            missing.slots.insert(position, CursorBoundarySlot::Missing);
            let error = primary_key_boundary_data_key(ENTITY, &missing).unwrap_err();
            assert_eq!(
                error.diagnostic(),
                InternalError::query_executor_invariant().diagnostic()
            );
        }
        let error = primary_key_boundary_value(&boundary, boundary.slots.len() + 1).unwrap_err();
        assert_eq!(
            error.diagnostic(),
            InternalError::query_executor_invariant().diagnostic()
        );
    }
}

#[test]
fn physical_polling_preserves_limits_across_chunk_boundaries() {
    reset_heap_stores();
    load_primary_keys();
    load_index_keys(&IndexId::new(ENTITY, 1), b"lane");
    for direction in [Direction::Asc, Direction::Desc] {
        for limit in [
            Some(0),
            Some(1),
            Some(63),
            Some(64),
            Some(65),
            Some(100),
            None,
        ] {
            let mut expected = (1..=100).map(data_key).collect::<Vec<_>>();
            if direction == Direction::Desc {
                expected.reverse();
            }
            if let Some(limit) = limit {
                expected.truncate(limit);
            }
            let mut primary =
                PrimaryRangeKeyStream::new_full_scan(STORE, ENTITY, direction, limit).unwrap();
            let mut index = ordered_index_stream(direction, limit, None);
            assert_eq!(collect_keys(&mut primary), expected);
            assert_eq!(collect_keys(&mut index), expected);
        }
    }
}

#[test]
fn direct_primary_visits_preserve_bounded_progress_after_early_stop() {
    for (composite, direction) in [
        (false, Direction::Asc),
        (false, Direction::Desc),
        (true, Direction::Asc),
        (true, Direction::Desc),
    ] {
        reset_heap_stores();
        let keys = (1..=100)
            .map(|value| {
                if composite {
                    DecodedDataStoreKey::new_primary_key_value(
                        ENTITY,
                        &PrimaryKeyValue::Composite(
                            CompositePrimaryKeyValue::try_from_components(&[
                                PrimaryKeyComponent::Nat64(value),
                                PrimaryKeyComponent::Nat64(500 + value),
                            ])
                            .unwrap(),
                        ),
                    )
                } else {
                    data_key(value)
                }
            })
            .collect::<Vec<_>>();
        DATA.with_borrow_mut(|store| {
            for key in &keys {
                store.insert_raw_for_test(key.to_raw().unwrap(), RawRow::try_new(vec![0]).unwrap());
            }
        });
        for limit in [Some(0), Some(1), Some(64), Some(65), None] {
            for stop_after in [0, 1, 63, 64, 65, 100] {
                for stop_in_visitor in [false, true] {
                    let mut stream = PrimaryRangeKeyStream::new(
                        STORE,
                        keys[9].clone(),
                        keys[89].clone(),
                        direction,
                        limit,
                    )
                    .unwrap();
                    let visited = Cell::new(0);
                    let mut rows = Vec::new();
                    assert_eq!(
                        stream
                            .try_visit_rows_direct(
                                &mut || Ok(visited.get() < stop_after),
                                &mut |key, _| {
                                    visited.set(visited.get() + 1);
                                    rows.push(key);
                                    Ok(if stop_in_visitor && visited.get() == stop_after {
                                        StoreVisit::Stop
                                    } else {
                                        StoreVisit::Continue
                                    })
                                },
                            )
                            .unwrap(),
                        Some(())
                    );
                    let mut expected = keys[9..90].to_vec();
                    if matches!(direction, Direction::Desc) {
                        expected.reverse();
                    }
                    if let Some(limit) = limit {
                        expected.truncate(limit);
                    }
                    assert_eq!(
                        rows,
                        expected
                            .iter()
                            .take(stop_after)
                            .cloned()
                            .collect::<Vec<_>>()
                    );
                    rows.extend(collect_keys(&mut stream));
                    assert_eq!(rows, expected);
                }
            }
        }
    }
}

#[test]
fn drained_primary_chunk_does_not_reopen_first_pull_admission() {
    reset_heap_stores();
    load_primary_keys();
    let mut stream =
        PrimaryRangeKeyStream::new_full_scan(STORE, ENTITY, Direction::Asc, None).unwrap();
    stream.chunk_entries = 1;
    assert_eq!(stream.next_key().unwrap(), Some(data_key(1)));
    assert!(stream.resume_strictly_after(&data_key(50)).is_err());
    let visits = Cell::new(0);
    assert_eq!(
        stream
            .try_visit_rows_direct(
                &mut || {
                    visits.set(visits.get() + 1);
                    Ok(true)
                },
                &mut |_, _| {
                    visits.set(visits.get() + 1);
                    Ok(StoreVisit::Continue)
                },
            )
            .unwrap(),
        None
    );
    assert_eq!(visits.get(), 0);
    assert_eq!(
        collect_keys(&mut stream),
        (2..=100).map(data_key).collect::<Vec<_>>()
    );
}

#[test]
fn primary_polling_preserves_malformed_key_rejection() {
    reset_heap_stores();
    let mut bytes = data_key(50).to_raw().unwrap().as_bytes().to_vec();
    bytes.pop();
    DATA.with_borrow_mut(|store| {
        store.insert_raw_for_test(
            RawDataStoreKey::from_persisted_bytes(bytes),
            RawRow::try_new(vec![0]).unwrap(),
        );
    });
    for direction in [Direction::Asc, Direction::Desc] {
        let mut stream =
            PrimaryRangeKeyStream::new_full_scan(STORE, ENTITY, direction, None).unwrap();
        let error = stream
            .next_key()
            .expect_err("malformed primary keys must fail before emission");
        assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
    }
}

#[test]
fn physical_polling_preserves_sparse_cursor_suffixes_and_limits() {
    // The oracle orders physical membership independently of chunk/refill
    // state, including cursor targets absent from the stored population.
    for seed in 1_u64..=8 {
        reset_heap_stores();
        let mut state = seed;
        let mut values = (0..48)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (state >> 32) % 100 + 1
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        values.dedup();
        let index_id = IndexId::new(ENTITY, 1);
        for &value in &values {
            DATA.with_borrow_mut(|store| {
                store.insert_raw_for_test(
                    data_key(value).to_raw().unwrap(),
                    RawRow::try_new(vec![0]).unwrap(),
                );
            });
            INDEX.with_borrow_mut(|store| {
                store.insert(
                    index_key(&index_id, b"lane", value).to_raw().unwrap(),
                    IndexEntryValue::presence(),
                );
            });
        }
        for direction in [Direction::Asc, Direction::Desc] {
            for limit in [None, Some(3)] {
                for boundary in [0, seed.wrapping_mul(17) % 100, 101] {
                    let mut expected = values
                        .iter()
                        .copied()
                        .filter(|value| match direction {
                            Direction::Asc => *value > boundary,
                            Direction::Desc => *value < boundary,
                        })
                        .map(data_key)
                        .collect::<Vec<_>>();
                    if direction == Direction::Desc {
                        expected.reverse();
                    }
                    if let Some(limit) = limit {
                        expected.truncate(limit);
                    }
                    let mut primary = PrimaryRangeKeyStream::new(
                        STORE,
                        data_key(1),
                        data_key(100),
                        direction,
                        limit,
                    )
                    .unwrap();
                    primary.resume_strictly_after(&data_key(boundary)).unwrap();
                    let anchor = index_key(&index_id, b"lane", boundary).to_raw().unwrap();
                    let mut index = ordered_index_stream(direction, limit, Some(anchor));
                    assert_eq!(collect_keys(&mut primary), expected);
                    assert_eq!(collect_keys(&mut index), expected);
                }
            }
        }
    }
}

#[test]
fn index_polling_preserves_duplicate_physical_occurrences() {
    reset_heap_stores();
    let index_id = IndexId::new(ENTITY, 1);
    for (component, value) in [(b"a", 1), (b"b", 1), (b"c", 2)] {
        INDEX.with_borrow_mut(|store| {
            store.insert(
                index_key(&index_id, component, value).to_raw().unwrap(),
                IndexEntryValue::presence(),
            );
        });
    }
    for direction in [Direction::Asc, Direction::Desc] {
        let mut stream = IndexRangeKeyStream::new(
            STORE,
            ENTITY,
            (
                Bound::Included(index_key(&index_id, b"a", 1).to_raw().unwrap()),
                Bound::Included(index_key(&index_id, b"c", 2).to_raw().unwrap()),
            ),
            direction,
            None,
            None,
            1,
        );
        let mut expected = vec![data_key(1), data_key(1), data_key(2)];
        if direction == Direction::Desc {
            expected.reverse();
        }
        assert_eq!(collect_keys(&mut stream), expected);
    }
}

#[test]
fn primary_polling_preserves_one_sided_bounds() {
    reset_heap_stores();
    load_primary_keys();
    for (start, end, direction, range) in [
        (Some(data_key(40)), None, Direction::Asc, 40..=100),
        (Some(data_key(40)), None, Direction::Desc, 40..=100),
        (None, Some(data_key(60)), Direction::Asc, 1..=60),
        (None, Some(data_key(60)), Direction::Desc, 1..=60),
    ] {
        let mut stream =
            PrimaryRangeKeyStream::new_bounds(STORE, ENTITY, start, end, direction, None).unwrap();
        let mut expected = range.map(data_key).collect::<Vec<_>>();
        if direction == Direction::Desc {
            expected.reverse();
        }
        assert_eq!(collect_keys(&mut stream), expected);
    }
}

#[test]
fn physical_polling_hard_budget_rejects_refill_before_progress() {
    reset_heap_stores();
    load_primary_keys();
    load_index_keys(&IndexId::new(ENTITY, 1), b"lane");
    for direction in [Direction::Asc, Direction::Desc] {
        let primary =
            PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), direction, None).unwrap();
        let index = ordered_index_stream(direction, None, None);
        for mut stream in [
            OrderedKeyStreamBox::primary_range(primary),
            OrderedKeyStreamBox::index_range(index),
        ] {
            let budget = HardExecutionBudget::uniform_for_tests(
                u64::MAX,
                HardExecutionFailureHeadroom::new(500, 256),
            )
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 0);
            let context = HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                0x706f_6c6c,
            );
            let error = with_query_execution_budget_for_tests(budget, context, || {
                stream.next_key().map_err(QueryError::execute)
            })
            .expect_err("physical refill must obey the hard budget");
            assert!(matches!(
                error.diagnostic().detail(),
                Some(DiagnosticDetail::RuntimeBoundary {
                    boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded
                })
            ));
            let first = if direction == Direction::Asc { 1 } else { 100 };
            assert_eq!(stream.next_key().unwrap(), Some(data_key(first)));
        }
    }
}

#[test]
fn materialized_primary_keys_resume_strictly_in_both_directions() {
    use crate::db::access::AccessPlan;
    for composite in [false, true] {
        let key_value = |key| {
            if composite {
                Value::List(vec![Value::Nat64(7), Value::Nat64(key)])
            } else {
                Value::Nat64(key)
            }
        };
        let keys = [3, 1, 2, 3].map(key_value);
        for path in [
            AccessPlan::by_key(keys[0].clone()),
            AccessPlan::by_keys(keys.to_vec()),
        ] {
            for direction in [Direction::Asc, Direction::Desc] {
                for boundary_value in [0, 2, 3, 4] {
                    let boundary = CursorBoundary {
                        slots: if composite {
                            vec![
                                CursorBoundarySlot::Present(Value::Nat64(7)),
                                CursorBoundarySlot::Present(Value::Nat64(boundary_value)),
                            ]
                        } else {
                            vec![CursorBoundarySlot::Present(Value::Nat64(boundary_value))]
                        },
                    };
                    let mut stream = resolve_structural_physical_key_stream(
                        &path.executable_contract(),
                        STORE,
                        ENTITY,
                        AccessStreamBindings::new(
                            AccessScanContinuationInput::with_primary_key_boundary(
                                None,
                                direction,
                                Some(&boundary),
                            ),
                        ),
                        AccessStreamExecutionPolicy::canonical_key_order(None),
                        None,
                    )
                    .unwrap();
                    let mut expected = (1..=3)
                        .filter(|key| !path.as_path().unwrap().is_by_key() || *key == 3)
                        .filter(|key| match direction {
                            Direction::Asc => *key > boundary_value,
                            Direction::Desc => *key < boundary_value,
                        })
                        .map(|key| {
                            DecodedDataStoreKey::try_from_structural_key(ENTITY, &key_value(key))
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    if direction == Direction::Desc {
                        expected.reverse();
                    }
                    for key in expected {
                        assert_eq!(stream.next_key().unwrap(), Some(key));
                    }
                    assert_eq!(stream.next_key().unwrap(), None);
                }
            }
        }
    }
}

#[test]
fn limited_primary_leaf_applies_page_fetch_hint_after_resume_boundary() {
    reset_heap_stores();
    load_primary_keys();

    for (direction, expected) in [
        (Direction::Asc, [51, 52, 53]),
        (Direction::Desc, [49, 48, 47]),
    ] {
        let mut stream =
            PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), direction, Some(3))
                .expect("limited primary stream should build");
        stream
            .resume_strictly_after(&data_key(50))
            .expect("primary continuation boundary should apply");

        for expected_key in expected {
            assert_eq!(
                stream.next_key().expect("resumed key should load"),
                Some(data_key(expected_key)),
            );
        }
        assert_eq!(
            stream.next_key().expect("fetch hint should end the page"),
            None,
        );
    }
}

#[test]
fn primary_leaf_caps_refills_to_the_active_page_entry_limit() {
    for (page_limit, expected_refill) in [(4, 4), (100, 64), (0, 1)] {
        let envelope = crate::db::executor::PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                page_limit,
            );
        let bounded = crate::db::executor::with_production_scalar_page_work(envelope, || {
            PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), Direction::Asc, None)
        })
        .expect("primary leaf should inherit the active page envelope");

        assert_eq!(bounded.value.next_pull_entry_bound(), expected_refill);
    }
}

#[test]
fn index_leaf_caps_refills_to_its_active_page_branch_share() {
    for (page_limit, branch_count, expected_refill) in
        [(4, 1, 4), (4, 2, 2), (100, 1, 64), (0, 1, 1)]
    {
        let envelope = crate::db::executor::PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                page_limit,
            );
        let bounded = crate::db::executor::with_production_scalar_page_work(envelope, || {
            index_leaf_chunk_entries_for_active_page(ACCESS_SCAN_CHUNK_ENTRIES, branch_count)
        })
        .expect("index leaf should inherit its share of the active page envelope");

        assert_eq!(bounded.value, expected_refill);
    }
}

#[test]
fn buffered_leaf_pulls_use_current_reservations_until_the_next_refill() {
    reset_heap_stores();
    load_primary_keys();
    load_index_keys(&IndexId::new(ENTITY, 1), b"lane");

    // A refill spends the page's four physical-entry allowance at once.
    // Its remaining buffered keys must still be admissible on that page;
    // the next refill must stop before another store visit.
    for index in [false, true] {
        for direction in [Direction::Asc, Direction::Desc] {
            let budget = HardExecutionBudget::uniform_for_tests(
                u64::MAX,
                HardExecutionFailureHeadroom::new(500, 256),
            );
            let context = HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::PublicRead,
                0,
            );
            let result = with_query_execution_budget_for_tests(budget, context, || {
                let envelope = PageWorkEnvelope::public_scalar().with_limit_for_tests(
                    DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                    4,
                );
                with_production_scalar_page_work(envelope, || {
                    let mut stream = if index {
                        let mut leaf = ordered_index_stream(direction, None, None);
                        leaf.chunk_entries =
                            index_leaf_chunk_entries_for_active_page(ACCESS_SCAN_CHUNK_ENTRIES, 1)?;
                        OrderedKeyStreamBox::IndexRange(leaf)
                    } else {
                        OrderedKeyStreamBox::PrimaryRange(PrimaryRangeKeyStream::new(
                            STORE,
                            data_key(1),
                            data_key(100),
                            direction,
                            None,
                        )?)
                    };
                    for (offset, expected_bound) in [4, 0, 0, 0].into_iter().enumerate() {
                        let bound = stream.page_access_entry_bound().unwrap();
                        assert_eq!(bound, expected_bound);
                        assert!(begin_production_scalar_page_unit(bound)?);
                        let key = match direction {
                            Direction::Asc => 1 + offset as u64,
                            Direction::Desc => 100 - offset as u64,
                        };
                        assert_eq!(stream.next_key()?, Some(data_key(key)));
                        finish_production_scalar_page_unit()?;
                    }
                    let bound = stream.page_access_entry_bound().unwrap();
                    assert_eq!(bound, 4);
                    assert!(!begin_production_scalar_page_unit(bound)?);
                    Ok(())
                })
                .map_err(QueryError::execute)
            })
            .unwrap();
            assert!(result.envelope_stopped);
            assert_eq!(
                result
                    .receipt
                    .observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
                4,
            );
        }
    }
}

#[test]
fn materialized_secondary_index_keys_charge_complete_temporary_backing() {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0);
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x6d61_7465_7269_616c,
    );
    let keys = vec![data_key(1)];
    let error = with_query_execution_budget_for_tests(budget, context, || {
        charge_materialized_secondary_index_keys(keys.as_slice(), keys.capacity())
            .map_err(QueryError::execute)
    })
    .expect_err("materialized key-vector capacity must consume the temporary-byte budget");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error.diagnostic_facts()[0],
        (
            DiagnosticFactTag::BudgetResource,
            DiagnosticExecutionBudgetResource::TemporaryBytes.raw(),
        ),
    );
}

#[test]
fn general_intersection_materialization_preserves_order_and_finite_page_bound() {
    for direction in [Direction::Asc, Direction::Desc] {
        let comparator = KeyOrderComparator::from_direction(direction);
        let streams = [vec![1, 2, 4], vec![2, 3, 4]]
            .into_iter()
            .map(|mut ids| {
                if direction == Direction::Desc {
                    ids.reverse();
                }
                OrderedKeyStreamBox::materialized(ids.into_iter().map(data_key).collect())
            })
            .collect();
        let stream = OrderedKeyStreamBox::intersect_all(streams, comparator).unwrap();
        assert_eq!(stream.page_access_entry_bound(), None);
        let mut materialized = materialize_ordered_key_stream(stream).unwrap();
        assert_eq!(materialized.page_access_entry_bound(), Some(0));
        let expected = if direction == Direction::Asc {
            [2, 4]
        } else {
            [4, 2]
        };
        assert_eq!(collect_keys(&mut materialized), expected.map(data_key));
    }
}

#[test]
fn general_intersection_materialization_retains_key_and_memory_budget_failures() {
    load_primary_keys();
    for resource in [
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        DiagnosticExecutionBudgetResource::TemporaryBytes,
    ] {
        let stream = if resource == DiagnosticExecutionBudgetResource::TemporaryBytes {
            // Prebuilt input isolates the collector's retained-memory charge.
            OrderedKeyStreamBox::materialized(vec![data_key(1)])
        } else {
            OrderedKeyStreamBox::primary_range(
                PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), Direction::Asc, None)
                    .unwrap(),
            )
        };
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(resource, 0);
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0x696e_7465_7273_6563,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            materialize_ordered_key_stream(stream)
                .map(|_| ())
                .map_err(QueryError::execute)
        })
        .expect_err("blocking materialization must retain hard budget refusal");
        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
        );
    }
}

#[test]
fn index_leaf_propagates_malformed_entry_corruption() {
    reset_heap_stores();
    let index_id = IndexId::new(ENTITY, 1);
    let component = b"lane";
    let raw_key = index_key(&index_id, component, 50)
        .to_raw()
        .expect("index key should encode");
    INDEX.with_borrow_mut(|store| {
        store.insert(
            raw_key.clone(),
            <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![0xff]))
                .expect("storage operation should succeed"),
        );
    });
    let mut stream = IndexRangeKeyStream::new(
        STORE,
        ENTITY,
        (Bound::Included(raw_key.clone()), Bound::Included(raw_key)),
        Direction::Asc,
        None,
        None,
        ACCESS_SCAN_CHUNK_ENTRIES,
    );
    let error = stream
        .next_key()
        .expect_err("malformed index entries must fail");
    assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
}

#[test]
fn physical_entrypoint_rejects_composite_plans() {
    use crate::db::access::AccessPlan;
    let plan = AccessPlan::<Value>::Union(vec![AccessPlan::full_scan(), AccessPlan::full_scan()]);
    assert!(
        resolve_structural_physical_key_stream(
            &plan.executable_contract(),
            STORE,
            ENTITY,
            AccessStreamBindings::new(AccessScanContinuationInput::new(None, Direction::Asc)),
            AccessStreamExecutionPolicy::canonical_key_order(None),
            None,
        )
        .is_err()
    );
}
