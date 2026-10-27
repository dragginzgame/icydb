mod borrowed_components;

use super::*;
use crate::{
    db::{
        QueryError, QueryExecutionError,
        data::{DataStore, RawRow},
        index::{IndexEntryValue, IndexId, IndexStore},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreAllocationIdentities, StoreRuntimeStorageCapabilities},
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
        schema::SchemaStore,
        test_support::index::nat64_index_key,
    },
    error::ErrorOrigin,
};
use ic_storage::adapter::Storable;
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, DiagnosticFactTag,
    RuntimeBoundaryCode,
};
use std::{borrow::Cow, cell::RefCell};

const EXACT_INTERSECTION_ENTITY: EntityTag = EntityTag::new(0x2221);

thread_local! {
    static EXACT_INTERSECTION_DATA: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static EXACT_INTERSECTION_INDEX: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static EXACT_INTERSECTION_SCHEMA: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
}

const EXACT_INTERSECTION_STORE: StoreHandle = StoreHandle::new(
    &EXACT_INTERSECTION_DATA,
    &EXACT_INTERSECTION_INDEX,
    &EXACT_INTERSECTION_SCHEMA,
    StoreAllocationIdentities::absent(),
    StoreRuntimeStorageCapabilities::heap(),
);

fn exact_intersection_budget() -> (HardExecutionBudget, HardExecutionContext) {
    (
        HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        ),
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0x6578_6163_745f_696e,
        ),
    )
}

fn exact_intersection_data_key(value: u64) -> DecodedDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EXACT_INTERSECTION_ENTITY,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
}

fn exact_intersection_primary_key(value: u64) -> ExactIntersectionPrimaryKey {
    ExactIntersectionPrimaryKey {
        value: PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    }
}

fn reset_exact_intersection_stores() {
    EXACT_INTERSECTION_DATA.with_borrow_mut(|store| *store = DataStore::init_heap());
    EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| *store = IndexStore::init_heap());
}

#[test]
fn filtered_index_scan_preserves_primary_keys_order_and_limits() {
    use crate::{
        db::{
            index::{
                EncodedValue, IndexKeyKind,
                predicate::{IndexCompareOp, IndexLiteral, IndexPredicateProgram},
            },
            key_taxonomy::CompositePrimaryKeyValue,
        },
        value::Value,
    };

    let predicate = IndexPredicateProgram::Compare {
        component_index: 0,
        op: IndexCompareOp::Gte,
        literal: IndexLiteral::One(
            EncodedValue::try_new(&Value::Nat64(2))
                .unwrap()
                .into_bytes(),
        ),
    };
    for composite in [false, true] {
        reset_exact_intersection_stores();
        let primary_keys: Vec<_> = (1..=5)
            .map(|index| {
                let primary_key = if composite {
                    PrimaryKeyValue::Composite(
                        CompositePrimaryKeyValue::try_from_components(&[
                            PrimaryKeyComponent::Nat64(index),
                            PrimaryKeyComponent::Nat64(index + 500),
                        ])
                        .unwrap(),
                    )
                } else {
                    PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(index))
                };
                let raw = IndexKey::new_from_components_with_primary_key_value(
                    &IndexId::new(EXACT_INTERSECTION_ENTITY, 1),
                    IndexKeyKind::User,
                    &[EncodedValue::try_new(&Value::Nat64(index)).unwrap()],
                    &primary_key,
                )
                .unwrap()
                .to_raw()
                .unwrap();
                let witness = if index == 1 {
                    // Predicate refusal must still precede witness decoding.
                    IndexEntryValue::from_persisted_bytes(vec![2])
                } else {
                    IndexEntryValue::presence()
                };
                EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| store.insert(raw, witness));
                primary_key
            })
            .collect();
        for direction in [Direction::Asc, Direction::Desc] {
            let mut expected = primary_keys[1..].to_vec();
            if direction == Direction::Desc {
                expected.reverse();
            }
            for limit in 0..=5 {
                let (budget, context) = exact_intersection_budget();
                let keys = with_query_execution_budget_for_tests(budget, context, || {
                    IndexScan::resolve_data_values_in_raw_range_limited(
                        EXACT_INTERSECTION_STORE,
                        EXACT_INTERSECTION_ENTITY,
                        &Bound::Unbounded,
                        &Bound::Unbounded,
                        IndexScanContinuationInput::new(None, direction),
                        limit,
                        Some(&predicate),
                    )
                    .map_err(QueryError::execute)
                })
                .unwrap();
                assert_eq!(keys.len(), limit.min(expected.len()));
                for (key, expected) in keys.iter().zip(expected.iter().take(limit)) {
                    assert_eq!(key.primary_key_value(), *expected);
                    assert_eq!(
                        key.to_raw().unwrap(),
                        DecodedDataStoreKey::new_primary_key_value(
                            EXACT_INTERSECTION_ENTITY,
                            expected,
                        )
                        .to_raw()
                        .unwrap()
                    );
                }
            }
        }
    }
}

#[test]
fn filtered_index_scan_preserves_corruption_and_refusal_order() {
    use crate::db::index::predicate::IndexPredicateProgram;

    let key = nat64_index_key(&IndexId::new(EXACT_INTERSECTION_ENTITY, 1), b"lane", 7);
    let raw = key.to_raw().unwrap();
    let mut invalid_primary = raw.as_bytes().to_vec();
    let primary_start = invalid_primary.len()
        - IndexKey::primary_key_value_and_bytes_from_raw(&raw)
            .unwrap()
            .1
            .len();
    invalid_primary[primary_start] = 0xFF;
    let invalid_primary = RawIndexStoreKey::from_persisted_bytes(invalid_primary);
    let mut trailing = raw.as_bytes().to_vec();
    trailing.push(0);
    let trailing = RawIndexStoreKey::from_persisted_bytes(trailing);
    let presence = IndexEntryValue::presence();
    let invalid_witness = IndexEntryValue::from_persisted_bytes(vec![2]);

    for (raw, value, expected_error) in [
        (
            &invalid_primary,
            &presence,
            InternalError::index_entry_decode_failed(),
        ),
        (
            &raw,
            &invalid_witness,
            InternalError::index_entry_decode_failed(),
        ),
        (&trailing, &presence, InternalError::index_corruption()),
    ] {
        for predicate in [IndexPredicateProgram::False, IndexPredicateProgram::True] {
            let mut keys = Vec::new();
            let result = IndexScan::decode_index_entry_and_push(
                EXACT_INTERSECTION_ENTITY,
                raw,
                value,
                &mut keys,
                Some(1),
                Some(&predicate),
            );
            if raw != &trailing && predicate == IndexPredicateProgram::False {
                assert!(!result.unwrap());
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.class(), expected_error.class());
                assert_eq!(error.origin(), expected_error.origin());
                assert_eq!(error.diagnostic(), expected_error.diagnostic());
            }
            assert!(keys.is_empty());
        }
    }
}

#[test]
fn merged_range_structure_returns_typed_temporary_byte_exhaustion() {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0);
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x6d65_7267_6564_7261,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        charge_merged_range_structural_bytes(1).map_err(QueryError::execute)
    })
    .expect_err("merged-range structure must consume the temporary-byte budget");

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
fn exact_intersection_structure_rejects_before_allocating_over_budget() {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0);
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x6578_6163_745f_6d65,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        IndexScan::charge_exact_intersection_structural_work(&[21, 20]).map_err(QueryError::execute)
    })
    .expect_err("direct intersection structure must be admitted before allocation");

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
fn exact_intersection_comparison_is_directionally_equivalent() {
    let (budget, context) = exact_intersection_budget();
    for (direction, left, right, expected) in [
        (Direction::Asc, vec![1, 3, 5], vec![2, 3, 5], vec![3, 5]),
        (Direction::Desc, vec![5, 3, 1], vec![5, 3, 2], vec![5, 3]),
    ] {
        let output = with_query_execution_budget_for_tests(budget, context, || {
            Ok::<_, QueryError>(IndexScan::intersect_exact_primary_keys(
                left.into_iter()
                    .map(exact_intersection_primary_key)
                    .collect(),
                right
                    .into_iter()
                    .map(exact_intersection_primary_key)
                    .collect::<Vec<_>>()
                    .as_slice(),
                direction,
            ))
        })
        .expect("bounded direct intersection should preserve direction");

        assert_eq!(
            output
                .into_iter()
                .map(|key| match key.value.scalar_component() {
                    Some(PrimaryKeyComponent::Nat64(value)) => value,
                    _ => panic!("exact intersection test key should remain Nat64"),
                })
                .collect::<Vec<_>>(),
            expected,
        );
    }
}

#[test]
fn exact_intersection_child_rejects_invalid_presence_marker() {
    reset_exact_intersection_stores();
    let index_id = IndexId::new(EXACT_INTERSECTION_ENTITY, 1);
    let raw_index_key = nat64_index_key(&index_id, b"lane", 7)
        .to_raw()
        .expect("exact intersection index key should encode");
    EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| {
        store.insert(
            raw_index_key.clone(),
            <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![1])),
        );
    });
    let lower = Bound::Included(raw_index_key.clone());
    let upper = Bound::Included(raw_index_key);
    let (budget, context) = exact_intersection_budget();
    let result = with_query_execution_budget_for_tests(budget, context, || {
        IndexScan::collect_exact_intersection_child_in_bounds(
            EXACT_INTERSECTION_STORE,
            (&lower, &upper),
            1,
            0,
            Direction::Asc,
        )
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("a direct intersection child must reject an invalid presence marker");
    };

    let QueryError::Execute(QueryExecutionError::Corruption(error)) = error else {
        panic!("invalid direct-intersection marker should retain corruption taxonomy");
    };
    assert_eq!(error.origin(), ErrorOrigin::Index);
}

#[test]
fn exact_intersection_child_accepts_a_present_authoritative_row() {
    reset_exact_intersection_stores();
    let data_key = exact_intersection_data_key(7);
    EXACT_INTERSECTION_DATA.with_borrow_mut(|store| {
        store.insert_raw_for_test(
            data_key
                .to_raw()
                .expect("exact intersection data key should encode"),
            RawRow::try_new(vec![0]).expect("exact intersection row should be bounded"),
        );
    });
    let index_id = IndexId::new(EXACT_INTERSECTION_ENTITY, 1);
    let raw_index_key = nat64_index_key(&index_id, b"lane", 7)
        .to_raw()
        .expect("exact intersection index key should encode");
    EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| {
        store.insert(raw_index_key.clone(), IndexEntryValue::presence());
    });
    let lower = Bound::Included(raw_index_key.clone());
    let upper = Bound::Included(raw_index_key);
    let (budget, context) = exact_intersection_budget();
    let keys = with_query_execution_budget_for_tests(budget, context, || {
        IndexScan::collect_exact_intersection_child_in_bounds(
            EXACT_INTERSECTION_STORE,
            (&lower, &upper),
            1,
            0,
            Direction::Asc,
        )
        .map_err(QueryError::execute)
    })
    .expect("a direct intersection child should accept an existing indexed row");

    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0].value,
        PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    );
}

#[test]
fn exact_intersection_child_rejects_an_uncounted_physical_entry() {
    reset_exact_intersection_stores();
    let index_id = IndexId::new(EXACT_INTERSECTION_ENTITY, 1);
    let first = nat64_index_key(&index_id, b"lane", 7)
        .to_raw()
        .expect("first exact intersection index key should encode");
    let second = nat64_index_key(&index_id, b"lane", 8)
        .to_raw()
        .expect("second exact intersection index key should encode");
    EXACT_INTERSECTION_INDEX.with_borrow_mut(|store| {
        store.insert(first.clone(), IndexEntryValue::presence());
        store.insert(second.clone(), IndexEntryValue::presence());
    });
    let lower = Bound::Included(first);
    let upper = Bound::Included(second);
    let (budget, context) = exact_intersection_budget();
    let result = with_query_execution_budget_for_tests(budget, context, || {
        IndexScan::collect_exact_intersection_child_in_bounds(
            EXACT_INTERSECTION_STORE,
            (&lower, &upper),
            1,
            0,
            Direction::Asc,
        )
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("an uncounted direct-intersection entry must fail as corruption");
    };

    let QueryError::Execute(QueryExecutionError::Corruption(error)) = error else {
        panic!("uncounted direct-intersection state should retain corruption taxonomy");
    };
    assert_eq!(error.origin(), ErrorOrigin::Store);
}
