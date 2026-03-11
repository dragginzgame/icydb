//! Module: db::executor::pipeline::operators::post_access::tests
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::post_access::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::AccessPath,
    cursor::CursorBoundary,
    predicate::Predicate,
    query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
};
use crate::{
    db::MissingRowPolicy,
    model::{field::FieldKind, index::IndexModel},
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

static BUDGET_METADATA_RANK_INDEX_FIELDS: [&str; 1] = ["rank"];
static BUDGET_METADATA_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "rank_idx",
    "budget_metadata_entity",
    &BUDGET_METADATA_RANK_INDEX_FIELDS,
    true,
)];

crate::test_entity! {
    ident = BudgetMetadataEntity,
    id = Ulid,
    entity_name = "BudgetMetadataEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Uint),
    ],
    indexes = [&BUDGET_METADATA_INDEX_MODELS[0]],
}

#[test]
fn bounded_order_keep_count_includes_offset_for_non_cursor_page() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<u64>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(5),
        offset: 3,
    });

    assert_eq!(
        crate::db::executor::ExecutionKernel::bounded_order_keep_count(&plan, None),
        Some(9),
        "bounded ordering should keep offset + limit + 1 rows"
    );
}

#[test]
fn bounded_order_keep_count_disabled_when_cursor_present() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<u64>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(5),
        offset: 0,
    });
    let cursor = CursorBoundary { slots: Vec::new() };

    assert_eq!(
        crate::db::executor::ExecutionKernel::bounded_order_keep_count(&plan, Some(&cursor),),
        None,
        "bounded ordering should be disabled for continuation requests"
    );
}

#[test]
fn budget_safety_metadata_marks_pk_order_plan_as_access_order_satisfied() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&plan);
    assert!(
        metadata.access_order_satisfied_by_path,
        "single-field PK ordering should be marked access-order-satisfied"
    );
    assert!(
        !metadata.has_residual_filter,
        "plan without predicate should not report residual filtering"
    );
    assert!(
        !metadata.requires_post_access_sort,
        "access-order-satisfied plan should not require post-access sorting"
    );
}

#[test]
fn budget_safety_metadata_marks_residual_filter_plan_as_unsafe() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().predicate = Some(Predicate::True);

    let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&plan);
    assert!(
        metadata.has_residual_filter,
        "predicate-bearing plan must report residual filtering"
    );
    assert!(
        metadata.access_order_satisfied_by_path,
        "residual filter should not hide access-order satisfaction result"
    );
    assert!(
        !metadata.requires_post_access_sort,
        "residual filtering alone should not imply post-access sorting"
    );
}

#[test]
fn budget_safety_metadata_marks_secondary_index_order_plan_as_access_order_satisfied() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: BUDGET_METADATA_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&plan);
    assert!(
        metadata.access_order_satisfied_by_path,
        "index-prefix order-compatible plans should be marked access-order-satisfied",
    );
    assert!(
        !metadata.requires_post_access_sort,
        "index-order-compatible plans should skip post-access sort requirements",
    );
}

#[test]
fn budget_safety_metadata_marks_index_range_order_plan_as_access_order_satisfied() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            BUDGET_METADATA_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&plan);
    assert!(
        metadata.access_order_satisfied_by_path,
        "index-range order-compatible plans should be marked access-order-satisfied",
    );
    assert!(
        !metadata.requires_post_access_sort,
        "index-range order-compatible plans should skip post-access sort requirements",
    );
}

#[test]
fn budget_safety_metadata_order_contract_stays_aligned_with_route_helper() {
    let mut unordered_plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    unordered_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let unordered_metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&unordered_plan);
    let unordered_contract = crate::db::executor::route::access_order_satisfied_by_route_contract::<
        BudgetMetadataEntity,
        _,
    >(&unordered_plan);
    assert_eq!(
        unordered_metadata.access_order_satisfied_by_path, unordered_contract,
        "full-scan ordering metadata must stay aligned with route order contract",
    );

    let mut ordered_plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            BUDGET_METADATA_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    ordered_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let ordered_metadata = crate::db::executor::ExecutionKernel::budget_safety_metadata::<
        BudgetMetadataEntity,
        _,
    >(&ordered_plan);
    let ordered_contract = crate::db::executor::route::access_order_satisfied_by_route_contract::<
        BudgetMetadataEntity,
        _,
    >(&ordered_plan);
    assert_eq!(
        ordered_metadata.access_order_satisfied_by_path, ordered_contract,
        "index-range ordering metadata must stay aligned with route order contract",
    );
}
