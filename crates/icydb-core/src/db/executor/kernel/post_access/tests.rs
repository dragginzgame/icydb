use crate::db::{
    access::AccessPath,
    contracts::Predicate,
    cursor::CursorBoundary,
    query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
};
use crate::{db::MissingRowPolicy, model::field::FieldKind, types::Ulid};

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
    indexes = [],
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
