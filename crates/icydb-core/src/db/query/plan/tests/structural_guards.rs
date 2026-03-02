use super::PlanModelEntity;
use crate::{
    db::{
        executor::{ExecutablePlan, LoadExecutor},
        query::plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupDistinctPolicyReason,
            GroupHavingSpec, GroupSpec, GroupedExecutionConfig,
            global_distinct_group_spec_for_semantic_aggregate,
            resolve_global_distinct_field_aggregate,
        },
    },
    traits::{EntityKey, EntityKind, EntitySchema, EntityValue},
};

type ExecutablePlanNewFn<E> =
    fn(crate::db::query::plan::AccessPlannedQuery<<E as EntityKey>::Key>) -> ExecutablePlan<E>;
type LoadExecuteFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
) -> Result<crate::db::Response<E>, crate::error::InternalError>;
type SlotValuesByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
) -> Result<Vec<crate::value::Value>, crate::error::InternalError>;
type SlotTopKByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
    u32,
) -> Result<crate::db::Response<E>, crate::error::InternalError>;
type SlotSumByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
) -> Result<Option<crate::types::Decimal>, crate::error::InternalError>;
type SlotCountDistinctByFn<E> =
    fn(&LoadExecutor<E>, ExecutablePlan<E>, FieldSlot) -> Result<u32, crate::error::InternalError>;
type SlotMinByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>;
type SlotMaxByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>;
type SlotNthByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
    usize,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>;
type SlotMedianByFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
    FieldSlot,
) -> Result<Option<crate::types::Id<E>>, crate::error::InternalError>;
type SlotMinMaxByFn<E> =
    fn(
        &LoadExecutor<E>,
        ExecutablePlan<E>,
        FieldSlot,
    )
        -> Result<Option<(crate::types::Id<E>, crate::types::Id<E>)>, crate::error::InternalError>;

fn assert_global_distinct_builder_signature(
    builder: fn(
        AggregateKind,
        &str,
        GroupedExecutionConfig,
    ) -> Result<GroupSpec, GroupDistinctPolicyReason>,
) {
    let _ = builder;
}

fn assert_executor_entry_signatures<E>()
where
    E: EntityKind + EntityValue,
{
    let executable_new: ExecutablePlanNewFn<E> = ExecutablePlan::<E>::new;
    let load_execute: LoadExecuteFn<E> = LoadExecutor::<E>::execute;
    let values_by_slot: SlotValuesByFn<E> = LoadExecutor::<E>::values_by_slot;
    let top_k_by_slot: SlotTopKByFn<E> = LoadExecutor::<E>::top_k_by_slot;
    let aggregate_sum_by_slot: SlotSumByFn<E> = LoadExecutor::<E>::aggregate_sum_by_slot;
    let aggregate_count_distinct_by_slot: SlotCountDistinctByFn<E> =
        LoadExecutor::<E>::aggregate_count_distinct_by_slot;
    let aggregate_min_by_slot: SlotMinByFn<E> = LoadExecutor::<E>::aggregate_min_by_slot;
    let aggregate_max_by_slot: SlotMaxByFn<E> = LoadExecutor::<E>::aggregate_max_by_slot;
    let aggregate_nth_by_slot: SlotNthByFn<E> = LoadExecutor::<E>::aggregate_nth_by_slot;
    let aggregate_median_by_slot: SlotMedianByFn<E> = LoadExecutor::<E>::aggregate_median_by_slot;
    let aggregate_min_max_by_slot: SlotMinMaxByFn<E> = LoadExecutor::<E>::aggregate_min_max_by_slot;

    let _ = executable_new;
    let _ = load_execute;
    let _ = values_by_slot;
    let _ = top_k_by_slot;
    let _ = aggregate_sum_by_slot;
    let _ = aggregate_count_distinct_by_slot;
    let _ = aggregate_min_by_slot;
    let _ = aggregate_max_by_slot;
    let _ = aggregate_nth_by_slot;
    let _ = aggregate_median_by_slot;
    let _ = aggregate_min_max_by_slot;
}

#[test]
fn planner_global_distinct_shape_builder_contract_is_semantic_only() {
    assert_global_distinct_builder_signature(global_distinct_group_spec_for_semantic_aggregate);
}

#[test]
fn executor_entry_contract_requires_planned_query_wrapping() {
    // Signature checks compile under trait bounds and fail if executor entrypoints
    // drift away from planned-query + executable-plan contracts.
    // Compile-time only helper; no runtime call is required for this guard.
    #[expect(dead_code)]
    fn compile_only<E>()
    where
        E: EntityKind + EntityValue,
    {
        assert_executor_entry_signatures::<E>();
    }
}

#[test]
fn planner_distinct_resolution_projects_semantic_shape_handle() {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);
    let group_fields = Vec::<FieldSlot>::new();
    let aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: Some("tag".to_string()),
        distinct: true,
    }];

    let resolved = resolve_global_distinct_field_aggregate(
        group_fields.as_slice(),
        aggregates.as_slice(),
        None::<&GroupHavingSpec>,
    )
    .expect("global distinct semantic shape should resolve without policy rejection")
    .expect("global distinct candidate should project one semantic aggregate handle");

    assert_eq!(resolved.kind(), AggregateKind::Count);
    assert_eq!(resolved.target_field(), "tag");

    let semantic_shape = global_distinct_group_spec_for_semantic_aggregate(
        resolved.kind(),
        resolved.target_field(),
        execution,
    )
    .expect("semantic aggregate handle should lower into grouped shape");
    let aggregate_expr_shape = GroupSpec::global_distinct_shape_from_aggregate_expr(
        &crate::db::count_by("tag").distinct(),
        execution,
    );

    assert_eq!(
        semantic_shape, aggregate_expr_shape,
        "global distinct grouped shape should be derivable from one semantic aggregate handle",
    );
}

#[test]
fn planner_distinct_resolution_requires_planner_visibility_boundary() {
    let model = <PlanModelEntity as EntitySchema>::MODEL;
    let unresolved = FieldSlot::resolve(model, "missing");

    assert!(
        unresolved.is_none(),
        "planner field-slot resolution should remain the canonical grouped field identity boundary",
    );
}
