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

    let _ = executable_new;
    let _ = load_execute;
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
    #[allow(dead_code)]
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
