//! Module: db::query::plan::tests::structural_guards
//! Responsibility: module-local ownership and contracts for db::query::plan::tests::structural_guards.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::PlanModelEntity;
use crate::{
    db::{
        access::AccessPath,
        executor::{ExecutablePlan, LoadExecutor},
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, AggregateKind, DistinctExecutionStrategy, FieldSlot,
            GroupAggregateSpec, GroupDistinctPolicyReason, GroupHavingSpec, GroupSpec,
            GroupedDistinctExecutionStrategy, GroupedExecutionConfig, GroupedExecutorHandoff,
            global_distinct_group_spec_for_semantic_aggregate,
            resolve_global_distinct_field_aggregate,
        },
    },
    traits::{EntityKey, EntityKind, EntitySchema, EntityValue},
    value::Value,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

type ExecutablePlanNewFn<E> =
    fn(crate::db::query::plan::AccessPlannedQuery<<E as EntityKey>::Key>) -> ExecutablePlan<E>;
type LoadExecuteFn<E> = fn(
    &LoadExecutor<E>,
    ExecutablePlan<E>,
) -> Result<crate::db::EntityResponse<E>, crate::error::InternalError>;
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
) -> Result<crate::db::EntityResponse<E>, crate::error::InternalError>;
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
type DistinctExecutionStrategyFn<K> = fn(&AccessPlannedQuery<K>) -> DistinctExecutionStrategy;

fn grouped_distinct_strategy_accessor_type_check<'a, K>(
    handoff: &'a GroupedExecutorHandoff<'a, K>,
) -> &'a GroupedDistinctExecutionStrategy {
    handoff.distinct_execution_strategy()
}

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
    let distinct_execution_strategy: DistinctExecutionStrategyFn<E::Key> =
        AccessPlannedQuery::distinct_execution_strategy;

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
    let _ = distinct_execution_strategy;
    let _ = grouped_distinct_strategy_accessor_type_check::<E::Key>;
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

// Walk one source tree and collect every Rust source path deterministically.
fn collect_rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root)
        .unwrap_or_else(|err| panic!("failed to read source directory {}: {err}", root.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!(
                "failed to read source directory entry under {}: {err}",
                root.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(path.as_path(), out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

// Strip top-level `#[cfg(test)]` items from source text so ownership checks
// only reason about runtime paths.
fn strip_cfg_test_items(source: &str) -> String {
    let mut output = String::new();
    let lines = source.lines();
    let mut pending_cfg_test = false;
    let mut skip_depth = 0usize;

    for line in lines {
        let trimmed = line.trim();
        if skip_depth > 0 {
            skip_depth = skip_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }
        if pending_cfg_test {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
            }
            pending_cfg_test = false;
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}

#[test]
fn projection_shape_construction_remains_planner_owned() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<String> = BTreeSet::from(["src/db/query/plan/projection.rs".to_string()]);
    let mut actual = BTreeSet::new();

    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());
        if !runtime_source.contains("ProjectionSpec::new(") {
            continue;
        }

        let relative = source_path
            .strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to compute relative source path for {}: {err}",
                    source_path.display()
                )
            })
            .to_string_lossy()
            .replace('\\', "/");
        actual.insert(relative);
    }

    assert_eq!(
        actual, allowed,
        "projection semantic shape construction must remain planner-owned; update allowlist only for intentional boundary changes",
    );
}

#[test]
fn grouped_and_scalar_projection_specs_share_planner_projection_boundary() {
    let model = <PlanModelEntity as EntitySchema>::MODEL;
    let scalar: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let grouped: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![
                    FieldSlot::resolve(model, "tag").expect("tag field should resolve"),
                ],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    target_field: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::unbounded(),
            });

    let scalar_projection = scalar.projection_spec(model);
    let grouped_projection = grouped.projection_spec(model);

    assert_eq!(
        scalar_projection.len(),
        model.fields.len(),
        "scalar projection should remain planner-owned and model-driven",
    );
    assert_eq!(
        grouped_projection.len(),
        2,
        "grouped projection should remain planner-owned and include grouped key + aggregate outputs",
    );
}
