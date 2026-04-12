//! Module: db::query::plan::tests::structural_guards
//! Covers structural guardrails enforced during query planning.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::PlanModelEntity;
use crate::{
    db::{
        PersistedRow,
        access::AccessPath,
        executor::{
            LoadExecutor, PreparedExecutionPlan, ScalarNumericFieldBoundaryRequest,
            ScalarProjectionBoundaryRequest, ScalarTerminalBoundaryRequest,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, AggregateKind, DistinctExecutionStrategy, FieldSlot,
            GroupAggregateSpec, GroupDistinctPolicyReason, GroupHavingSpec, GroupSpec,
            GroupedDistinctExecutionStrategy, GroupedExecutionConfig, GroupedExecutorHandoff,
            global_distinct_group_spec_for_semantic_aggregate,
            resolve_global_distinct_field_aggregate,
        },
    },
    traits::{EntitySchema, EntityValue},
    value::Value,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

type ExecutablePlanNewFn<E> =
    fn(crate::db::query::plan::AccessPlannedQuery) -> PreparedExecutionPlan<E>;
type LoadExecuteFn<E> = fn(
    &LoadExecutor<E>,
    PreparedExecutionPlan<E>,
) -> Result<crate::db::EntityResponse<E>, crate::error::InternalError>;
type SlotTopKByFn<E> = fn(
    &LoadExecutor<E>,
    PreparedExecutionPlan<E>,
    FieldSlot,
    u32,
) -> Result<crate::db::EntityResponse<E>, crate::error::InternalError>;
type DistinctExecutionStrategyFn = fn(&AccessPlannedQuery) -> DistinctExecutionStrategy;

fn grouped_distinct_strategy_accessor_type_check<'a>(
    handoff: &'a GroupedExecutorHandoff<'a>,
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
    E: PersistedRow + EntityValue,
{
    let executable_new: ExecutablePlanNewFn<E> = PreparedExecutionPlan::<E>::new;
    let load_execute: LoadExecuteFn<E> = LoadExecutor::<E>::execute;
    let scalar_projection_boundary = LoadExecutor::<E>::execute_scalar_projection_boundary;
    let top_k_by_slot: SlotTopKByFn<E> = LoadExecutor::<E>::top_k_by_slot;
    let scalar_numeric_boundary = LoadExecutor::<E>::execute_numeric_field_boundary;
    let scalar_terminal_boundary = LoadExecutor::<E>::execute_scalar_terminal_request;
    let distinct_execution_strategy: DistinctExecutionStrategyFn =
        AccessPlannedQuery::distinct_execution_strategy;
    let count_request = ScalarTerminalBoundaryRequest::Count;
    let exists_request = ScalarTerminalBoundaryRequest::Exists;
    let projection_values_request = ScalarProjectionBoundaryRequest::Values;
    let projection_distinct_values_request = ScalarProjectionBoundaryRequest::DistinctValues;
    let projection_count_distinct_request = ScalarProjectionBoundaryRequest::CountDistinct;
    let numeric_sum_request = ScalarNumericFieldBoundaryRequest::Sum;
    let numeric_avg_request = ScalarNumericFieldBoundaryRequest::Avg;
    let id_terminal_request = ScalarTerminalBoundaryRequest::IdTerminal {
        kind: AggregateKind::Min,
    };
    let id_by_slot_request = ScalarTerminalBoundaryRequest::IdBySlot {
        kind: AggregateKind::Max,
        target_field: FieldSlot::from_parts_for_test(0, "field".to_string()),
    };
    let nth_by_slot_request = ScalarTerminalBoundaryRequest::NthBySlot {
        target_field: FieldSlot::from_parts_for_test(0, "field".to_string()),
        nth: 0,
    };
    let median_by_slot_request = ScalarTerminalBoundaryRequest::MedianBySlot {
        target_field: FieldSlot::from_parts_for_test(0, "field".to_string()),
    };
    let min_max_by_slot_request = ScalarTerminalBoundaryRequest::MinMaxBySlot {
        target_field: FieldSlot::from_parts_for_test(0, "field".to_string()),
    };

    let _ = executable_new;
    let _ = load_execute;
    let _ = scalar_projection_boundary;
    let _ = top_k_by_slot;
    let _ = scalar_numeric_boundary;
    let _ = scalar_terminal_boundary;
    let _ = distinct_execution_strategy;
    let _ = count_request;
    let _ = exists_request;
    let _ = projection_values_request;
    let _ = projection_distinct_values_request;
    let _ = projection_count_distinct_request;
    let _ = numeric_sum_request;
    let _ = numeric_avg_request;
    let _ = id_terminal_request;
    let _ = id_by_slot_request;
    let _ = nth_by_slot_request;
    let _ = median_by_slot_request;
    let _ = min_max_by_slot_request;
    let _ = grouped_distinct_strategy_accessor_type_check;
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
    fn compile_only<E>()
    where
        E: PersistedRow + EntityValue,
    {
        assert_executor_entry_signatures::<E>();
    }

    compile_only::<PlanModelEntity>();
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
#[expect(
    clippy::too_many_lines,
    reason = "structural ownership guard intentionally checks one full canonicalization boundary in one assertion flow"
)]
fn canonicalization_ownership_stays_in_access_and_predicate_layers() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let planner_root = crate_root.join("src/db/query/plan/planner");
    let mut sources = Vec::new();
    collect_rust_sources(planner_root.as_path(), &mut sources);
    sources.sort();

    let mut forbidden_hits = Vec::new();
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
        for forbidden in [
            "fn canonicalize_",
            "canonicalize_in_literal_values(",
            "normalize_planned_access_plan_for_stability(",
        ] {
            if runtime_source.contains(forbidden) {
                let relative = source_path
                    .strip_prefix(crate_root)
                    .unwrap_or_else(|err| {
                        panic!(
                            "failed to compute relative source path for {}: {err}",
                            source_path.display()
                        )
                    })
                    .to_string_lossy()
                    .replace('\\', "/");
                forbidden_hits.push(format!("{relative}: {forbidden}"));
            }
        }
    }

    assert!(
        forbidden_hits.is_empty(),
        "planner canonicalization drift detected; keep canonicalization in access/predicate owners: {forbidden_hits:?}",
    );

    let access_owner = fs::read_to_string(crate_root.join("src/db/access/canonical.rs"))
        .expect("access canonical owner source should be readable");
    assert!(
        access_owner.contains("pub(crate) fn normalize_access_plan_value("),
        "access canonicalization owner surface should expose normalize_access_plan_value(...)",
    );

    let predicate_owner = fs::read_to_string(crate_root.join("src/db/predicate/normalize.rs"))
        .expect("predicate normalize owner source should be readable");
    assert!(
        predicate_owner.contains("pub(in crate::db) fn normalize("),
        "predicate canonicalization owner surface should expose normalize(...)",
    );

    let sql_lowering_source =
        fs::read_to_string(crate_root.join("src/db/sql/lowering/normalize.rs"))
            .expect("sql lowering normalize source should be readable");
    let sql_lowering_runtime_source = strip_cfg_test_items(sql_lowering_source.as_str());
    assert!(
        sql_lowering_runtime_source.contains("rewrite_field_identifiers("),
        "SQL lowering predicate adaptation should delegate traversal to predicate owner",
    );
    for forbidden in [
        "fn normalize_predicate_identifiers(",
        "fn normalize_compare(",
    ] {
        assert!(
            !sql_lowering_runtime_source.contains(forbidden),
            "SQL lowering must not own predicate canonical traversal helpers ({forbidden})",
        );
    }

    let explain_plan_source = fs::read_to_string(crate_root.join("src/db/query/explain/plan.rs"))
        .expect("query explain plan source should be readable");
    let explain_plan_runtime_source = strip_cfg_test_items(explain_plan_source.as_str());
    assert!(
        !explain_plan_runtime_source.contains("map(normalize)"),
        "EXPLAIN must consume canonical predicate models instead of re-normalizing",
    );

    let planner_predicate_source =
        fs::read_to_string(crate_root.join("src/db/query/plan/planner/predicate.rs"))
            .expect("planner predicate source should be readable");
    let planner_predicate_runtime_source = strip_cfg_test_items(planner_predicate_source.as_str());
    assert!(
        !planner_predicate_runtime_source.contains("plan_strict_same_field_eq_or("),
        "planner must not own local OR->IN structural rewrite helpers",
    );

    assert!(
        predicate_owner.contains("fn collapse_same_field_or_eq_to_in("),
        "predicate canonicalization owner should expose OR->IN structural rewrite boundary",
    );

    let access_choice_source =
        fs::read_to_string(crate_root.join("src/db/query/plan/access_choice/mod.rs"))
            .expect("access choice source should be readable");
    let access_choice_runtime_source = strip_cfg_test_items(access_choice_source.as_str());
    for forbidden in ["fn schema_literal_compatible(", "fn indexable_compare_op("] {
        assert!(
            !access_choice_runtime_source.contains(forbidden),
            "access-choice must consume shared planner predicate helpers ({forbidden})",
        );
    }
}

#[test]
fn grouped_and_scalar_projection_specs_share_planner_projection_boundary() {
    let model = <PlanModelEntity as EntitySchema>::MODEL;
    let scalar: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let grouped: AccessPlannedQuery =
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
