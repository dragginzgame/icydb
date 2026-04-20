//! Module: db::query::plan::tests::structural_guards
//! Covers structural guardrails enforced during query planning.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::PlanModelEntity;
use crate::{
    db::{
        access::AccessPath,
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec,
            GroupDistinctPolicyReason, GroupSpec, GroupedExecutionConfig, expr::Expr,
            global_distinct_group_spec_for_semantic_aggregate,
            resolve_global_distinct_field_aggregate,
        },
    },
    traits::EntitySchema,
    value::Value,
};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

fn assert_global_distinct_builder_signature(
    builder: fn(
        AggregateKind,
        &str,
        GroupedExecutionConfig,
    ) -> Result<GroupSpec, GroupDistinctPolicyReason>,
) {
    let _ = builder;
}
#[test]
fn planner_global_distinct_shape_builder_contract_is_semantic_only() {
    assert_global_distinct_builder_signature(global_distinct_group_spec_for_semantic_aggregate);
}

#[test]
fn planner_distinct_resolution_projects_semantic_shape_handle() {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);
    let group_fields = Vec::<FieldSlot>::new();
    let aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: Some("tag".to_string()),
        input_expr: None,
        filter_expr: None,
        distinct: true,
    }];

    let resolved = resolve_global_distinct_field_aggregate(
        group_fields.as_slice(),
        aggregates.as_slice(),
        None::<&Expr>,
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
                    input_expr: None,
                    filter_expr: None,
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

#[test]
fn sql_where_predicate_compiler_stays_structural_and_boundary_scoped() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let compile_source =
        fs::read_to_string(crate_root.join("src/db/sql/lowering/predicate/compile.rs"))
            .expect("sql predicate compile source should be readable");
    let compile_runtime_source = strip_cfg_test_items(compile_source.as_str());

    for forbidden in [
        "normalize::",
        "lower_sql_expr(",
        "SqlExpr",
        "SqlLoweringError",
        "normalize_where_bool_expr(",
        "validate_where_bool_expr(",
    ] {
        assert!(
            !compile_runtime_source.contains(forbidden),
            "WHERE predicate compiler must not depend on semantic normalization or SQL-lowering helpers ({forbidden})",
        );
    }

    assert!(
        compile_runtime_source.contains("compile_bool_expr_to_predicate(expr)"),
        "WHERE predicate compiler should stay as a thin structural wrapper over the shared boolean compiler",
    );

    let shared_bool_compile_source =
        fs::read_to_string(crate_root.join("src/db/predicate/bool_expr.rs"))
            .expect("shared bool compiler source should be readable");
    let shared_bool_compile_runtime_source =
        strip_cfg_test_items(shared_bool_compile_source.as_str());

    assert!(
        shared_bool_compile_runtime_source.contains("debug_assert!(")
            && shared_bool_compile_runtime_source.contains("\"normalized boolean expression\""),
        "shared boolean compiler should assert normalized-expression invariants at the compile boundary",
    );

    let orchestrator_source =
        fs::read_to_string(crate_root.join("src/db/sql/lowering/predicate/mod.rs"))
            .expect("sql predicate lowering module source should be readable");
    let orchestrator_runtime_source = strip_cfg_test_items(orchestrator_source.as_str());

    assert!(
        orchestrator_runtime_source.contains("validate::validate_where_bool_expr(&expr)?;"),
        "WHERE predicate orchestration should validate before normalization",
    );
    assert!(
        orchestrator_runtime_source.contains("normalize::normalize_where_bool_expr(expr)"),
        "WHERE predicate orchestration should normalize before compilation",
    );
    assert!(
        orchestrator_runtime_source.contains("derive_where_predicate_subset(&expr)"),
        "WHERE predicate orchestration should derive predicate subsets only after validation and normalization",
    );
}

#[test]
fn aggregate_filter_predicate_flow_reuses_shared_where_and_boolean_boundaries() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));

    // Phase 1: aggregate FILTER lowering must reuse the shared WHERE boolean
    // seam instead of reopening clause-local normalization or compilation.
    let aggregate_lowering_source =
        fs::read_to_string(crate_root.join("src/db/sql/lowering/aggregate.rs"))
            .expect("aggregate lowering source should be readable");
    let aggregate_lowering_runtime_source =
        strip_cfg_test_items(aggregate_lowering_source.as_str());

    assert!(
        aggregate_lowering_runtime_source.contains("lower_sql_where_bool_expr(expr.as_ref())"),
        "aggregate FILTER lowering must reuse lower_sql_where_bool_expr(...)",
    );
    for forbidden in [
        "normalize_where_bool_expr(",
        "validate_where_bool_expr(",
        "compile_where_bool_expr_to_predicate(",
    ] {
        assert!(
            !aggregate_lowering_runtime_source.contains(forbidden),
            "aggregate FILTER lowering must not reopen WHERE boolean ownership locally ({forbidden})",
        );
    }

    // Phase 2: runtime boolean admission must stay on one shared TRUE-only
    // collapse helper across grouped HAVING and aggregate FILTER paths.
    let projection_grouped_source =
        fs::read_to_string(crate_root.join("src/db/executor/projection/grouped.rs"))
            .expect("grouped projection source should be readable");
    let projection_grouped_runtime_source =
        strip_cfg_test_items(projection_grouped_source.as_str());
    assert!(
        projection_grouped_runtime_source.contains("collapse_true_only_boolean_admission("),
        "grouped HAVING should reuse the shared TRUE-only boolean admission helper",
    );

    let grouped_aggregate_state_source =
        fs::read_to_string(crate_root.join("src/db/executor/aggregate/contracts/state.rs"))
            .expect("grouped aggregate state source should be readable");
    let grouped_aggregate_state_runtime_source =
        strip_cfg_test_items(grouped_aggregate_state_source.as_str());
    assert!(
        grouped_aggregate_state_runtime_source.contains("collapse_true_only_boolean_admission("),
        "grouped aggregate FILTER should reuse the shared TRUE-only boolean admission helper",
    );

    let sql_aggregate_execute_source =
        fs::read_to_string(crate_root.join("src/db/session/sql/execute/aggregate.rs"))
            .expect("sql aggregate execute source should be readable");
    let sql_aggregate_execute_runtime_source =
        strip_cfg_test_items(sql_aggregate_execute_source.as_str());
    assert!(
        sql_aggregate_execute_runtime_source.contains("collapse_true_only_boolean_admission("),
        "structural SQL aggregate FILTER should reuse the shared TRUE-only boolean admission helper",
    );
}

#[test]
fn typed_fluent_ordering_remains_the_only_builder_surface() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let core_files = [
        "src/db/query/expr.rs",
        "src/db/query/fluent/load/builder.rs",
        "src/db/query/fluent/delete.rs",
        "src/db/query/intent/query.rs",
    ];

    for relative in core_files {
        let source = fs::read_to_string(crate_root.join(relative))
            .unwrap_or_else(|err| panic!("failed to read {relative}: {err}"));
        let runtime_source = strip_cfg_test_items(source.as_str());

        for forbidden in [
            "pub fn order_by(",
            "pub fn order_by_desc(",
            "pub fn sort_expr(",
        ] {
            assert!(
                !runtime_source.contains(forbidden),
                "typed fluent ordering cut must not reintroduce removed builder APIs in {relative} ({forbidden})",
            );
        }
    }

    let public_surface = fs::read_to_string(
        crate_root
            .parent()
            .expect("crate root should have workspace parent")
            .join("icydb/src/db/query/expr.rs"),
    )
    .expect("public query expr surface should be readable");
    let public_runtime_source = strip_cfg_test_items(public_surface.as_str());

    assert!(
        !public_runtime_source.contains("SortExpr"),
        "public query expr surface must not reintroduce SortExpr after the hard cut",
    );
}
