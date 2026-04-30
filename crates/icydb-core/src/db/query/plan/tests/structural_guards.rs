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
            global_distinct_group_spec_for_aggregate_identity,
            resolve_global_distinct_field_aggregate,
        },
    },
    traits::EntitySchema,
    value::Value,
};
use std::{
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
    assert_global_distinct_builder_signature(global_distinct_group_spec_for_aggregate_identity);
}

#[test]
fn planner_distinct_resolution_projects_identity_shape_handle() {
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
    .expect("global distinct identity shape should resolve without policy rejection")
    .expect("global distinct candidate should project one aggregate identity handle");

    assert_eq!(resolved.kind(), AggregateKind::Count);
    assert_eq!(resolved.target_field(), "tag");

    let identity_shape = global_distinct_group_spec_for_aggregate_identity(
        resolved.kind(),
        resolved.target_field(),
        execution,
    )
    .expect("aggregate identity handle should lower into grouped shape");
    let aggregate_expr_shape = GroupSpec::global_distinct_shape_from_aggregate_expr(
        &crate::db::count_by("tag").distinct(),
        execution,
    );

    assert_eq!(
        identity_shape, aggregate_expr_shape,
        "global distinct grouped shape should be derivable from one aggregate identity handle",
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
fn planner_bool_expr_semantics_are_not_routed_through_predicate_runtime_paths() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = crate_root.join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
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
        if relative.starts_with("src/db/predicate/") {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());
        for forbidden in [
            "predicate::canonicalize_grouped_having_bool_expr",
            "predicate::canonicalize_scalar_where_bool_expr",
            "predicate::normalize_bool_expr",
            "predicate::is_normalized_bool_expr",
        ] {
            if runtime_source.contains(forbidden) {
                forbidden_hits.push(format!("{relative}: {forbidden}"));
            }
        }
    }

    assert!(
        forbidden_hits.is_empty(),
        "planner-owned boolean semantics should flow through query::plan::expr, not predicate re-exports: {forbidden_hits:?}",
    );
}

#[test]
fn planner_truth_admission_stays_below_normalize_and_case() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/canonicalize/truth_admission.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));

    let forbidden_patterns = [
        "canonicalize::normalize",
        "canonicalize::case",
        "super::normalize",
        "super::case",
        "normalize_bool_expr",
        "canonicalize_normalized_bool_case",
        "lower_searched_case_to_boolean",
        "rewrite_affine_numeric_compare_expr",
    ];
    let forbidden_hits = forbidden_patterns
        .iter()
        .copied()
        .filter(|pattern| source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(
        forbidden_hits.is_empty(),
        "truth admission must remain a pure lower-level predicate owner and must not depend on normalize, case, or rewrite modules: {forbidden_hits:?}",
    );
}

#[test]
fn planner_bool_associative_dedup_requires_determinism_contract() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/canonicalize/normalize.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let expected_sequence = [
        "    assert!(",
        "        children.iter().all(expr_is_deterministic),",
        "        \"associative boolean dedup requires deterministic child expressions\",",
        "    );",
        "    children.dedup();",
    ]
    .join("\n");

    assert!(
        source.contains(&expected_sequence),
        "associative boolean dedup must keep a release-enforced determinism assertion immediately adjacent to children.dedup()",
    );
}

#[test]
fn planner_expr_canonicalize_does_not_depend_on_downstream_stages() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = crate_root.join("src/db/query/plan/expr/canonicalize");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let forbidden_patterns = [
        "type_inference",
        "infer_expr_type",
        "infer_typed_expr",
        "TypedExpr",
        "predicate_compile",
        "compile_normalized_bool_expr_to_predicate",
        "CompiledPredicate",
        "projection_eval",
        "eval_projection_function_call",
        "collapse_true_only_boolean_admission",
        "db::predicate",
        "Predicate::",
    ];
    let mut forbidden_hits = Vec::new();
    for source_path in sources {
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
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());

        forbidden_hits.extend(
            forbidden_patterns
                .iter()
                .copied()
                .filter(|pattern| runtime_source.contains(pattern))
                .map(|pattern| format!("{relative}: {pattern}")),
        );
    }

    assert!(
        forbidden_hits.is_empty(),
        "canonicalize must remain the first expression stage and must not depend on downstream type, predicate, or projection stages: {forbidden_hits:?}",
    );
}

#[test]
fn planner_expr_truth_value_policy_does_not_depend_on_pipeline_stages() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/truth_value.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());
    let forbidden_patterns = [
        "canonicalize",
        "CanonicalExpr",
        "normalize_bool_expr",
        "type_inference",
        "TypedExpr",
        "infer_expr_type",
        "predicate_compile",
        "CompiledPredicate",
        "Predicate::",
        "projection_eval",
        "eval_projection_function_call",
    ];
    let forbidden_hits = forbidden_patterns
        .iter()
        .copied()
        .filter(|pattern| runtime_source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(
        forbidden_hits.is_empty(),
        "truth-value admission must remain a stage-neutral evaluated-value policy and must not depend on canonicalize, type inference, predicate compile, or projection eval: {forbidden_hits:?}",
    );
}

#[test]
fn planner_expr_type_inference_does_not_call_predicate_compile() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/type_inference/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());
    let forbidden_patterns = [
        "predicate_compile",
        "canonicalize",
        "CanonicalExpr",
        "normalize_bool_expr",
        "rewrite_affine_numeric_compare_expr",
        "compile_normalized_bool_expr_to_predicate",
        "CompiledPredicate",
        "derive_normalized_bool_expr_predicate_subset",
        "projection_eval",
        "eval_projection_function_call",
        "db::predicate",
        "predicate::",
    ];
    let forbidden_hits = forbidden_patterns
        .iter()
        .copied()
        .filter(|pattern| runtime_source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(
        forbidden_hits.is_empty(),
        "type inference must not call downstream predicate compilation or predicate runtime surfaces: {forbidden_hits:?}",
    );
}

#[test]
fn planner_expr_predicate_compile_does_not_rerun_type_inference() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/predicate_compile.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());
    let forbidden_patterns = [
        "TruthAdmission::",
        "TruthAdmission {",
        "TruthWrapperScope::",
        "TruthWrapperScope {",
        "affine_field_offset",
        "compile_ready_",
        "infer_expr_type",
        "ExprType",
        "type_inference",
        "SchemaInfo",
        "FieldKind",
        "classify_field_kind",
        ".field_kind(",
        "canonicalize_",
        "lower_searched_case_to_boolean",
        "normalize_bool_expr",
        "normalize_bool_expr_impl",
        "normalize_bool_case_expr",
        "bool_expr_normalized_order",
        "collect_normalized_bool_associative_children",
        "expr_is_deterministic",
        "rebuild_normalized_bool_associative_chain",
        "simplify_bool_expr_constants",
        "rewrite_affine_numeric_compare_expr",
    ];
    let forbidden_hits = forbidden_patterns
        .iter()
        .copied()
        .filter(|pattern| runtime_source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(
        forbidden_hits.is_empty(),
        "predicate compilation must consume already-typed canonical boolean shape instead of re-running type inference or recreating canonicalization helpers: {forbidden_hits:?}",
    );
}

#[test]
fn planner_expr_predicate_artifact_requires_canonical_expr() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/predicate_compile.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));

    assert!(
        source.contains("expr: &CanonicalExpr"),
        "compiled predicate artifacts must be produced from CanonicalExpr, not raw Expr",
    );
    assert!(
        !source.contains("compiled_predicate(\n    expr: &Expr"),
        "compiled predicate artifact entrypoints must not accept raw Expr",
    );
    assert!(
        source.contains("derive_canonical_bool_expr_predicate_subset"),
        "predicate subset derivation must expose a CanonicalExpr artifact entrypoint",
    );
}

#[test]
fn planner_expr_projection_eval_does_not_canonicalize_or_import_predicates() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/projection_eval.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());
    let forbidden_patterns = [
        "CoercionId::",
        "CoercionSpec::",
        "CompareFieldsPredicate",
        "CompareOp::",
        "ComparePredicate",
        "MembershipCompareLeaf",
        "Predicate::",
        "enum Predicate",
        "struct Predicate",
        "canonicalize_",
        "normalize_bool_expr",
        "simplify_bool_expr_constants",
        "rewrite_affine_numeric_compare_expr",
        "compile_normalized_bool_expr_to_predicate",
        "compile_canonical_bool_expr_to_compiled_predicate",
        "CompiledPredicate",
        "derive_normalized_bool_expr_predicate_subset",
        "type_inference",
        "infer_expr_type",
        "infer_typed_expr",
        "TypedExpr",
        "query::plan::expr::canonicalize",
        "db::predicate",
        "predicate::",
        "fn collapse_true_only_boolean_admission",
    ];
    let forbidden_hits = forbidden_patterns
        .iter()
        .copied()
        .filter(|pattern| runtime_source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(
        forbidden_hits.is_empty(),
        "projection evaluation must not perform canonicalization, import predicate runtime semantics, or construct predicate AST nodes: {forbidden_hits:?}",
    );
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
