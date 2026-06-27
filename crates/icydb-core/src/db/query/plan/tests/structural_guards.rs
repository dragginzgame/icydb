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
        test_support::source_guard::{
            collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
        },
    },
    traits::EntitySchema,
    value::Value,
};
use std::{collections::BTreeMap, fs, path::Path};

fn assert_global_distinct_builder_signature(
    builder: fn(
        AggregateKind,
        &str,
        GroupedExecutionConfig,
    ) -> Result<GroupSpec, GroupDistinctPolicyReason>,
) {
    let _ = builder;
}

fn runtime_pattern_counts(pattern: &str) -> BTreeMap<String, usize> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = crate_root.join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let mut counts = BTreeMap::new();
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

        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let count = source
            .lines()
            .filter(|line| {
                let trimmed = line.trim_start();
                line.contains(pattern) && !trimmed.starts_with("fn ") && !trimmed.starts_with("pub")
            })
            .count();
        if count != 0 {
            counts.insert(relative, count);
        }
    }

    counts
}

fn source_for(crate_root: &Path, relative_path: &str) -> String {
    let source_path = crate_root.join(relative_path);
    fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()))
}

fn assert_source_contains_patterns(source: &str, patterns: &[&str], message: &str) {
    let missing = patterns
        .iter()
        .copied()
        .filter(|pattern| !source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(missing.is_empty(), "{message}: {missing:?}");
}

fn assert_source_excludes_patterns(source: &str, patterns: &[&str], message: &str) {
    let present = patterns
        .iter()
        .copied()
        .filter(|pattern| source.contains(pattern))
        .collect::<Vec<_>>();

    assert!(present.is_empty(), "{message}: {present:?}");
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
        input_expr: Some(Box::new(crate::db::query::plan::expr::Expr::Field(
            crate::db::query::plan::expr::FieldId::new("tag"),
        ))),
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

        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        if relative.starts_with("src/db/predicate/") {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());
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
        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());

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
    let runtime_source = runtime_source_without_test_items(source.as_str());
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
    let source_root = crate_root.join("src/db/query/plan/expr/type_inference");
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

        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());

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
        "type inference must not call downstream predicate compilation or predicate runtime surfaces: {forbidden_hits:?}",
    );
}

#[test]
fn planner_expr_predicate_compile_does_not_rerun_type_inference() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/predicate/compile.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = runtime_source_without_test_items(source.as_str());
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
    let source_path = crate_root.join("src/db/query/plan/expr/predicate/compile.rs");
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
fn filter_authority_predicate_subset_derivation_sites_are_explicit() {
    let expected = BTreeMap::from([
        ("src/db/query/intent/state.rs".to_string(), 1),
        ("src/db/sql/lowering/predicate/mod.rs".to_string(), 1),
    ]);

    assert_eq!(
        runtime_pattern_counts("derive_normalized_bool_expr_predicate_subset("),
        expected,
        "pre-access predicate-subset derivation should stay localized to query intent and SQL admission/access-mirror seams recorded for 0.186",
    );
}

#[test]
fn filter_authority_sql_explicit_predicate_lanes_are_explicit() {
    let expected = BTreeMap::from([
        ("src/db/sql/lowering/predicate/mod.rs".to_string(), 1),
        ("src/db/sql/lowering/select/mod.rs".to_string(), 3),
    ]);

    assert_eq!(
        runtime_pattern_counts("derive_sql_where_expr_predicate_subset("),
        expected,
        "SQL predicate-subset extraction should stay localized to the shared helper and the explicit admission/access-mirror lanes recorded for 0.186",
    );
}

#[test]
fn filter_authority_sql_predicate_handoffs_are_explicit() {
    assert_eq!(
        runtime_pattern_counts("filter_expr_with_normalized_predicate("),
        BTreeMap::from([
            ("src/db/query/intent/query.rs".to_string(), 1),
            ("src/db/sql/lowering/select/mod.rs".to_string(), 2),
        ]),
        "expression-plus-predicate handoff should remain localized to query intent and the SQL strict-predicate/access-mirror policy boundaries",
    );
    assert_eq!(
        runtime_pattern_counts("filter_normalized_predicate("),
        BTreeMap::from([
            ("src/db/query/intent/query.rs".to_string(), 1),
            ("src/db/sql/lowering/select/mod.rs".to_string(), 1),
        ]),
        "predicate-only handoff should remain localized to query intent and the SQL strict-predicate policy boundary",
    );
    assert_eq!(
        runtime_pattern_counts("lower_sql_where_expr("),
        BTreeMap::from([("src/db/sql/lowering/select/mod.rs".to_string(), 1)]),
        "strict SQL WHERE lowering should remain localized to UPDATE selector admission",
    );
    assert_eq!(
        runtime_pattern_counts("from_update_where_expr("),
        BTreeMap::from([("src/db/sql/lowering/select/mod.rs".to_string(), 1)]),
        "UPDATE selector predicate admission should stay localized to the explicit strict UPDATE policy lane",
    );
    assert_eq!(
        runtime_pattern_counts("from_where_expr_requiring_predicate_subset("),
        BTreeMap::from([(
            "src/db/sql/lowering/aggregate/command/global.rs".to_string(),
            1,
        )]),
        "global aggregate base-WHERE predicate admission should stay localized to the explicit fail-closed global aggregate lane",
    );
}

#[test]
fn filter_authority_downstream_consumers_do_not_extract_predicate_facts() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_roots = [
        "src/db/executor/aggregate/count_terminal.rs",
        "src/db/executor/explain",
        "src/db/executor/planning/route",
        "src/db/query/intent/cache_key.rs",
        "src/db/query/intent/query.rs",
        "src/db/query/plan/access_plan.rs",
        "src/db/query/plan/pipeline.rs",
        "src/db/query/plan/planner",
        "src/db/query/plan/semantics/logical.rs",
        "src/db/session/query/cache.rs",
    ];
    let forbidden_patterns = [
        "derive_normalized_bool_expr_predicate_subset(",
        "derive_canonical_bool_expr_predicate_subset(",
        "derive_sql_where_expr_predicate_subset(",
        "compile_normalized_bool_expr_to_predicate(",
        "compile_canonical_bool_expr_to_compiled_predicate(",
        "lower_sql_where_expr(",
    ];
    let mut forbidden_hits = Vec::new();

    let mut source_paths = Vec::new();
    for source_root in source_roots {
        let path = crate_root.join(source_root);
        if path.is_dir() {
            collect_rust_sources(path.as_path(), &mut source_paths);
        } else {
            source_paths.push(path);
        }
    }
    source_paths.sort();

    for source_path in source_paths {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());

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
        "downstream cache, route, EXPLAIN, and count/cardinality consumers must consume query-intent/planner predicate projections instead of deriving predicate facts from SQL/fluent expressions: {forbidden_hits:?}",
    );
}

#[test]
fn prefix_cardinality_count_entrypoints_share_proof_and_terminal_authority() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let session_cache = source_for(crate_root, "src/db/session/query/cache.rs");
    let count_terminal = source_for(crate_root, "src/db/executor/aggregate/count_terminal.rs");
    let direct_count = source_for(crate_root, "src/db/session/sql/execute/direct_count.rs");

    assert_source_contains_patterns(
        &session_cache,
        &[
            "pub(in crate::db) fn direct_count_cardinality_prefix_specs_for_accepted_authority(",
            "query.try_build_count_cardinality_prefix_access_with_schema_info(",
            "query.build_plan_with_visible_indexes(visible_indexes)?",
            "Self::direct_count_cardinality_prefix_specs_from_planned_query(authority, &plan)",
            "fn direct_count_cardinality_prefix_specs_from_planned_query(",
            "lower_access(authority.entity_tag(), &plan.access)",
            "exact_count_cardinality_prefixes_for_plan(",
            "lowered_access.index_prefix_specs()",
            "lowered_index_prefix_cardinality_specs_from_plan(",
        ],
        "direct SQL count prefix-spec admission may keep its accepted-authority shortcut, but its planned fallback must share the planner prefix-cardinality proof",
    );

    assert_source_contains_patterns(
        &count_terminal,
        &[
            "fn try_prepare_index_prefix_cardinality_preflight(",
            "let prefixes = exact_count_cardinality_prefixes_for_plan(",
            "terminal.into_preflight(authority, logical_plan, prefixes)",
            "execute_measured_index_prefix_cardinality_terminal(",
            "count_index_prefix_cardinality_specs(store, page, prefixes)",
            "count_index_prefix_cardinality(store, page, prefixes)",
            "exists_index_prefix_cardinality(store, page, prefixes)",
            "index_prefix_cardinality_sum_for_specs(",
            "index_prefix_cardinality_sum_for_plan(",
            "fn index_prefix_cardinality_sum<",
        ],
        "direct SQL COUNT and prepared COUNT/EXISTS prefix-cardinality terminals must converge on the shared proof, page-window, measurement, and store-cardinality helper path",
    );

    assert_source_excludes_patterns(
        &direct_count,
        &[
            "exact_prefix_cardinality_sum(",
            "index_prefix_cardinality_sum(",
            "count_index_prefix_cardinality_specs(",
            "count_index_prefix_cardinality(",
        ],
        "SQL direct COUNT should carry accepted prefix specs to count_terminal.rs instead of owning store-cardinality execution",
    );
}

#[test]
fn sql_write_candidate_bounds_keep_mutation_batch_and_delete_boundaries_explicit() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let write_mod = source_for(crate_root, "src/db/session/sql/execute/write/mod.rs");
    let update = source_for(crate_root, "src/db/session/sql/execute/write/update.rs");
    let insert = source_for(crate_root, "src/db/session/sql/execute/write/insert.rs");
    let delete = source_for(crate_root, "src/db/session/sql/execute/write/delete.rs");
    let delete_api = source_for(crate_root, "src/db/executor/delete/api.rs");
    let structural_delete = source_for(
        crate_root,
        "src/db/executor/delete/structural_projection.rs",
    );

    assert_source_contains_patterns(
        &write_mod,
        &[
            "struct SqlWriteCandidateBounds",
            "fn validate(self, candidate_rows: SqlWriteCandidateRows)",
            "struct SqlWriteMutationExecution<E>",
            "fn from_bounded_batch(",
            "let staged_rows = rows.validate_staged_rows(bounds)?;",
            "fn execute_sql_write_mutation_batch<E>(",
        ],
        "SQL UPDATE/INSERT staged-row admission should stay centralized in SqlWriteMutationExecution",
    );

    assert_source_contains_patterns(
        &update,
        &[
            "collect_sql_write_mutation_batch_from_structural_query(",
            "SqlWriteMutationExecution::from_bounded_batch(",
            "sql_update_candidate_bounds(execution_bounds)",
        ],
        "SQL UPDATE should feed collected selector rows through the shared mutation batch bound",
    );

    assert_source_contains_patterns(
        &insert,
        &[
            "let candidate_bounds =",
            "sql_insert_candidate_bounds(execution_bounds, statement.returning.is_some())",
            "candidate_bounds.validate(SqlWriteCandidateRows::from_len(values.len()))?",
            "SqlWriteMutationExecution::from_bounded_batch(",
        ],
        "SQL INSERT VALUES and INSERT SELECT should share candidate bounds through the mutation batch handoff",
    );

    assert_source_contains_patterns(
        &delete,
        &[
            "const fn sql_delete_candidate_bounds(",
            "const fn sql_delete_projection_bounds(",
            "DeleteProjectionBounds::max_rows(max_rows)",
            ".execute_count_with_bounds(plan, bounds)",
            ".execute_structural_projection_with_bounds(",
        ],
        "SQL DELETE should project SQL write bounds into the delete-specific pre-commit projection/count boundary",
    );

    assert_source_contains_patterns(
        &delete_api,
        &[
            "prepare_structural_delete_projection_core(",
            "prepare_structural_delete_count_core_with_bounds(",
            "Self::apply_prepared_delete_commit(db, &prepared, projection.commit.row_ops)?",
            "Self::apply_prepared_delete_commit(db, &prepared, count.commit.row_ops)?",
        ],
        "delete executor wrappers should keep SQL projection/count bounds before the typed commit-window bridge",
    );

    assert_source_contains_patterns(
        &structural_delete,
        &[
            "validate_structural_delete_projection_bounds(&prepared_projection.output, bounds)?",
            "validate_precommit(&prepared_projection.output)?",
            "prepare_structural_delete_count_core_with_optional_bounds(",
            "validate_structural_delete_row_count_bounds(",
        ],
        "structural DELETE count/RETURNING bounds should stay in the delete post-access output boundary, before commit",
    );
}

#[test]
fn scalar_entrypoints_share_execution_inputs_spine() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let execution = source_for(
        crate_root,
        "src/db/executor/pipeline/entrypoints/scalar/execution.rs",
    );
    let materialized = source_for(
        crate_root,
        "src/db/executor/pipeline/entrypoints/scalar/materialized.rs",
    );
    let streaming = source_for(
        crate_root,
        "src/db/executor/pipeline/entrypoints/scalar/streaming.rs",
    );
    let entrypoints = source_for(
        crate_root,
        "src/db/executor/pipeline/entrypoints/scalar/entrypoints.rs",
    );

    assert_source_contains_patterns(
        &execution,
        &[
            "pub(super) fn execute_prepared_scalar_kernel<T>(",
            "prepare_scalar_route_for_execution(",
            "let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputContext {",
            "record_plan_metrics(entity_path, plan);",
            "with_execution_stats_capture(debug, ||",
            "pub(super) fn finish_scalar_kernel_observability(",
        ],
        "scalar route setup, ExecutionInputs construction, plan metrics, and observability should stay centralized in execute_prepared_scalar_kernel",
    );

    assert_source_contains_patterns(
        &materialized,
        &[
            "execute_prepared_scalar_kernel(",
            "ExecutionKernel::materialize_with_optional_residual_retry(",
            "finish_scalar_kernel_observability(",
        ],
        "materialized scalar pages should consume the shared scalar kernel spine and own only page materialization/finalization",
    );

    assert_source_contains_patterns(
        &streaming,
        &[
            "execute_prepared_scalar_kernel(",
            "ExecutionKernel::materialize_kernel_rows_with_optional_residual_retry(",
            "finish_scalar_kernel_observability(",
        ],
        "aggregate row sinks should consume the shared scalar kernel spine and own only retained kernel-row sinking",
    );

    assert_source_contains_patterns(
        &entrypoints,
        &[
            "execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister",
            "prepare_initial_scalar_retained_slot_page_runtime_from_handoff(",
            "execute_prepared_scalar_route_runtime(prepared)?",
            "execute_prepared_scalar_aggregate_kernel_row_sink_for_canister",
            "execute_prepared_scalar_kernel_row_sink_execution(prepared, row_sink)?",
        ],
        "retained-slot page and aggregate row-sink entrypoints should prepare route runtime handoffs, then enter the shared scalar kernel spine",
    );

    let duplicate_input_builders = [
        ("materialized.rs", materialized.as_str()),
        ("streaming.rs", streaming.as_str()),
        ("entrypoints.rs", entrypoints.as_str()),
    ]
    .into_iter()
    .filter_map(|(file, source)| {
        source
            .contains("ExecutionInputs::new_prepared(")
            .then_some(file)
    })
    .collect::<Vec<_>>();

    assert!(
        duplicate_input_builders.is_empty(),
        "scalar terminal adapters should not rebuild ExecutionInputs outside execution.rs: {duplicate_input_builders:?}",
    );
}

#[test]
fn filter_authority_residual_contract_creation_stays_in_logical_semantics() {
    let logical_semantics_only =
        BTreeMap::from([("src/db/query/plan/semantics/logical.rs".to_string(), 1)]);

    assert_eq!(
        runtime_pattern_counts("ResidualFilterContract::new("),
        logical_semantics_only,
        "post-access residual filter contracts should be frozen only by logical planning semantics",
    );
    assert_eq!(
        runtime_pattern_counts("PredicatePushdownDiagnostics::from_plan("),
        logical_semantics_only,
        "predicate pushdown diagnostics should be projected from the same finalized logical plan",
    );
}

#[test]
fn planner_expr_projection_eval_does_not_canonicalize_or_import_predicates() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_path = crate_root.join("src/db/query/plan/expr/projection_eval.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = runtime_source_without_test_items(source.as_str());
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
