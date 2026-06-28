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
    assert_source_contains_patterns(
        &source,
        &[
            "fn try_compile_canonical_bool_expr_to_compiled_predicate(",
            ") -> Option<PredicateCompilation>",
            "fn compile_normalized_bool_expr_to_predicate_impl(expr: &Expr) -> Option<Predicate>",
            "compile_bool_truth_sets(expr)?",
            "return None;",
        ],
        "predicate compilation should fail closed when runtime predicate admission and lowering drift",
    );
    assert_source_excludes_patterns(
        &runtime_source,
        &["unreachable!(", "predicate compiler invariant", ".expect("],
        "runtime predicate compilation must not trap on admitted-shape drift",
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
            "struct SqlWriteCandidateAccounting",
            "semantic_candidates: SqlWriteCandidateRows",
            "enum SqlWriteCandidateBoundCheck",
            "struct SqlWriteCandidateDiagnostics",
            "projected_source_rows: Option<SqlWriteProjectedSourceRows>",
            "struct SqlWriteCandidateBounds",
            "fn validate_at(",
            "struct SqlWriteCandidateCollection<K>",
            "struct SqlWriteMutationExecution<E>",
            "fn from_bounded_collection(",
            "SqlWriteCandidateBoundCheck::MutationBatchHandoff",
            "fn collect_bounded_sql_write_candidate_collection_from_structural_query",
            "record_projected_source_rows(",
            "SqlWriteCandidateBoundCheck::SelectorSourceBatch",
            "fn execute_sql_write_mutation_batch<E>(",
        ],
        "SQL UPDATE/INSERT staged-row admission should stay centralized in SqlWriteMutationExecution",
    );

    assert_source_contains_patterns(
        &update,
        &[
            "let candidate_bounds = sql_update_candidate_bounds(execution_bounds);",
            "collect_bounded_sql_write_candidate_collection_from_structural_query(",
            "candidate_bounds,",
            "SqlWriteMutationExecution::from_bounded_collection(",
        ],
        "SQL UPDATE should feed selector rows through bounded collection and the shared mutation batch bound",
    );

    assert_source_contains_patterns(
        &insert,
        &[
            "let candidate_bounds =",
            "sql_insert_candidate_bounds(execution_bounds, statement.returning.is_some())",
            "SqlWriteCandidateBoundCheck::InsertValuesSource",
            "execute_sql_insert_select_source_patches::<E>(",
            "collect_bounded_sql_write_candidate_collection_from_structural_query(",
            "SqlWriteMutationExecution::from_bounded_collection(",
        ],
        "SQL INSERT VALUES and INSERT SELECT should share candidate bounds through bounded source collection and the mutation batch handoff",
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
            "struct StructuralDeleteCandidateDiagnostics",
            "enum StructuralDeleteCandidateBoundCheck",
            "struct StructuralDeleteCandidateBounds",
            "struct StructuralDeleteCandidateCollection",
            "apply_delete_post_access_rows(prepared, &mut self.rows)?",
            "StructuralDeleteCandidateBoundCheck::PostAccessSelection",
            "diagnostics.selected_candidates",
            "package_rows(collection.into_rows())",
            "validate_structural_delete_projection_bounds(&prepared_projection.output, bounds)?",
            "validate_precommit(&prepared_projection.output)?",
            "prepare_structural_delete_count_core_with_optional_bounds(",
            "StructuralDeleteCandidateBoundCheck::FinalProjection",
        ],
        "structural DELETE count/RETURNING bounds should stay at the post-access candidate boundary, before packaging and commit",
    );
}

#[test]
fn sql_write_policy_validated_plan_helpers_are_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let write_policy = source_for(crate_root, "src/db/session/sql/write_policy.rs");
    let update_policy = source_for(crate_root, "src/db/session/sql/update_policy.rs");
    let delete_policy = source_for(crate_root, "src/db/session/sql/delete_policy.rs");

    assert_source_contains_patterns(
        &write_policy,
        &[
            "fn from_admitted_shape(",
            ") -> Option<Self> {",
            "shape.limit?",
        ],
        "bounded SQL write proof construction should stay fallible when the admitted-shape limit is absent",
    );
    assert_source_excludes_patterns(
        &write_policy,
        &[".expect(\"bounded policy admitted a limit\")"],
        "bounded SQL write proof construction must not trap on a missing limit",
    );

    for (policy_name, source) in [
        ("UPDATE", update_policy.as_str()),
        ("DELETE", delete_policy.as_str()),
    ] {
        assert_source_contains_patterns(
            source,
            &[
                "const fn validated_admission_lane(self) -> Option<SqlWriteAdmissionLane>",
                "fn validated_",
                "-> Option<SqlValidated",
                "const fn generated_policy_rejection(",
            ],
            &format!(
                "{policy_name} generated-policy and validated-plan helpers should stay fallible"
            ),
        );
        assert_source_excludes_patterns(
            source,
            &[
                "unreachable!(\"generated policies",
                "unreachable!(\"generated policies returned before shared checks\")",
            ],
            &format!(
                "{policy_name} generated-policy classification must reject or fail closed instead of trapping"
            ),
        );
    }
}

#[test]
fn aggregate_projection_terminal_value_selection_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let projection = source_for(crate_root, "src/db/executor/aggregate/projection/mod.rs");

    assert_source_contains_patterns(
        &projection,
        &[
            "fn terminal_value_from_covering_projection_pairs(",
            "AggregateKind::First =>",
            "AggregateKind::Last =>",
            "AggregateKind::Count\n        | AggregateKind::Sum\n        | AggregateKind::Avg\n        | AggregateKind::Exists\n        | AggregateKind::Min\n        | AggregateKind::Max => None,",
            "None",
        ],
        "covering aggregate terminal-value selection should fail closed for non-FIRST/LAST kinds",
    );
    assert_source_excludes_patterns(
        &projection,
        &["_ => unreachable!(),"],
        "covering aggregate terminal-value selection must not trap on non-FIRST/LAST kinds",
    );
}

#[test]
fn sql_execution_routing_fallbacks_are_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let execute = source_for(crate_root, "src/db/session/sql/execute/mod.rs");
    let explain = source_for(crate_root, "src/db/session/sql/execute/explain.rs");

    assert_source_contains_patterns(
        &execute,
        &[
            "CompiledSqlCommand::ShowMemory => Err(QueryError::execute(",
            "InternalError::query_executor_invariant()",
        ],
        "compiled SQL metadata/write routing drift should return a typed query error",
    );
    assert_source_excludes_patterns(
        &execute,
        &["unreachable!(\"metadata/write SQL handled above\")"],
        "compiled SQL metadata/write routing drift must not trap",
    );

    assert_source_contains_patterns(
        &explain,
        &[
            "SqlExplainMode::Execution => {",
            "return Err(QueryError::execute(",
            "InternalError::query_executor_invariant()",
        ],
        "SQL EXPLAIN PLAN/JSON routing drift should return a typed query error",
    );
    assert_source_excludes_patterns(
        &explain,
        &["unreachable!(\"execution explain is handled separately\")"],
        "SQL EXPLAIN PLAN/JSON routing drift must not trap",
    );
}

#[test]
fn query_intent_grouped_shape_lift_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let state = source_for(crate_root, "src/db/query/intent/state.rs");

    assert_source_contains_patterns(
        &state,
        &[
            "fn ensure_grouped_mut(",
            ") -> Option<&mut GroupedIntent<K>>",
            "return None;",
            "QueryShape::Scalar(_) => None,",
        ],
        "query intent grouped-shape lifting should stay recoverable when called from non-load state",
    );
    assert_source_excludes_patterns(
        &state,
        &[
            "panic!(\"query intent invariant\")",
            "unreachable!(\"shape checked above\")",
            "unreachable!(\"scalar shape lifted to grouped\")",
        ],
        "query intent grouped-shape lifting must not trap on mode or shape drift",
    );
}

#[test]
fn route_execution_stage_dispatch_is_exhaustive() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let execution = source_for(
        crate_root,
        "src/db/executor/planning/route/planner/execution/mod.rs",
    );

    assert_source_contains_patterns(
        &execution,
        &[
            "match route_shape_kind {",
            "RouteShapeKind::LoadScalar =>",
            "RouteShapeKind::MutationDelete =>",
            "RouteShapeKind::AggregateCount =>",
            "RouteShapeKind::AggregateNonCount =>",
            "RouteShapeKind::AggregateGrouped =>",
        ],
        "route execution-stage dispatch should stay exhaustive over route shape kinds",
    );
    assert_source_excludes_patterns(
        &execution,
        &["staged execution derivation only admits load and aggregate route shapes"],
        "route execution-stage dispatch must not trap on route-shape drift",
    );
}

#[test]
fn ordered_compare_range_planning_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let compare = source_for(crate_root, "src/db/query/plan/planner/compare.rs");

    assert_source_contains_patterns(
        &compare,
        &[
            "CompareOp::Gt =>",
            "CompareOp::Gte =>",
            "CompareOp::Lt =>",
            "CompareOp::Lte =>",
            "CompareOp::Eq\n            | CompareOp::Ne\n            | CompareOp::In\n            | CompareOp::NotIn\n            | CompareOp::Contains\n            | CompareOp::StartsWith\n            | CompareOp::EndsWith => return None,",
        ],
        "ordered compare range planning should fail closed for non-range operators",
    );
    assert_source_excludes_patterns(
        &compare,
        &["unreachable!(\"query planner invariant\")"],
        "ordered compare range planning must not trap on operator drift",
    );
}

#[test]
fn affine_numeric_compare_flip_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let affine = source_for(
        crate_root,
        "src/db/query/plan/expr/rewrite/affine_numeric.rs",
    );

    assert_source_contains_patterns(
        &affine,
        &[
            "const fn flip_compare_binary_op(op: BinaryOp) -> Option<BinaryOp>",
            "BinaryOp::Eq => Some(BinaryOp::Eq),",
            "BinaryOp::Gt => Some(BinaryOp::Lt),",
            "| BinaryOp::Div => None,",
            "&& let Some(flipped_op) = flip_compare_binary_op(compare_op)",
        ],
        "affine numeric compare flipping should fail closed for non-compare operator drift",
    );
    assert_source_excludes_patterns(
        &affine,
        &["unreachable!(\"only compare operators can be flipped\")"],
        "affine numeric compare flipping must not trap on non-compare operator drift",
    );
}

#[test]
fn scalar_count_reducer_output_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let reducer = source_for(
        crate_root,
        "src/db/executor/aggregate/contracts/state/reducer.rs",
    );

    assert_source_contains_patterns(
        &reducer,
        &[
            "Self::Count(value) => match finalize_count(u64::from(value)) {",
            "Value::Nat64(count) =>",
            "_ => ScalarAggregateOutput::Count(value),",
        ],
        "scalar COUNT reducer output should fall back to reducer-local count on finalization drift",
    );
    assert_source_excludes_patterns(
        &reducer,
        &["unreachable!(\"COUNT finalization must produce Nat\")"],
        "scalar COUNT reducer output must not trap on finalization-shape drift",
    );
}

#[test]
fn grouped_projection_aggregate_scan_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let group = source_for(crate_root, "src/db/query/plan/group.rs");

    assert_source_contains_patterns(
        &group,
        &[
            "fn planned_projection_layout_and_aggregate_specs_core(",
            ") -> Result<",
            "collect_grouped_projection_aggregate_scan(root_expr, &mut aggregate_specs)?",
            ") -> Result<GroupedAggregateExpressionScan, InternalError>",
            "extend_unique_grouped_aggregate_specs_from_expr(aggregate_specs, expr)",
        ],
        "grouped projection aggregate scanning should propagate traversal errors",
    );
    assert_source_excludes_patterns(
        &group,
        &[".expect(\"query group invariant\")"],
        "grouped projection aggregate scanning must not trap on traversal drift",
    );
}

#[test]
fn query_fingerprint_hashing_drift_paths_are_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let profile = source_for(
        crate_root,
        "src/db/query/fingerprint/hash_sections/profile.rs",
    );
    let having = source_for(
        crate_root,
        "src/db/query/fingerprint/hash_sections/grouping/having.rs",
    );

    assert_source_contains_patterns(
        &profile,
        &[
            "const MISSING_ENTITY_PATH_HASH_SENTINEL:",
            "fn hash_entity_path(",
            "entity_path.unwrap_or(MISSING_ENTITY_PATH_HASH_SENTINEL)",
        ],
        "query fingerprint profile hashing should use deterministic entity-path drift material",
    );
    assert_source_excludes_patterns(
        &profile,
        &[".expect(\"entity path required by hash profile\")"],
        "query fingerprint profile hashing must not trap when profile/entity-path wiring drifts",
    );

    assert_source_contains_patterns(
        &having,
        &[
            "const GROUP_HAVING_MISSING_SLOT_SENTINEL:",
            "write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);",
            "hash_missing_group_having_aggregate_expr(hasher, aggregate_expr);",
            "fn hash_missing_group_having_aggregate_expr(",
        ],
        "grouped HAVING fingerprint hashing should emit deterministic missing-slot material",
    );
    assert_source_excludes_patterns(
        &having,
        &[".expect(\"query fingerprint invariant\")"],
        "grouped HAVING fingerprint hashing must not trap on missing lookup facts",
    );
}

#[test]
fn sql_frontend_lowering_invariant_drift_paths_are_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cursor = source_for(crate_root, "src/db/sql_shared/cursor.rs");
    let analysis = source_for(crate_root, "src/db/sql/lowering/analysis.rs");
    let ast = source_for(crate_root, "src/db/query/plan/expr/ast.rs");
    let aggregate_semantics = source_for(crate_root, "src/db/sql/lowering/aggregate/semantics.rs");
    let aggregate_strategy = source_for(crate_root, "src/db/sql/lowering/aggregate/strategy.rs");
    let aggregate_terminal = source_for(crate_root, "src/db/sql/lowering/aggregate/terminal.rs");

    assert_source_contains_patterns(
        &cursor,
        &[
            "let found = self.peek_kind().cloned();",
            "token.kind = token_kind;",
            "return Err(SqlParseError::expected(",
        ],
        "SQL cursor token movers should restore mismatched tokens and return parse errors",
    );
    assert_source_excludes_patterns(
        &cursor,
        &["unreachable!(\"sql cursor invariant\")"],
        "SQL cursor token movers must not trap on token-kind drift",
    );

    assert_source_contains_patterns(
        &ast,
        &["pub(in crate::db) fn for_each_tree_expr(&self,"],
        "planner expressions should expose an infallible traversal for infallible consumers",
    );
    assert_source_contains_patterns(
        &analysis,
        &["expr.for_each_tree_expr(&mut |node| match node {"],
        "SQL lowering analysis should consume the infallible planner-expression traversal",
    );
    assert_source_excludes_patterns(
        &analysis,
        &[".expect(\"sql lowering invariant\")"],
        "SQL lowering analysis must not trap after an infallible traversal",
    );

    assert_source_contains_patterns(
        &aggregate_semantics,
        &[
            "fn try_from_kind_target_and_distinct(",
            ") -> Result<Self, SqlLoweringError>",
            "return Err(SqlLoweringError::unsupported_global_aggregate_projection());",
        ],
        "SQL aggregate semantic preparation should return a lowering error for unsupported kind drift",
    );
    assert_source_contains_patterns(
        &aggregate_strategy,
        &[
            "PreparedAggregateSemantics::try_from_kind_target_and_distinct(",
            ")?;",
        ],
        "SQL aggregate strategy should propagate aggregate semantic preparation drift",
    );
    assert_source_contains_patterns(
        &aggregate_terminal,
        &[
            "pub(in crate::db::sql::lowering::aggregate) fn count_rows() -> Self {",
            "input: LoweredAggregateInput::Rows,",
            "filter_expr: None,",
        ],
        "direct COUNT(*) lowering should build the known row-count terminal without a fallible round trip",
    );
    assert_source_excludes_patterns(
        &aggregate_semantics,
        &["unreachable!(\"sql aggregate invariant\")"],
        "SQL aggregate semantic preparation must not trap on unsupported kind drift",
    );
    assert_source_excludes_patterns(
        &aggregate_terminal,
        &[".expect(\"COUNT(*) is a supported global aggregate terminal\")"],
        "direct COUNT(*) lowering must not trap through a helper round trip",
    );
}

#[test]
fn scalar_predicate_lexer_and_key_codec_drift_paths_are_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert_scalar_predicate_runtime_drift_paths_are_recoverable(crate_root);
    assert_sql_lexer_drift_paths_are_recoverable(crate_root);
    assert_key_codec_drift_paths_are_recoverable(crate_root);
}

fn assert_scalar_predicate_runtime_drift_paths_are_recoverable(crate_root: &Path) {
    let predicate_runtime = source_for(crate_root, "src/db/predicate/runtime/mod.rs");

    assert_source_contains_patterns(
        &predicate_runtime,
        &[
            "fn eval_optional_scalar_slot(",
            "return Ok(false);",
            "eval_optional_scalar_text_contains(*field_slot, needle, TextMode::Ci, slots)",
        ],
        "scalar predicate runtime should fail closed when scalar admission/slot resolution drifts",
    );
    assert_source_excludes_patterns(
        &predicate_runtime,
        &["field_slot.expect(\"scalar predicate invariant\")"],
        "scalar predicate runtime must not trap on missing slot drift",
    );
}

fn assert_sql_lexer_drift_paths_are_recoverable(crate_root: &Path) {
    let lexer_scan = source_for(crate_root, "src/db/sql_shared/lexer/scan.rs");
    let lexer_token_body = source_for(crate_root, "src/db/sql_shared/lexer/token_body.rs");

    assert_source_contains_patterns(
        &lexer_scan,
        &[
            "other => Err(SqlParseError::invalid_syntax(",
            "SqlSyntaxErrorKind::UnexpectedCharacter { byte: other },",
        ],
        "SQL lexer comparison operator drift should return a parse error",
    );
    assert_source_excludes_patterns(
        &lexer_scan,
        &["unreachable!(\"sql lexer invariant\")"],
        "SQL lexer comparison operator drift must not trap",
    );

    assert_source_contains_patterns(
        &lexer_token_body,
        &[
            "let Some(high) = hex_nibble(pair[0]) else {",
            "let Some(low) = hex_nibble(pair[1]) else {",
            "SqlSyntaxErrorKind::BlobLiteralNonHexDigit,",
        ],
        "hex blob decoding should keep malformed nibbles on parse-error paths",
    );
    assert_source_excludes_patterns(
        &lexer_token_body,
        &[".expect(\"sql lexer invariant\")"],
        "hex blob decoding must not trap on malformed nibble drift",
    );
}

fn assert_key_codec_drift_paths_are_recoverable(crate_root: &Path) {
    let key_taxonomy = source_for(crate_root, "src/db/key_taxonomy.rs");
    let index_key_codec = source_for(crate_root, "src/db/index/key/codec/mod.rs");
    let index_key_tuple = source_for(crate_root, "src/db/index/key/codec/tuple.rs");

    assert_source_contains_patterns(
        &key_taxonomy,
        &[
            "PrimaryKeyKind::Composite => return Err(CompactPrimaryKeyDecodeError::NestedComposite),",
            "return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });",
            "map_err(|_| CompactStoreKeyEncodeError::IndexSegmentTooLarge)?;",
        ],
        "compact key taxonomy should route malformed decode/encode drift through typed errors",
    );
    assert_source_excludes_patterns(
        &key_taxonomy,
        &[
            "unreachable!(\"composite handled above\")",
            ".expect(\"primary-key invariant\")",
            ".expect(\"compact key segment fits in u16\")",
        ],
        "compact key taxonomy must not trap on malformed primary-key or segment-length drift",
    );

    assert_source_contains_patterns(
        &index_key_codec,
        &[
            "pub(crate) fn to_raw(&self) -> Result<RawIndexStoreKey, IndexKeyEncodeError>",
            "return Err(IndexKeyEncodeError::TooManyComponents);",
            "return Err(IndexKeyEncodeError::EmptySegment);",
            "return Err(IndexKeyEncodeError::SegmentTooLarge);",
            ".map_err(IndexKeyEncodeError::from)?;",
            "push_segment(&mut bytes, component)?;",
            "push_segment(&mut bytes, primary_key)?;",
        ],
        "raw index-key codec should route raw encoding drift through typed errors",
    );
    assert_source_contains_patterns(
        &index_key_tuple,
        &[
            "pub(super) fn push_segment(bytes: &mut Vec<u8>, segment: &[u8]) -> Result<(), IndexKeyEncodeError>",
            "return Err(IndexKeyEncodeError::EmptySegment);",
            "u16::try_from(segment.len()).map_err(|_| IndexKeyEncodeError::SegmentTooLarge)?;",
        ],
        "raw index-key tuple segments should encode fallibly",
    );
    assert_source_excludes_patterns(
        &index_key_codec,
        &[
            ".expect(\"index key invariant\")",
            ".expect(\"component count should fit in one byte\")",
            ".expect(\"primary-key invariant\")",
        ],
        "raw index-key codec must not trap on raw encode drift",
    );
    assert_source_excludes_patterns(
        &index_key_tuple,
        &[
            "assert!(segment.len() <= u16::MAX as usize, \"index tuple invariant\")",
            ".expect(\"index tuple invariant\")",
        ],
        "raw index-key tuple codec must not trap on segment length drift",
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
fn static_execution_planning_contract_access_is_recoverable() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let logical = source_for(crate_root, "src/db/query/plan/semantics/logical.rs");

    assert_source_contains_patterns(
        &logical,
        &[
            "const fn static_execution_planning_contract(&self) -> Option<&StaticExecutionPlanningContract>",
            "fn require_static_execution_planning_contract(",
            "Result<&StaticExecutionPlanningContract, InternalError>",
            ".ok_or_else(InternalError::query_executor_invariant)",
        ],
        "static execution planning metadata should expose optional and fallible accessors instead of panicking",
    );
    assert_source_excludes_patterns(
        &logical,
        &[".expect(\"query semantics invariant\")"],
        "static execution planning contract access must not trap through expect",
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
