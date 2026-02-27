fn kernel_aggregate_mod_source() -> &'static str {
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/mod.rs"
    ))
}

fn kernel_aggregate_fast_path_source() -> &'static str {
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/fast_path.rs"
    ))
}

fn kernel_aggregate_field_extrema_source() -> &'static str {
    include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/field_extrema.rs"
    ))
}

#[test]
fn load_fast_path_resolution_is_gated_by_route_execution_mode() {
    let execute_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/execute.rs"
    ));

    assert!(
        execute_source.contains("match route_plan.execution_mode"),
        "load execution must branch on route-owned execution mode before fast-path evaluation",
    );
    assert!(
        execute_source.contains("ExecutionMode::Materialized => FastPathDecision::None"),
        "materialized load routes must bypass fast-path stream attempts",
    );
}

#[test]
fn load_trace_outcome_mapping_is_single_owner_boundary() {
    let load_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/mod.rs"
    ));

    assert_eq!(
        load_mod_source
            .matches("execution_trace.set_path_outcome(")
            .count(),
        1,
        "load trace outcome wiring should go through one set_path_outcome boundary",
    );
    assert!(
        load_mod_source.contains("execution_trace.keys_scanned,")
            && load_mod_source.contains("rows_scanned,")
            && load_mod_source
                .contains("execution trace keys_scanned must match rows_scanned metrics input"),
        "load trace wiring must keep keys_scanned aligned with rows_scanned metrics",
    );
}

#[test]
fn route_planner_remains_decomposed_into_dedicated_submodules() {
    let route_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../route/mod.rs"
    ));
    let planner_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../route/planner.rs"
    ));

    assert!(
        route_mod_source.contains("mod capability;")
            && route_mod_source.contains("mod fast_path;")
            && route_mod_source.contains("mod hints;")
            && route_mod_source.contains("mod mode;"),
        "route root must keep capability/fast_path/hints/mode decomposition",
    );
    assert!(
        planner_source.contains("Self::derive_route_capabilities("),
        "route planner root should delegate capability derivation to the capability module boundary",
    );
    assert!(
        planner_source.contains("Self::load_streaming_allowed("),
        "route planner root should delegate execution-mode branch predicates to the mode module boundary",
    );
}

#[test]
fn ranked_terminal_families_share_one_ranked_row_helper() {
    let terminal_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/terminal.rs"
    ));
    assert!(
        terminal_source.contains("fn rank_k_rows_from_materialized("),
        "ranked terminals must expose one shared ranked-row helper",
    );
    assert!(
        terminal_source.contains("Self::rank_k_rows_from_materialized("),
        "top/bottom terminal helpers must route through the shared ranked-row helper",
    );
    assert!(
        terminal_source.contains("RankedFieldDirection::Descending"),
        "top-k ranking must route through descending field direction",
    );
    assert!(
        terminal_source.contains("RankedFieldDirection::Ascending"),
        "bottom-k ranking must route through ascending field direction",
    );
}

#[test]
fn ranked_terminals_remain_materialized_without_heap_streaming_path() {
    let terminal_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/terminal.rs"
    ));

    assert!(
        terminal_source.contains("let response = self.execute(plan)?;"),
        "ranked terminals must run over canonical materialized execute() responses in 0.29",
    );
    assert!(
        !terminal_source.contains("BinaryHeap"),
        "0.29 must defer heap-streaming top-k optimization to preserve current ranking semantics",
    );
}

#[test]
fn aggregate_execution_mode_selection_is_route_owned_and_explicit() {
    let aggregate_terminals_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/terminals.rs"
    ));
    let kernel_aggregate_source = kernel_aggregate_mod_source();
    let distinct_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/distinct.rs"
    ));
    let aggregate_contracts_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/contracts.rs"
    ));

    assert!(
        kernel_aggregate_source.contains("build_execution_route_plan_for_aggregate_spec"),
        "aggregate execution mode must be derived by route planning",
    );
    assert!(
        kernel_aggregate_source.contains("AggregateReducerDispatch::from_descriptor(&descriptor)"),
        "aggregate orchestration must derive reducer dispatch from one descriptor boundary",
    );
    assert!(
        kernel_aggregate_source.contains("descriptor.route_plan.execution_mode"),
        "aggregate reducer dispatch must remain route-execution-mode driven",
    );
    assert!(
        kernel_aggregate_source.contains(
            "let (aggregate_output, keys_scanned) = Self::run_streaming_aggregate_reducer("
        ),
        "kernel aggregate orchestration must route streaming fold dispatch through one canonical reducer runner callsite",
    );
    assert!(
        aggregate_terminals_source.contains("ExecutionKernel::execute_aggregate_spec("),
        "aggregate terminal wrappers must delegate execution to kernel orchestration",
    );
    assert!(
        kernel_aggregate_source
            .contains("ExecutionKernel::execute_materialized_aggregate_spec(executor, plan, spec)"),
        "kernel aggregate orchestration should route materialized terminals through one shared helper boundary",
    );
    assert!(
        distinct_source.contains("let response = self.execute(plan)?;"),
        "count_distinct must run through canonical execute() orchestration",
    );
    assert!(
        !distinct_source.contains("build_execution_route_plan_for_load"),
        "count_distinct should not carry standalone route orchestration once unified",
    );
    assert!(
        !aggregate_contracts_source.contains("ExecutionMode"),
        "aggregate contracts must not own or branch on execution mode",
    );
}

#[test]
fn aggregate_streaming_runner_dispatch_removes_scalar_special_gates() {
    let kernel_aggregate_sources = [
        kernel_aggregate_mod_source(),
        kernel_aggregate_fast_path_source(),
        kernel_aggregate_field_extrema_source(),
    ];
    assert!(
        kernel_aggregate_sources
            .iter()
            .all(|source| !source
                .contains("fn scalar_runner_eligible(spec: &AggregateSpec) -> bool")),
        "kernel aggregate orchestration should not keep a scalar-only eligibility gate after reducer convergence",
    );
    assert!(
        kernel_aggregate_sources
            .iter()
            .all(|source| !source.contains("Self::run_low_risk_streaming_reducer(")),
        "kernel aggregate orchestration should not route through a low-risk-only reducer wrapper",
    );
    assert!(
        kernel_aggregate_sources
            .iter()
            .all(|source| !source.contains("if Self::scalar_runner_eligible(&descriptor.spec)")),
        "kernel aggregate streaming dispatch should not branch on scalar-only special gates",
    );
}

#[test]
fn aggregate_generic_streaming_fold_is_kernel_reducer_owned() {
    let kernel_aggregate_mod_source = kernel_aggregate_mod_source();
    let kernel_aggregate_fast_path_source = kernel_aggregate_fast_path_source();
    let kernel_reducer_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../kernel/reducer.rs"
    ));
    let aggregate_contracts_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/contracts.rs"
    ));

    assert!(
        kernel_reducer_source.contains("fn run_streaming_aggregate_reducer<E>("),
        "kernel reducer module must define one generic aggregate reducer runner",
    );
    assert!(
        kernel_aggregate_mod_source
            .matches("Self::run_streaming_aggregate_reducer(")
            .count()
            .saturating_add(
                kernel_aggregate_fast_path_source
                    .matches("Self::run_streaming_aggregate_reducer(")
                    .count(),
            )
            >= 2,
        "kernel aggregate orchestration should use reducer runner for routed stream folding and generic streaming fallback",
    );
    assert!(
        !kernel_aggregate_mod_source.contains("LoadExecutor::<E>::fold_aggregate_over_key_stream(")
            && !kernel_aggregate_fast_path_source
                .contains("LoadExecutor::<E>::fold_aggregate_over_key_stream("),
        "kernel aggregate orchestration must not call legacy generic fold helper directly",
    );
    assert!(
        !aggregate_contracts_source.contains("fn key_qualifies_for_fold("),
        "aggregate contract module should not host aggregate key-qualification execution helpers after kernel migration",
    );
    assert!(
        !aggregate_contracts_source.contains("fn row_exists_for_key("),
        "aggregate contract module should not host row-existence execution helpers after kernel migration",
    );
    assert!(
        !aggregate_contracts_source.contains("impl<E> LoadExecutor<E>"),
        "aggregate contract module should remain a contract/state module, not a load execution owner",
    );
}

#[test]
fn aggregate_streaming_paths_share_one_preparation_boundary() {
    let load_aggregate_root_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/mod.rs"
    ));
    let kernel_aggregate_mod_source = kernel_aggregate_mod_source();
    let kernel_aggregate_field_extrema_source = kernel_aggregate_field_extrema_source();
    let executor_window_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../window.rs"
    ));
    let aggregate_contracts_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../aggregate/contracts.rs"
    ));

    assert!(
        kernel_aggregate_mod_source.contains("fn prepare_aggregate_streaming_inputs<E>(")
            || kernel_aggregate_mod_source
                .contains("fn prepare_aggregate_streaming_inputs<'ctx, E>("),
        "aggregate execution must expose one shared kernel-owned streaming-input preparation helper",
    );
    assert!(
        !load_aggregate_root_source.contains("mod orchestration;"),
        "load aggregate module root should not keep a standalone orchestration wrapper module",
    );
    assert!(
        !load_aggregate_root_source.contains("type "),
        "load aggregate module root should stay wiring-only without local type contracts",
    );
    assert!(
        !load_aggregate_root_source.contains("impl<E> LoadExecutor<E>"),
        "load aggregate module root should not host aggregate executor impl blocks",
    );
    assert_eq!(
        kernel_aggregate_mod_source
            .matches("let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;")
            .count()
            .saturating_add(
                kernel_aggregate_field_extrema_source
                    .matches(
                        "let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;"
                    )
                    .count(),
            ),
        2,
        "kernel aggregate orchestration should call the shared preparation helper from both streaming branches",
    );
    assert_eq!(
        kernel_aggregate_mod_source
            .matches("plan.index_prefix_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming spec extraction should be defined in one shared helper only",
    );
    assert_eq!(
        kernel_aggregate_mod_source
            .matches("plan.index_range_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming range-spec extraction should be defined in one shared helper only",
    );
    assert!(
        executor_window_source.contains("fn effective_keep_count_for_limit<K>("),
        "cursor/window keep-count computation should be centralized in one executor window helper",
    );
    assert!(
        !aggregate_contracts_source.contains("struct AggregateWindowState"),
        "aggregate contracts should not own window progression state once cursor/window policy is kernel-owned",
    );
}

#[test]
fn cursor_spine_validates_signature_direction_and_window_shape() {
    let cursor_spine_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../../cursor/spine.rs"
    ));

    assert!(
        cursor_spine_source.contains(
            "validate_cursor_signature(entity_path, &expected_signature, &token.signature())"
        ),
        "cursor spine must validate continuation signatures before boundary materialization",
    );
    assert!(
        cursor_spine_source
            .contains("validate_cursor_direction(expected_direction, actual_direction)?;"),
        "cursor spine must validate cursor direction against executable direction",
    );
    assert!(
        cursor_spine_source.contains(
            "validate_cursor_window_offset(expected_initial_offset, actual_initial_offset)?;"
        ),
        "cursor spine must validate cursor window shape (initial offset) before boundary decode",
    );
}

#[test]
fn post_access_runtime_ownership_is_kernel_scoped() {
    let executor_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../mod.rs"
    ));
    let kernel_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../kernel/mod.rs"
    ));

    assert!(
        !executor_mod_source.contains("mod query_bridge;"),
        "executor module root should not expose a standalone query_bridge adapter layer",
    );
    assert!(
        kernel_mod_source.contains("mod post_access;"),
        "kernel module must own post-access runtime semantics",
    );
    assert!(
        kernel_mod_source.contains("use post_access::{PlanRow, PostAccessStats};"),
        "post-access row contracts should be re-exported from kernel ownership",
    );
}

#[test]
fn load_row_collector_runner_remains_kernel_ordered_and_single_owner() {
    let kernel_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../kernel/mod.rs"
    ));
    let kernel_reducer_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../kernel/reducer.rs"
    ));
    let kernel_aggregate_sources = [
        kernel_aggregate_mod_source(),
        kernel_aggregate_fast_path_source(),
        kernel_aggregate_field_extrema_source(),
    ];
    let load_mod_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/mod.rs"
    ));
    let load_execute_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/db/executor/tests/route/",
        "../../load/execute.rs"
    ));

    assert_eq!(
        kernel_reducer_source
            .matches("fn try_materialize_load_via_row_collector<")
            .count(),
        1,
        "kernel reducer module must define exactly one row-collector short-path helper",
    );
    assert_eq!(
        kernel_mod_source
            .matches("Self::try_materialize_load_via_row_collector(")
            .count(),
        2,
        "row-collector short path should be called only from primary and retry materialization branches",
    );
    assert!(
        kernel_aggregate_sources
            .iter()
            .all(|source| !source.contains("try_materialize_load_via_row_collector(")),
        "aggregate orchestration must not call load row-collector short-path helpers",
    );
    assert!(
        !load_mod_source.contains("try_materialize_load_via_row_collector("),
        "load module entrypoints must not bypass kernel-owned row-collector wiring",
    );
    assert!(
        !load_execute_source.contains("try_materialize_load_via_row_collector("),
        "load execution helpers must not bypass kernel-owned row-collector wiring",
    );

    // Order guard: the kernel must resolve/decorate execution streams before
    // invoking the row-collector short path in both primary and retry branches.
    let primary_resolve_position = kernel_mod_source.find(
        "let mut resolved =\n            Self::resolve_execution_key_stream(inputs, route_plan, predicate_compile_mode)?;",
    );
    let primary_row_runner_position =
        kernel_mod_source.find("Self::try_materialize_load_via_row_collector(");
    assert!(
        matches!(
            (primary_resolve_position, primary_row_runner_position),
            (Some(resolve_pos), Some(row_runner_pos)) if resolve_pos < row_runner_pos
        ),
        "primary load materialization must resolve + decorate stream before row-collector runner calls",
    );

    let retry_resolve_position =
        kernel_mod_source.find("let mut fallback_resolved = Self::resolve_execution_key_stream(");
    let retry_row_runner_position =
        kernel_mod_source.rfind("Self::try_materialize_load_via_row_collector(");
    assert!(
        matches!(
            (retry_resolve_position, retry_row_runner_position),
            (Some(resolve_pos), Some(row_runner_pos)) if resolve_pos < row_runner_pos
        ),
        "retry load materialization must resolve + decorate stream before row-collector runner calls",
    );
}
