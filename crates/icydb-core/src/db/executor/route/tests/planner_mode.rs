#[test]
fn load_fast_path_resolution_is_gated_by_route_execution_mode() {
    let execute_source = include_str!("../../load/execute.rs");

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
fn ranked_terminal_families_share_one_ranked_row_helper() {
    let terminal_source = include_str!("../../load/terminal/mod.rs");
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
    let terminal_source = include_str!("../../load/terminal/mod.rs");

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
    let aggregate_terminals_source = include_str!("../../load/aggregate/terminals.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let distinct_source = include_str!("../../load/aggregate/distinct.rs");
    let aggregate_contracts_source = include_str!("../../aggregate/contracts.rs");

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
        kernel_aggregate_source.contains("fn scalar_runner_eligible(spec: &AggregateSpec) -> bool"),
        "kernel aggregate orchestration must define one explicit scalar-runner eligibility gate",
    );
    assert!(
        kernel_aggregate_source.contains("spec.target_field().is_none()"),
        "kernel scalar-runner eligibility must exclude field-target aggregate specs",
    );
    assert!(
        kernel_aggregate_source.contains("Self::scalar_runner_eligible(&descriptor.spec)"),
        "kernel aggregate orchestration must gate reducer-runner usage through scalar eligibility",
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
fn aggregate_scalar_runner_remains_non_field_target_scalar_only() {
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let scalar_gate_start = kernel_aggregate_source
        .find("fn scalar_runner_eligible(spec: &AggregateSpec) -> bool {")
        .expect("scalar runner eligibility gate must exist in kernel aggregate orchestration");
    let scalar_gate_end = kernel_aggregate_source[scalar_gate_start..]
        .find("\n    // Execute one aggregate terminal via canonical materialized load execution.")
        .map(|offset| scalar_gate_start + offset)
        .expect(
            "scalar runner eligibility gate should be followed by materialized helper boundary",
        );
    let scalar_gate = &kernel_aggregate_source[scalar_gate_start..scalar_gate_end];

    assert!(
        scalar_gate.contains("spec.target_field().is_none()"),
        "scalar runner eligibility must exclude field-target aggregate specs",
    );
    assert!(
        scalar_gate.contains("AggregateKind::Count")
            && scalar_gate.contains("AggregateKind::Exists")
            && scalar_gate.contains("AggregateKind::Min")
            && scalar_gate.contains("AggregateKind::Max"),
        "scalar runner eligibility must include count/exists/min/max terminals",
    );
    assert!(
        !scalar_gate.contains("AggregateKind::First")
            && !scalar_gate.contains("AggregateKind::Last"),
        "scalar runner eligibility must not include first/last terminals",
    );

    assert_eq!(
        kernel_aggregate_source
            .matches("Self::run_low_risk_streaming_reducer(")
            .count(),
        1,
        "aggregate runner wiring should call low-risk reducer runner from one guarded streaming site only",
    );
    assert!(
        kernel_aggregate_source.contains("if Self::scalar_runner_eligible(&descriptor.spec)"),
        "aggregate runner callsite must stay gated by scalar eligibility",
    );
}

#[test]
fn aggregate_generic_streaming_fold_is_kernel_reducer_owned() {
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let kernel_reducer_source = include_str!("../../kernel/reducer.rs");
    let aggregate_contracts_source = include_str!("../../aggregate/contracts.rs");

    assert!(
        kernel_reducer_source.contains("fn run_streaming_aggregate_reducer<E>("),
        "kernel reducer module must define one generic aggregate reducer runner",
    );
    assert!(
        kernel_aggregate_source
            .matches("Self::run_streaming_aggregate_reducer(")
            .count()
            >= 2,
        "kernel aggregate orchestration should use reducer runner for routed stream folding and generic streaming fallback",
    );
    assert!(
        !kernel_aggregate_source.contains("LoadExecutor::<E>::fold_aggregate_over_key_stream("),
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
    let load_aggregate_root_source = include_str!("../../load/aggregate/mod.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let kernel_window_source = include_str!("../../kernel/window.rs");
    let aggregate_contracts_source = include_str!("../../aggregate/contracts.rs");

    assert!(
        kernel_aggregate_source.contains("fn prepare_aggregate_streaming_inputs<E>("),
        "aggregate execution must expose one shared kernel-owned streaming-input preparation helper",
    );
    assert!(
        !load_aggregate_root_source.contains("mod orchestration;"),
        "load aggregate module root should not keep a standalone orchestration wrapper module",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;")
            .count(),
        2,
        "kernel aggregate orchestration should call the shared preparation helper from both streaming branches",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("plan.index_prefix_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming spec extraction should be defined in one shared helper only",
    );
    assert_eq!(
        kernel_aggregate_source
            .matches("plan.index_range_specs()?.to_vec();")
            .count(),
        1,
        "aggregate streaming range-spec extraction should be defined in one shared helper only",
    );
    assert!(
        kernel_window_source.contains("fn effective_keep_count_for_limit<K>("),
        "cursor/window keep-count computation should be centralized in one kernel window helper",
    );
    assert!(
        !aggregate_contracts_source.contains("struct AggregateWindowState"),
        "aggregate contracts should not own window progression state once cursor/window policy is kernel-owned",
    );
}

#[test]
fn cursor_spine_validates_signature_direction_and_window_shape() {
    let cursor_spine_source = include_str!("../../cursor/spine.rs");

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
    let executor_mod_source = include_str!("../../mod.rs");
    let kernel_mod_source = include_str!("../../kernel/mod.rs");

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
    let kernel_mod_source = include_str!("../../kernel/mod.rs");
    let kernel_reducer_source = include_str!("../../kernel/reducer.rs");
    let kernel_aggregate_source = include_str!("../../kernel/aggregate.rs");
    let load_mod_source = include_str!("../../load/mod.rs");
    let load_execute_source = include_str!("../../load/execute.rs");

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
        !kernel_aggregate_source.contains("try_materialize_load_via_row_collector("),
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
