use super::*;

#[test]
fn grouped_order_limit_policy_symbols_remain_planner_owned() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

    let forbidden = [
        "GroupPlanError::OrderRequiresLimit",
        "GroupPlanError::OrderPrefixNotAlignedWithGroupKeys",
        "validate_group_cursor_constraints(",
    ];
    let mut violations = Vec::new();

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
        if forbidden
            .iter()
            .any(|symbol| runtime_source.contains(symbol))
        {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "grouped order/limit policy legality must remain planner-owned; executor runtime must consume projected contracts only. Violations: {}",
        join_display_paths(&violations),
    );
}

#[test]
fn grouped_fold_runtime_uses_grouped_projection_consistency_contract() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/db/executor/aggregate/runtime/grouped_fold/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan().scalar_plan().consistency"),
        "grouped fold runtime must consume grouped route-stage projection consistency contract instead of direct planner scalar-plan consistency reads",
    );
}

#[test]
fn grouped_fold_runtime_does_not_consult_grouped_plan_strategy_directly() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/db/executor/aggregate/runtime/grouped_fold/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("grouped_plan_strategy()"),
        "grouped fold runtime must consume planner-carried fold-path projection instead of direct grouped planner strategy access",
    );
}

#[test]
fn grouped_route_stage_does_not_carry_grouped_plan_strategy_after_projection() {
    for relative_path in [
        "src/db/executor/pipeline/contracts/grouped/route_stage/payload.rs",
        "src/db/executor/pipeline/contracts/grouped/route_stage/projection.rs",
    ] {
        let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path);
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = strip_cfg_test_items(source.as_str());

        assert!(
            !runtime_source.contains("grouped_plan_strategy"),
            "grouped route stage must carry execution-mechanical projection artifacts, not planner strategy, after grouped mode/fold-path projection: {}",
            source_path.display(),
        );
    }
}

#[test]
fn grouped_distinct_runtime_uses_grouped_projection_consistency_contract() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/db/executor/aggregate/runtime/grouped_distinct/aggregate.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan.scalar_plan().consistency"),
        "grouped DISTINCT runtime must consume grouped route-stage projection consistency contract instead of direct planner scalar-plan consistency reads",
    );
}

#[test]
fn load_page_materialization_uses_execution_input_consistency_projection() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/terminal/page/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan.scalar_plan().consistency"),
        "load page materialization must consume execution-input consistency projection instead of direct planner scalar-plan consistency reads",
    );
}

#[test]
fn aggregate_field_extrema_uses_prepared_input_consistency_projection() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/aggregate/field_extrema.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("prepared.logical_plan.scalar_plan().consistency"),
        "aggregate field-extrema runtime must consume prepared-input consistency projection instead of direct logical-plan consistency reads",
    );
}

#[test]
fn aggregate_fast_path_uses_projection_consistency_contract() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/aggregate/fast_path.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("inputs.logical_plan.scalar_plan().consistency"),
        "aggregate fast-path runtime must consume input projection consistency contract instead of direct logical-plan consistency reads",
    );
}

#[test]
fn aggregate_primary_key_fast_path_uses_route_budget_safety_filter_gate() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/aggregate/fast_path.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("if plan.scalar_plan().predicate.is_some()"),
        "aggregate primary-key fast-path gate must consume route-owned budget safety filter checks instead of direct scalar-plan predicate reads",
    );
}

#[test]
fn route_hints_use_route_window_and_budget_safety_filter_gates() {
    let source_paths = [
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/planning/route/hints/mod.rs"),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/planning/route/hints/load.rs"),
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/db/executor/planning/route/hints/aggregate.rs"),
    ];
    let mut source = String::new();
    for source_path in source_paths {
        if !source.is_empty() {
            source.push('\n');
        }
        source.push_str(
            fs::read_to_string(&source_path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()))
                .as_str(),
        );
    }
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("let page = plan.scalar_plan().page.as_ref()?;"),
        "route hinting must consume route-window projections instead of direct scalar-plan page reads for limit-pushdown hint gating",
    );
    assert!(
        !runtime_source.contains("plan.scalar_plan().predicate.is_some()"),
        "route hinting must consume route-owned budget-safety residual-filter checks instead of direct scalar-plan predicate reads",
    );
}

#[test]
fn delete_runtime_uses_executor_row_read_consistency_helper() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/delete/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan.scalar_plan().consistency"),
        "delete runtime must consume executor row-read consistency helper instead of direct scalar-plan consistency reads",
    );
}

#[test]
fn kernel_reducer_uses_executor_row_read_consistency_helper() {
    let source_root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/pipeline/operators/reducer");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

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
        assert!(
            !runtime_source.contains("plan.scalar_plan().consistency"),
            "pipeline reducer runners must consume executor row-read consistency helper instead of direct scalar-plan consistency reads; found in {}",
            source_path.display(),
        );
    }
}

#[test]
fn kernel_post_access_runtime_uses_projection_phase_gate_accessors() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/db/executor/pipeline/operators/post_access/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("if cursor.is_some() && !self.plan.scalar_plan().mode.is_load()"),
        "pipeline post-access cursor validation must consume post-access mode projection accessor",
    );
    assert!(
        !runtime_source.contains("let filtered = if self.plan.scalar_plan().predicate.is_some()"),
        "pipeline post-access filter phase must consume post-access predicate projection accessor",
    );
    assert!(
        !runtime_source.contains("let logical = self.plan.scalar_plan();"),
        "pipeline post-access phase gates must avoid repeated direct scalar-plan projection bindings",
    );
}

#[test]
fn runtime_scalar_plan_consistency_reads_stay_boundary_local() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<String> = std::iter::once("src/db/executor/traversal.rs")
        .map(str::to_string)
        .collect();

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
        if !runtime_source.contains("scalar_plan().consistency") {
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

    let unexpected = actual
        .iter()
        .filter(|path| !allowed.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        unexpected.is_empty(),
        "runtime scalar-plan consistency reads must remain boundary-local. Unexpected: {}",
        unexpected.join(", "),
    );
}
