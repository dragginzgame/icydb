use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

// Detect direct store-registry traversal hooks in source text.
fn source_uses_direct_store_or_registry_access(source: &str) -> bool {
    source.contains(".with_store(") || source.contains(".with_store_registry(")
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

// Strip top-level `#[cfg(test)]` items from source text using a lightweight
// brace-depth scanner so runtime-only guard scans ignore inline test modules.
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
fn load_module_has_no_direct_store_traversal() {
    let load_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load");
    let mut sources = Vec::new();
    collect_rust_sources(load_root.as_path(), &mut sources);
    sources.sort();

    for source_path in sources {
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        assert!(
            !source_uses_direct_store_or_registry_access(source.as_str()),
            "load module file {} must not directly traverse store/registry; route through resolver",
            source_path.display(),
        );
    }
}

#[test]
fn physical_module_has_no_direct_store_traversal() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/stream/access/physical.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));

    assert!(
        !source_uses_direct_store_or_registry_access(source.as_str()),
        "stream access physical resolver must request access via PrimaryScan/IndexScan adapters, not direct store handles",
    );
}

#[test]
fn executor_runtime_modules_have_no_raw_access_path_variant_matching() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

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
        if runtime_source.contains("AccessPath::") {
            violations.push(source_path);
        }
    }

    assert!(
        violations.is_empty(),
        "executor runtime modules must not pattern-match raw AccessPath variants; violations: {}",
        violations
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
}

#[test]
fn runtime_as_inner_calls_are_limited_to_boundary_adapters() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<String> = BTreeSet::new();
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
        if !runtime_source.contains(".as_inner(") {
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
        "runtime .as_inner() call sites must remain boundary-local; update allowlist only for intentional boundary changes",
    );
}

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
        violations
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
}

#[test]
fn runtime_route_capability_shims_are_not_reintroduced() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(executor_root.as_path(), &mut sources);
    sources.sort();

    let forbidden = [
        "supports_pk_stream_access_executable_path",
        "primary_scan_fetch_hint_for_executable_access_path",
        "secondary_extrema_probe_fetch_hint(",
        "aggregate_secondary_extrema_probe_fetch_hint",
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
        "executor runtime must consume direct capability snapshots instead of reintroducing route-capability shim helpers. Violations: {}",
        violations
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
}

#[test]
fn grouped_fold_runtime_uses_grouped_projection_consistency_contract() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load/grouped_fold/ingest.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan().scalar_plan().consistency"),
        "grouped fold runtime must consume grouped route-stage projection consistency contract instead of direct planner scalar-plan consistency reads",
    );
}

#[test]
fn grouped_distinct_runtime_uses_grouped_projection_consistency_contract() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load/grouped_distinct.rs");
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
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load/page.rs");
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
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/route/hints/mod.rs"),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/route/hints/load.rs"),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/route/hints/aggregate.rs"),
    ];
    let source = source_paths
        .iter()
        .map(|source_path| {
            fs::read_to_string(source_path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n");
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
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/kernel/reducer.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("plan.scalar_plan().consistency"),
        "kernel reducer runners must consume executor row-read consistency helper instead of direct scalar-plan consistency reads",
    );
}

#[test]
fn kernel_post_access_runtime_uses_projection_phase_gate_accessors() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/kernel/post_access/mod.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
    let runtime_source = strip_cfg_test_items(source.as_str());

    assert!(
        !runtime_source.contains("if cursor.is_some() && !self.plan.scalar_plan().mode.is_load()"),
        "kernel post-access cursor validation must consume post-access mode projection accessor",
    );
    assert!(
        !runtime_source.contains("let filtered = if self.plan.scalar_plan().predicate.is_some()"),
        "kernel post-access filter phase must consume post-access predicate projection accessor",
    );
    assert!(
        !runtime_source.contains("let logical = self.plan.scalar_plan();"),
        "kernel post-access phase gates must avoid repeated direct scalar-plan projection bindings",
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
