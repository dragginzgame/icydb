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
