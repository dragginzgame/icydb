//! Module: db::executor::planning::route::tests::structural_guards
//! Covers structural guardrails for staged route planning ownership.
//! Does not own: production route planning logic outside this test module.
//! Boundary: verifies route-planner entry and assembly boundaries remain collapsed.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

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

// Strip top-level `#[cfg(test)]` items from source text so structural checks
// only reason about runtime route-planning paths.
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
fn route_planner_public_surface_has_single_route_builder() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let route_root = crate_root.join("src/db/executor/planning/route");
    let planner_mod = fs::read_to_string(route_root.join("planner/mod.rs"))
        .unwrap_or_else(|err| panic!("failed to read planner/mod.rs: {err}"));
    let route_mod = fs::read_to_string(route_root.join("mod.rs"))
        .unwrap_or_else(|err| panic!("failed to read route/mod.rs: {err}"));

    for source in [planner_mod.as_str(), route_mod.as_str()] {
        assert!(
            source.contains("build_execution_route_plan"),
            "route planning modules must re-export the single canonical route builder",
        );
        for forbidden in [
            "build_execution_route_plan_for_load",
            "build_execution_route_plan_for_mutation",
            "build_execution_route_plan_for_aggregate_spec",
            "build_execution_route_plan_for_grouped_plan",
        ] {
            assert!(
                !source.contains(forbidden),
                "route planning public surface must not re-export legacy per-family builders: {forbidden}",
            );
        }
    }
}

#[test]
fn execution_route_plan_is_only_built_from_staged_planner() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = crate_root.join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<String> =
        BTreeSet::from(["src/db/executor/planning/route/planner/stages.rs".to_string()]);
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
        let has_direct_constructor = runtime_source
            .lines()
            .any(|line| line.trim() == "ExecutionRoutePlan {");
        if !has_direct_constructor {
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
        actual.insert(relative);
    }

    assert_eq!(
        actual, allowed,
        "direct ExecutionRoutePlan construction must stay inside staged planner assembly",
    );
}
