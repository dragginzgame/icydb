//! Module: db::executor::planning::route::tests::structural_guards
//! Covers structural guardrails for staged route planning ownership.
//! Does not own: production route planning logic outside this test module.
//! Boundary: verifies route-planner entry and assembly boundaries remain collapsed.

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
};

use std::{collections::BTreeSet, fs, path::Path};

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
        let runtime_source = runtime_source_without_test_items(source.as_str());
        let has_direct_constructor = runtime_source
            .lines()
            .any(|line| line.trim() == "ExecutionRoutePlan {");
        if !has_direct_constructor {
            continue;
        }

        let relative = relative_rust_source_path(crate_root, source_path.as_path());
        actual.insert(relative);
    }

    assert_eq!(
        actual, allowed,
        "direct ExecutionRoutePlan construction must stay inside staged planner assembly",
    );
}
