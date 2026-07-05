//! Module: db::executor::planning::route::tests::fast_path_guards
//! Covers structural ownership guards for route fast-path derivation.
//! Does not own: production route or terminal execution behavior.
//! Boundary: enforces that fast-path eligibility and precedence stay route-owned.

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
};

use std::{collections::BTreeSet, fs, path::Path};

// Scan one source tree for a runtime token and collect the relative paths that
// still mention that token after test-only items are stripped.
fn runtime_token_hits(root: &Path, token: &str) -> BTreeSet<String> {
    let mut sources = Vec::new();
    collect_rust_sources(root, &mut sources);
    sources.sort();

    let mut hits = BTreeSet::new();
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
        if !runtime_source.contains(token) {
            continue;
        }

        let relative =
            relative_rust_source_path(Path::new(env!("CARGO_MANIFEST_DIR")), source_path.as_path());
        hits.insert(relative);
    }

    hits
}

#[test]
fn terminal_fast_path_derivation_stays_route_owned() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let cases = [
        (
            "derive_count_terminal_fast_path_contract_for_model(",
            BTreeSet::from([
                "src/db/executor/aggregate/terminals.rs".to_string(),
                "src/db/executor/planning/route/terminal.rs".to_string(),
            ]),
        ),
        (
            "derive_exists_terminal_fast_path_contract_for_model(",
            BTreeSet::from([
                "src/db/executor/aggregate/terminals.rs".to_string(),
                "src/db/executor/planning/route/terminal.rs".to_string(),
            ]),
        ),
        (
            "derive_load_terminal_fast_path_contract_for_plan(",
            BTreeSet::from([
                "src/db/executor/planning/route/planner/entrypoints.rs".to_string(),
                "src/db/executor/planning/route/terminal.rs".to_string(),
                "src/db/executor/prepared_execution_plan/snapshot.rs".to_string(),
            ]),
        ),
    ];

    for (token, allowed) in cases {
        let actual = runtime_token_hits(source_root.as_path(), token);
        assert_eq!(
            actual, allowed,
            "terminal fast-path derivation token `{token}` drifted outside the shared route owner boundary; update allowlist only for intentional boundary changes",
        );
    }
}

#[test]
fn stream_fast_path_precedence_stays_route_owned() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let cases = [
        (
            "try_first_verified_fast_path_hit(",
            BTreeSet::from([
                "src/db/executor/aggregate/fast_path.rs".to_string(),
                "src/db/executor/pipeline/runtime/fast_path/strategy.rs".to_string(),
            ]),
        ),
        (
            "load_fast_path_route_eligible(",
            BTreeSet::from([
                "src/db/executor/explain/descriptor/shared/mod.rs".to_string(),
                "src/db/executor/pipeline/runtime/fast_path/strategy.rs".to_string(),
            ]),
        ),
        (
            "FastPathResolutionStrategy::for_route(",
            BTreeSet::from(["src/db/executor/pipeline/runtime/fast_path/mod.rs".to_string()]),
        ),
    ];

    for (token, allowed) in cases {
        let actual = runtime_token_hits(source_root.as_path(), token);
        assert_eq!(
            actual, allowed,
            "stream fast-path precedence token `{token}` drifted outside the shared route owner boundary; update allowlist only for intentional boundary changes",
        );
    }
}
