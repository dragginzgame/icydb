//! Module: db::tests::ic_update_model
//! Covers source-shape guards for IC update-message execution assumptions.
//! Does not own: runtime scheduling, recovery, or commit behavior.
//! Boundary: prevents write/recovery paths from introducing async or reentry surfaces.

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
};

use std::{collections::BTreeMap, fs, path::Path};

const GUARDED_RUNTIME_PATHS: &[&str] = &[
    "src/db/commit/",
    "src/db/executor/delete/",
    "src/db/executor/mutation/",
    "src/db/session/sql/execute/write/",
    "src/db/session/write.rs",
];

const FORBIDDEN_SYNC_MODEL_NEEDLES: &[&str] = &[
    ".await",
    "async fn ",
    "async move",
    "async {",
    "async_std::",
    "ic_cdk::call",
    "ic_cdk::spawn",
    "spawn_local",
    "std::thread",
    "tokio::",
];

const TEST_ONLY_FAILPOINT_NEEDLES: &[&str] = &[
    "CommitFailpoint",
    "arm_commit_failpoint_for_tests",
    "clear_commit_failpoint_for_tests",
    "commit::failpoint",
    "hit_commit_failpoint",
    "mod failpoint;",
];

const TEST_HELPER_NEEDLES: &[&str] = &["_for_tests"];

fn path_is_guarded(relative_path: &str) -> bool {
    GUARDED_RUNTIME_PATHS.iter().any(|guarded_path| {
        if guarded_path.ends_with('/') {
            return relative_path.starts_with(guarded_path);
        }
        relative_path == *guarded_path
    })
}

fn source_is_test_only(source_path: &Path) -> bool {
    source_path
        .components()
        .any(|component| component.as_os_str() == "tests")
        || source_path
            .file_name()
            .is_some_and(|name| name == "tests.rs")
}

fn guarded_runtime_sources() -> Vec<(String, String)> {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = manifest_root.join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    sources
        .into_iter()
        .filter_map(|source_path| {
            if source_is_test_only(source_path.as_path()) {
                return None;
            }

            let relative = relative_rust_source_path(manifest_root, source_path.as_path());
            if !path_is_guarded(relative.as_str()) {
                return None;
            }

            let source = fs::read_to_string(source_path.as_path())
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
            let runtime_source = runtime_source_without_test_items(source.as_str());

            Some((relative, runtime_source))
        })
        .collect()
}

fn source_violations(
    sources: &[(String, String)],
    needles: &'static [&'static str],
) -> BTreeMap<String, Vec<&'static str>> {
    let mut violations = BTreeMap::new();
    for (relative, runtime_source) in sources {
        let matched = needles
            .iter()
            .copied()
            .filter(|needle| runtime_source.contains(needle))
            .collect::<Vec<_>>();

        if !matched.is_empty() {
            violations.insert(relative.clone(), matched);
        }
    }

    violations
}

fn synchronous_update_model_violations() -> BTreeMap<String, Vec<&'static str>> {
    source_violations(
        guarded_runtime_sources().as_slice(),
        FORBIDDEN_SYNC_MODEL_NEEDLES,
    )
}

fn production_failpoint_symbol_violations() -> BTreeMap<String, Vec<&'static str>> {
    let sources = guarded_runtime_sources()
        .into_iter()
        .filter(|(relative, _)| relative != "src/db/commit/failpoint.rs")
        .collect::<Vec<_>>();
    source_violations(sources.as_slice(), TEST_ONLY_FAILPOINT_NEEDLES)
}

fn production_test_helper_symbol_violations() -> BTreeMap<String, Vec<&'static str>> {
    let sources = guarded_runtime_sources()
        .into_iter()
        .filter(|(relative, _)| relative != "src/db/commit/failpoint.rs")
        .collect::<Vec<_>>();
    source_violations(sources.as_slice(), TEST_HELPER_NEEDLES)
}

#[test]
fn write_and_recovery_paths_stay_synchronous_for_ic_update_model() {
    let violations = synchronous_update_model_violations();

    assert!(
        violations.is_empty(),
        "commit, recovery, and write mutation paths must stay synchronous under the IC update-message model; violations: {violations:?}",
    );
}

#[test]
fn commit_failpoint_hooks_stay_test_only() {
    let violations = production_failpoint_symbol_violations();

    assert!(
        violations.is_empty(),
        "commit failpoint symbols must not survive outside cfg(test) runtime code; violations: {violations:?}",
    );
}

#[test]
fn write_and_recovery_test_helpers_stay_test_only() {
    let violations = production_test_helper_symbol_violations();

    assert!(
        violations.is_empty(),
        "test helper symbols must not survive outside cfg(test) write/recovery runtime code; violations: {violations:?}",
    );
}
