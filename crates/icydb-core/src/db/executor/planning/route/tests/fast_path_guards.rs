//! Module: db::executor::route::tests::fast_path_guards
//! Covers structural ownership guards for terminal fast-path derivation.
//! Does not own: production route or terminal execution behavior.
//! Boundary: enforces that terminal fast-path eligibility stays route-owned.

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

// Strip top-level `#[cfg(test)]` items from source text so ownership scans
// only reason about runtime paths.
fn strip_cfg_test_items(source: &str) -> String {
    let mut output = String::new();
    let mut pending_cfg_test = false;
    let mut skip_depth = 0usize;

    for line in source.lines() {
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
        let runtime_source = strip_cfg_test_items(source.as_str());
        if !runtime_source.contains(token) {
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
                "src/db/executor/prepared_execution_plan.rs".to_string(),
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
fn sql_count_routes_stay_on_shared_scalar_terminal_route() {
    let aggregate_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/session/sql/execute/aggregate.rs");
    let source = fs::read_to_string(&aggregate_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", aggregate_path.display()));

    assert!(
        source.contains("ScalarTerminalBoundaryRequest::Count"),
        "SQL count consumers should keep calling the shared scalar terminal boundary",
    );
    assert!(
        !source.contains("select_fields([authority.primary_key_name()])"),
        "SQL count consumers must not drift back to the structural projection-and-count detour",
    );
    assert!(
        source.contains("sql_count_field_uses_shared_count_terminal"),
        "SQL count-field routing should stay behind one explicit non-nullability guard helper",
    );
}
