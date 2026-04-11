//! Module: db::executor::tests::continuation_structure
//! Covers continuation structure and cursor-shape invariants in executor
//! output.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

const CONTINUATION_POLICY_METHOD_REFERENCE_BASELINE_0432: usize = 3;
const CONTINUATION_POLICY_METHOD_REFERENCE_SOFT_BUDGET_DELTA: usize = 0;

const CONTINUATION_POLICY_METHOD_TOKENS: &[&str] = &[
    "requires_anchor(",
    "requires_strict_advance(",
    "is_grouped_safe(",
];

const EXECUTOR_FORBIDDEN_CONTINUATION_DEFINITION_FUNCTIONS: &[&str] = &[
    "continuation_advanced",
    "resume_bounds_from_refs",
    "validate_index_scan_continuation_envelope",
    "validate_index_scan_continuation_advancement",
    "next_cursor_for_materialized_rows",
    "effective_page_offset_for_window",
    "effective_keep_count_for_limit",
];

const CURSOR_SIGNATURE_VALIDATION_FACADE_TOKENS: &[&str] = &[
    "crate::db::cursor::validate_cursor_compatibility::<",
    "crate::db::cursor::revalidate_cursor::<",
    "crate::db::cursor::revalidate_grouped_cursor(",
];

const CURSOR_SIGNATURE_VALIDATION_INTERNAL_TOKENS: &[&str] = &[
    "crate::db::cursor::spine::",
    "crate::db::cursor::validation::",
    "cursor::spine::",
    "cursor::validation::",
];

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

// Count all direct continuation-policy method token references in one source.
fn continuation_policy_method_reference_count(source: &str) -> usize {
    CONTINUATION_POLICY_METHOD_TOKENS
        .iter()
        .map(|token| source.matches(token).count())
        .sum()
}

// Match one function definition token for both generic and non-generic signatures.
fn contains_function_definition(source: &str, function_name: &str) -> bool {
    let non_generic = format!("fn {function_name}(");
    let generic = format!("fn {function_name}<");

    source.contains(non_generic.as_str()) || source.contains(generic.as_str())
}

// Collect runtime continuation-policy method references for executor sources.
fn runtime_continuation_policy_reference_counts() -> BTreeMap<String, usize> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let mut counts = BTreeMap::new();
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
        let reference_count = continuation_policy_method_reference_count(runtime_source.as_str());
        if reference_count == 0 {
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
        counts.insert(relative, reference_count);
    }

    counts
}

fn runtime_forbidden_continuation_definitions() -> BTreeMap<String, Vec<String>> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let mut violations = BTreeMap::new();
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
        let mut matched = Vec::new();
        for function_name in EXECUTOR_FORBIDDEN_CONTINUATION_DEFINITION_FUNCTIONS {
            if contains_function_definition(runtime_source.as_str(), function_name) {
                matched.push((*function_name).to_string());
            }
        }
        if matched.is_empty() {
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
        violations.insert(relative, matched);
    }

    violations
}

fn runtime_cursor_signature_validation_facade_sites() -> BTreeMap<String, Vec<String>> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let mut sites = BTreeMap::new();
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
        let matched = CURSOR_SIGNATURE_VALIDATION_FACADE_TOKENS
            .iter()
            .filter(|token| runtime_source.contains(**token))
            .map(|token| (*token).to_string())
            .collect::<Vec<_>>();
        if matched.is_empty() {
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
        sites.insert(relative, matched);
    }

    sites
}

fn runtime_cursor_signature_validation_internal_references() -> BTreeMap<String, Vec<String>> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let mut references = BTreeMap::new();
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
        let matched = CURSOR_SIGNATURE_VALIDATION_INTERNAL_TOKENS
            .iter()
            .filter(|token| runtime_source.contains(**token))
            .map(|token| (*token).to_string())
            .collect::<Vec<_>>();
        if matched.is_empty() {
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
        references.insert(relative, matched);
    }

    references
}

#[test]
fn runtime_continuation_policy_method_references_stay_within_continuation_boundary() {
    let counts = runtime_continuation_policy_reference_counts();
    let allowed_prefix = "src/db/executor/continuation/";

    let violations = counts
        .keys()
        .filter(|relative| !relative.starts_with(allowed_prefix))
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "runtime ContinuationPolicy method references must stay continuation-boundary-local; violations: {}",
        violations.join(", "),
    );
}

#[test]
fn runtime_continuation_policy_method_reference_count_stays_within_soft_budget() {
    let counts = runtime_continuation_policy_reference_counts();
    let total_references = counts.values().copied().sum::<usize>();
    let max_references = CONTINUATION_POLICY_METHOD_REFERENCE_BASELINE_0432
        + CONTINUATION_POLICY_METHOD_REFERENCE_SOFT_BUDGET_DELTA;

    if max_references == 0 {
        assert_eq!(
            total_references, 0,
            "continuation-policy method fan-out exceeded zero-reference contract",
        );
    } else {
        assert!(
            total_references <= max_references,
            "continuation-policy method fan-out exceeded baseline budget: total={total_references}, max={max_references}",
        );
    }
}

#[test]
fn runtime_continuation_semantic_definitions_stay_cursor_owned() {
    let violations = runtime_forbidden_continuation_definitions();

    assert!(
        violations.is_empty(),
        "continuation semantic definitions must remain cursor-owned (`cursor/envelope.rs` + `cursor/continuation.rs`); executor duplicates found: {violations:?}",
    );
}

#[test]
fn runtime_cursor_signature_validation_entrypoints_stay_cursor_facade_owned() {
    let facade_sites = runtime_cursor_signature_validation_facade_sites();
    let actual = facade_sites.keys().cloned().collect::<BTreeSet<_>>();
    let expected =
        std::iter::once("src/db/query/plan/continuation.rs".to_string()).collect::<BTreeSet<_>>();

    assert_eq!(
        actual, expected,
        "cursor-signature validation entrypoints must remain centralized in cursor facade calls from query/plan continuation; actual sites: {facade_sites:?}",
    );

    let internal_references = runtime_cursor_signature_validation_internal_references();
    let has_internal_violations = internal_references
        .keys()
        .filter(|relative| !relative.starts_with("src/db/cursor/"))
        .any(|_| true);
    assert!(
        !has_internal_violations,
        "cursor-signature validation internals must stay cursor-owned; non-cursor references: {internal_references:?}",
    );
}
