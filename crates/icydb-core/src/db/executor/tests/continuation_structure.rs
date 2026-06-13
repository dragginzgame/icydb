//! Module: db::executor::tests::continuation_structure
//! Covers continuation structure and cursor-shape invariants in executor
//! output.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
};

use std::{collections::BTreeMap, fs, path::Path};

const EXECUTOR_FORBIDDEN_CONTINUATION_DEFINITION_FUNCTIONS: &[&str] = &[
    "continuation_advanced",
    "resume_bounds_from_refs",
    "validate_index_scan_continuation_envelope",
    "validate_index_scan_continuation_advancement",
    "next_cursor_for_materialized_rows",
    "effective_page_offset_for_window",
    "effective_keep_count_for_limit",
];

const CURSOR_SIGNATURE_VALIDATION_INTERNAL_TOKENS: &[&str] = &[
    "crate::db::cursor::spine::",
    "crate::db::cursor::validation::",
    "cursor::spine::",
    "cursor::validation::",
];

// Match one function definition token for both generic and non-generic signatures.
fn contains_function_definition(source: &str, function_name: &str) -> bool {
    let non_generic = format!("fn {function_name}(");
    let generic = format!("fn {function_name}<");

    source.contains(non_generic.as_str()) || source.contains(generic.as_str())
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
        let runtime_source = runtime_source_without_test_items(source.as_str());
        let mut matched = Vec::new();
        for function_name in EXECUTOR_FORBIDDEN_CONTINUATION_DEFINITION_FUNCTIONS {
            if contains_function_definition(runtime_source.as_str(), function_name) {
                matched.push((*function_name).to_string());
            }
        }
        if matched.is_empty() {
            continue;
        }

        let relative =
            relative_rust_source_path(Path::new(env!("CARGO_MANIFEST_DIR")), source_path.as_path());
        violations.insert(relative, matched);
    }

    violations
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
        let runtime_source = runtime_source_without_test_items(source.as_str());
        let matched = CURSOR_SIGNATURE_VALIDATION_INTERNAL_TOKENS
            .iter()
            .filter(|token| runtime_source.contains(**token))
            .map(|token| (*token).to_string())
            .collect::<Vec<_>>();
        if matched.is_empty() {
            continue;
        }

        let relative =
            relative_rust_source_path(Path::new(env!("CARGO_MANIFEST_DIR")), source_path.as_path());
        references.insert(relative, matched);
    }

    references
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
fn runtime_cursor_signature_validation_internals_stay_cursor_owned() {
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
