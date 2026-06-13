//! Module: db::tests
//! Covers db-level structural guard behavior that spans multiple subsystems.
//! Does not own: runtime value conversion or storage encoding.
//! Boundary: keeps cross-subsystem source-shape invariants at the db root.

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
};

use std::{collections::BTreeMap, fs, path::Path};

fn runtime_surface_needles() -> [String; 2] {
    [
        ["Value", "Surface", "Encode"].concat(),
        ["Value", "Surface", "Decode"].concat(),
    ]
}

fn forbidden_runtime_surface_references() -> BTreeMap<String, Vec<String>> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let forbidden_prefixes = ["src/db/commit/", "src/db/data/", "src/db/index/"];
    let needles = runtime_surface_needles();
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

        let relative =
            relative_rust_source_path(Path::new(env!("CARGO_MANIFEST_DIR")), source_path.as_path());

        if !forbidden_prefixes
            .iter()
            .any(|prefix| relative.starts_with(prefix))
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());
        let matched = needles
            .iter()
            .filter(|needle| runtime_source.contains(needle.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        if !matched.is_empty() {
            violations.insert(relative, matched);
        }
    }

    violations
}

#[test]
fn runtime_surface_traits_stay_out_of_storage_boundaries() {
    let violations = forbidden_runtime_surface_references();

    assert!(
        violations.is_empty(),
        "runtime value-surface traits must not appear in persistence or storage encoding code; violations: {violations:?}",
    );
}
