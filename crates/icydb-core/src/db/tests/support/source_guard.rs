//! Module: db::tests::support::source_guard
//! Responsibility: source-file collection and cfg-test stripping for guard tests.
//! Does not own: production source parsing or formatting policy.
//! Boundary: supports db source-shape tests with filesystem-backed fixtures.

use std::{
    fs,
    path::{Path, PathBuf},
};

/// Identify external test source before applying runtime-only source guards.
#[must_use]
pub(in crate::db) fn source_is_test_only(source_path: &Path) -> bool {
    source_path.components().any(|part| {
        part.as_os_str()
            .to_str()
            .is_some_and(|name| name == "tests" || name.ends_with("_tests"))
    }) || source_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
}

#[test]
fn test_source_classification_preserves_runtime_neighbors() {
    for path in [
        "src/tests/case.rs",
        "src/tests.rs",
        "src/tests/mod.rs",
        "src/owner/fixture_tests/mod.rs",
        "src/owner/fixture_tests/helpers.rs",
        "src/owner/fixture_tests.rs",
    ] {
        assert!(source_is_test_only(Path::new(path)));
    }
    for path in [
        "src/contest.rs",
        "src/tests_runtime.rs",
        "src/owner/runtime.rs",
        "src/owner/fixture_tests_runtime/mod.rs",
    ] {
        assert!(!source_is_test_only(Path::new(path)));
    }
}

/// Collect all Rust source files below one root directory.
///
/// # Panics
///
/// Panics if the root directory or one of its entries cannot be read.
pub(in crate::db) fn collect_rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
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
        let metadata = fs::metadata(&path).unwrap_or_else(|err| {
            panic!("failed to inspect source entry {}: {err}", path.display())
        });
        if metadata.is_dir() {
            collect_rust_sources(path.as_path(), out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[cfg(unix)]
#[test]
fn source_discovery_refuses_missing_roots_and_unreadable_metadata() {
    let root = std::env::temp_dir().join(format!("icydb-source-guard-{}", std::process::id()));
    fs::create_dir(&root).expect("fresh source guard fixture");
    let missing = std::panic::catch_unwind(|| {
        collect_rust_sources(&root.join("missing"), &mut Vec::new());
    });
    std::os::unix::fs::symlink(root.join("absent-target"), root.join("broken.rs"))
        .expect("dangling source fixture");
    let metadata = std::panic::catch_unwind(|| {
        collect_rust_sources(&root, &mut Vec::new());
    });
    fs::remove_dir_all(root).expect("remove source guard fixture");
    assert!(missing.is_err());
    assert!(metadata.is_err());
}

/// Return a normalized relative Rust source path for guard diagnostics.
///
/// # Panics
///
/// Panics if `source_path` is not under `manifest_root`.
pub(in crate::db) fn relative_rust_source_path(manifest_root: &Path, source_path: &Path) -> String {
    source_path
        .strip_prefix(manifest_root)
        .unwrap_or_else(|err| {
            panic!(
                "failed to compute relative source path for {}: {err}",
                source_path.display()
            )
        })
        .to_string_lossy()
        .replace('\\', "/")
}

/// Remove direct `#[cfg(test)]` items from a Rust source string.
///
/// This lightweight scanner is for source-shape tests, not a general Rust
/// parser.
pub(in crate::db) fn runtime_source_without_test_items(source: &str) -> String {
    let mut output = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_cfg_test_item = false;
    let mut skip_depth = 0usize;

    for line in source.lines() {
        let trimmed = line.trim();
        if skip_depth > 0 {
            skip_depth = skip_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            continue;
        }
        if skipping_cfg_test_item {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
                skipping_cfg_test_item = skip_depth > 0;
            } else if trimmed.ends_with(';') {
                skipping_cfg_test_item = false;
            }
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
                skipping_cfg_test_item = skip_depth > 0;
            } else if !trimmed.ends_with(';') {
                skipping_cfg_test_item = true;
            }
            pending_cfg_test = false;
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}
