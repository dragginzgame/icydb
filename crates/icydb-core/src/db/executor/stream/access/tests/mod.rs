//! Module: db::executor::stream::access::tests
//! Covers access-stream traversal behavior used by executor stream readers.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod boundary;
mod consistency;

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

// Detect direct store-registry traversal hooks in source text.
fn source_uses_direct_store_or_registry_access(source: &str) -> bool {
    source.contains(".with_store(") || source.contains(".with_store_registry(")
}

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

// Render one deterministic path list without materializing an intermediate Vec
// solely for separator joining in assertion messages.
fn join_display_paths(paths: &[PathBuf]) -> String {
    let mut joined = String::new();

    for path in paths {
        if !joined.is_empty() {
            joined.push_str(", ");
        }
        write!(&mut joined, "{}", path.display()).expect("writing to String should succeed");
    }

    joined
}

// Strip top-level `#[cfg(test)]` items from source text using a lightweight
// brace-depth scanner so runtime-only guard scans ignore inline test modules.
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
