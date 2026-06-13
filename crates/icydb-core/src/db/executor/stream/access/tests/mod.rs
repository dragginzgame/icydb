//! Module: db::executor::stream::access::tests
//! Covers access-stream traversal behavior used by executor stream readers.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod boundary;
mod consistency;

use crate::db::test_support::source_guard::{
    collect_rust_sources, relative_rust_source_path,
    runtime_source_without_test_items as strip_cfg_test_items,
};

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
