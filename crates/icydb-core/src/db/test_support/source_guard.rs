//! Module: db::test_support::source_guard
//! Shared helpers for source-shape boundary tests.

use std::{
    fs,
    path::{Path, PathBuf},
};

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
        if path.is_dir() {
            collect_rust_sources(path.as_path(), out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

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
