//! Module: db::executor::tests::load_structure
//! Covers structural load execution behavior and output shaping invariants.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn route_layer_does_not_compute_page_window_directly() {
    let route_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/planning/route");
    let files = collect_rs_files(&route_root);
    let mut offenders = Vec::new();

    for path in files {
        let source = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read route source {}: {err}", path.display());
        });
        if source.contains("compute_page_window(") {
            let relative = path
                .strip_prefix(&route_root)
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            offenders.push(relative);
        }
    }

    offenders.sort();
    assert!(
        offenders.is_empty(),
        "route layer must map projections to contracts; found direct window math in: {offenders:?}"
    );
}

#[test]
fn load_entrypoint_leaf_modules_do_not_resolve_continuation_directly() {
    let entrypoints_root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/pipeline/entrypoints");
    let files = collect_rs_files(&entrypoints_root);
    let mut offenders = Vec::new();

    for path in files {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name == "mod.rs" {
            continue;
        }

        let source = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "failed to read pipeline entrypoint source {}: {err}",
                path.display()
            );
        });
        if source.contains("LoadCursorResolver::resolve_") {
            let relative = path
                .strip_prefix(&entrypoints_root)
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            offenders.push(relative);
        }
    }

    offenders.sort();
    assert!(
        offenders.is_empty(),
        "leaf pipeline entrypoint modules must consume resolved continuation context; found direct continuation resolution in: {offenders:?}"
    );
}

#[test]
fn executor_layer_modules_do_not_import_forbidden_cross_layer_dependencies() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checks = [
        (
            "src/db/executor/aggregate",
            "db::executor::scan::",
            "aggregate layer must not import scan layer internals",
        ),
        (
            "src/db/executor/terminal",
            "db::executor::scan::",
            "terminal layer must not import scan layer internals",
        ),
        (
            "src/db/executor/terminal",
            "db::query::plan::",
            "terminal layer must not import planner contracts directly",
        ),
        (
            "src/db/executor/pipeline",
            "db::query::plan::",
            "pipeline layer must not import planner contracts directly",
        ),
        (
            "src/db/executor/scan",
            "db::executor::aggregate::",
            "scan layer must not import aggregate layer internals",
        ),
    ];

    let mut offenders = Vec::new();
    for (relative_root, pattern, error_message) in checks {
        let source_root = crate_root.join(relative_root);
        for path in collect_rs_files(&source_root) {
            if pattern == "db::query::plan::"
                && path
                    .to_string_lossy()
                    .contains("/src/db/executor/pipeline/contracts/")
            {
                continue;
            }
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            if !source.contains(pattern) {
                continue;
            }
            if source
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .any(|line| line.contains(pattern))
            {
                let relative = path
                    .strip_prefix(crate_root)
                    .unwrap_or(path.as_path())
                    .display()
                    .to_string();
                offenders.push(format!("{error_message}: {relative}"));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "executor layer import guardrails violated: {offenders:?}"
    );
}

fn collect_rs_files(root: &Path) -> Vec<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).unwrap_or_else(|err| {
            panic!("failed to read route directory {}: {err}", dir.display());
        });
        for entry in entries {
            let entry = entry.unwrap_or_else(|err| {
                panic!(
                    "failed to read route directory entry in {}: {err}",
                    dir.display()
                );
            });
            let path = entry.path();
            let file_type = entry.file_type().unwrap_or_else(|err| {
                panic!(
                    "failed to read route file type for {}: {err}",
                    path.display()
                );
            });
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if file_type.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }

    files
}
