//! Module: db::executor::tests::load_structure
//! Responsibility: module-local ownership and contracts for db::executor::tests::load_structure.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::db::executor::pipeline::entrypoints::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

const LOAD_PIPELINE_STAGE_ARTIFACT_SOFT_BUDGET_DELTA: usize = 0;
const LOAD_PIPELINE_OPTIONAL_STAGE_SLOT_BASELINE_0250: usize = 0;

#[test]
fn load_pipeline_optional_stage_slots_stay_within_soft_delta() {
    let optional_slots = load_pipeline_state_optional_slot_count_guard();
    let max_slots = LOAD_PIPELINE_OPTIONAL_STAGE_SLOT_BASELINE_0250
        + LOAD_PIPELINE_STAGE_ARTIFACT_SOFT_BUDGET_DELTA;

    if max_slots == 0 {
        assert_eq!(
            optional_slots, 0,
            "load pipeline optional stage artifacts exceeded zero-slot contract; keep stage artifacts required-by-construction"
        );
    } else {
        assert!(
            optional_slots <= max_slots,
            "load pipeline optional stage artifacts exceeded baseline; split state into stage-local artifacts before adding slots"
        );
    }
}

#[test]
fn load_execute_stage_order_matches_linear_contract() {
    let stage_order = load_execute_stage_order_guard();

    assert_eq!(
        stage_order,
        [
            "build_execution_context",
            "execute_access_path",
            "apply_grouping_projection",
            "apply_paging",
            "apply_tracing",
            "materialize_surface",
        ],
        "load execute stage order changed; keep one linear orchestration spine and update the structural contract explicitly if this is intentional",
    );
}

#[test]
fn route_layer_does_not_compute_page_window_directly() {
    let route_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/route");
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
        if source.contains("ContinuationEngine::resolve_") {
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
fn executor_internal_stream_and_window_types_do_not_widen_to_pub_crate() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checks = [
        (
            "src/db/executor/stream/key/contracts.rs",
            "pub(crate) trait OrderedKeyStream",
        ),
        (
            "src/db/executor/stream/key/contracts.rs",
            "pub(crate) type OrderedKeyStreamBox",
        ),
        (
            "src/db/executor/stream/key/contracts.rs",
            "pub(crate) struct VecOrderedKeyStream",
        ),
        (
            "src/db/executor/stream/key/contracts.rs",
            "pub(crate) struct BudgetedOrderedKeyStream",
        ),
        (
            "src/db/executor/stream/key/composite.rs",
            "pub(crate) struct MergeOrderedKeyStream",
        ),
        (
            "src/db/executor/stream/key/composite.rs",
            "pub(crate) struct IntersectOrderedKeyStream",
        ),
        (
            "src/db/executor/stream/key/order.rs",
            "pub(crate) struct KeyOrderComparator",
        ),
        (
            "src/db/executor/pipeline/contracts/mod.rs",
            "pub(crate) struct CursorPage",
        ),
        (
            "src/db/executor/pipeline/contracts/mod.rs",
            "pub(crate) struct LoadExecutor",
        ),
        ("src/db/executor/mod.rs", "pub(crate) enum ExecutorError"),
        (
            "src/db/executor/pipeline/operators/post_access/mod.rs",
            "pub(crate) struct BudgetSafetyMetadata",
        ),
        (
            "src/db/executor/mutation/save.rs",
            "pub(crate) struct SaveExecutor",
        ),
    ];

    let mut offenders = Vec::new();
    for (relative_path, forbidden_pattern) in checks {
        let path = crate_root.join(relative_path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        if source.contains(forbidden_pattern) {
            offenders.push(format!("{relative_path} contains `{forbidden_pattern}`"));
        }
    }

    assert!(
        offenders.is_empty(),
        "executor-only key/window/error contracts must not widen to pub(crate): {offenders:?}"
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

#[test]
fn executor_legacy_load_module_directory_is_removed() {
    let load_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load");
    assert!(
        !load_root.exists(),
        "executor/load directory must remain removed after 0.49 stabilization work",
    );
}

#[test]
fn executor_shared_module_directory_is_removed() {
    let shared_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/shared");
    assert!(
        !shared_root.exists(),
        "executor/shared directory must remain removed after owner-named contract consolidation",
    );
}

#[test]
fn executor_modules_do_not_reference_shared_namespace() {
    let executor_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor");
    let mut offenders = Vec::new();
    for path in collect_rs_files(&executor_root) {
        if path.to_string_lossy().contains("/src/db/executor/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        if source.contains("executor::shared") {
            let relative = path
                .strip_prefix(&executor_root)
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            offenders.push(relative);
        }
    }

    offenders.sort();
    assert!(
        offenders.is_empty(),
        "executor modules must not reference deprecated executor::shared namespace: {offenders:?}"
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
