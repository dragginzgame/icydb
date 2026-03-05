use super::*;
use crate::db::executor::load::{
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
    let optional_slots = load_pipeline_state_optional_slot_count_guard::<SimpleEntity>();
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
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/executor/load/entrypoints");
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
                "failed to read load entrypoint source {}: {err}",
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
        "leaf load entrypoint modules must consume resolved continuation context; found direct continuation resolution in: {offenders:?}"
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
