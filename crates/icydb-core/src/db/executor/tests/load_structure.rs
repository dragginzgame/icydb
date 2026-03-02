use super::*;
use crate::db::executor::load::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
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
