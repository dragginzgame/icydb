//! Module: db::executor::pipeline::orchestrator::guards
//! Defines guard checks that reject invalid pipeline orchestrator setups.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(test)]
use crate::db::executor::pipeline::orchestrator::state::{LoadAccessState, LoadPayloadState};

#[cfg(test)]
pub(in crate::db::executor) const fn load_execute_stage_order_guard() -> [&'static str; 6] {
    [
        "build_execution_context",
        "execute_access_path",
        "apply_grouping_projection",
        "apply_paging",
        "apply_tracing",
        "materialize_surface",
    ]
}

#[cfg(test)]
pub(in crate::db::executor) const fn load_pipeline_state_optional_slot_count_guard() -> usize {
    fn consume_access_state_shape(state: LoadAccessState) {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let _ = (context, access_inputs);
    }

    fn consume_payload_state_shape(state: LoadPayloadState) {
        let LoadPayloadState {
            context,
            payload,
            trace,
        } = state;
        let _ = (context, payload, trace);
    }

    let _ = consume_access_state_shape as fn(LoadAccessState);
    let _ = consume_payload_state_shape as fn(LoadPayloadState);

    0
}
