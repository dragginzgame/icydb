//! Module: db::executor::pipeline::orchestrator::guards
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::orchestrator::guards.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::pipeline::{
        orchestrator::state::{LoadAccessState, LoadPayloadState},
        stages::load_stage_labels,
    },
    traits::EntityKind,
};

pub(in crate::db::executor) const fn load_execute_stage_order_guard() -> [&'static str; 6] {
    load_stage_labels()
}

pub(in crate::db::executor) fn load_pipeline_state_optional_slot_count_guard<E: EntityKind>()
-> usize {
    fn consume_access_state_shape<E: EntityKind>(state: LoadAccessState<E>) {
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

    let _ = consume_access_state_shape::<E> as fn(LoadAccessState<E>);
    let _ = consume_payload_state_shape as fn(LoadPayloadState);

    0
}
