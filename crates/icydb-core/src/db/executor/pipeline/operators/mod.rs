//! Module: db::executor::pipeline::operators
//! Responsibility: pipeline-owned execution operators shared by kernel/runtime entrypoints.
//! Does not own: route feasibility decisions or planner semantic validation.
//! Boundary: exports reusable operator contracts used by execution-kernel orchestration.

mod distinct;
mod post_access;
mod reducer;
mod terminal;

pub(in crate::db::executor) use distinct::{
    decorate_key_stream_for_plan, decorate_resolved_execution_key_stream,
};
pub(in crate::db::executor) use post_access::PlanRow;
