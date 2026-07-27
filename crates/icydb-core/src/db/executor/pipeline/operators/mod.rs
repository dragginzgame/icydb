//! Module: db::executor::pipeline::operators
//! Responsibility: pipeline-owned execution operators shared by kernel/runtime entrypoints.
//! Does not own: route feasibility decisions or planner semantic validation.
//! Boundary: exports reusable operator contracts used by execution-kernel orchestration.

mod distinct;
mod terminal;

pub(in crate::db::executor) use distinct::decorate_resolved_execution_key_stream;
