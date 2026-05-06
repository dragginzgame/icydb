//! Module: executor::delete
//! Responsibility: delete-plan execution and commit-window handoff.
//! Does not own: logical planning, relation semantics, or cursor protocol details.
//! Boundary: delete-specific preflight/decode/apply flow over executable plans.

mod api;
mod commit;
mod runtime;
mod structural_projection;
mod typed;
mod types;

pub(in crate::db) use api::DeleteExecutor;
pub(in crate::db::executor::delete) use commit::{
    apply_delete_commit_window_for_type, prepare_delete_commit,
};
pub(in crate::db::executor::delete) use runtime::{
    apply_delete_post_access_rows, prepare_delete_runtime, resolve_delete_candidate_rows_as,
};
pub(in crate::db::executor::delete) use structural_projection::{
    execute_structural_delete_count_core, execute_structural_delete_projection_core,
};
pub(in crate::db::executor::delete) use typed::{
    package_typed_delete_rows, prepare_typed_delete_core,
};
pub(in crate::db) use types::DeleteProjection;
pub(in crate::db::executor) use types::DeleteRow;
