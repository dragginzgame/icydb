//! Module: executor::aggregate::runtime::grouped_output
//! Responsibility: grouped row projection materialization and output finalization.
//! Does not own: grouped stream/fold execution orchestration.
//! Boundary: grouped output shaping + observability finalization helpers.

mod finalize;
mod projection;

pub(in crate::db::executor) use finalize::{
    GroupedOutputRuntimeObserverBindings, finalize_grouped_output_with_observer,
    finalize_path_outcome_for_path,
};
pub(in crate::db::executor) use projection::{
    project_grouped_rows_from_projection, project_grouped_values_from_projection,
};
