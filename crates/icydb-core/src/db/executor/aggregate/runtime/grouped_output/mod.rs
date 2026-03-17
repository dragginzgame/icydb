//! Module: executor::aggregate::runtime::grouped_output
//! Responsibility: grouped row projection materialization and output finalization.
//! Does not own: grouped stream/fold execution orchestration.
//! Boundary: grouped output shaping + observability finalization helpers.

mod finalize;
mod projection;

pub(in crate::db::executor) use projection::{
    aggregate_output_to_value, project_grouped_row_from_projection,
    project_grouped_rows_from_projection,
};
