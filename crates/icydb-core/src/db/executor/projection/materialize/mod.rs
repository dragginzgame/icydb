//! Module: db::executor::projection::materialize
//! Responsibility: projection materialization module wiring.
//! Does not own: row loops, DISTINCT key storage, or structural page dispatch directly.
//! Boundary: exposes the materialization surface through owner-focused child modules.

mod distinct;
mod execute;
mod metrics;
mod plan;
mod row_view;
mod structural;

#[cfg(test)]
pub(in crate::db::executor::projection) use execute::project_rows_from_projection;
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use execute::{
    count_borrowed_data_row_views_for_test, count_borrowed_identity_data_row_views_for_test,
    count_borrowed_slot_row_views_for_test,
};
pub(in crate::db) use metrics::ProjectionMaterializationMetricsRecorder;
#[cfg(test)]
pub(in crate::db) use plan::PreparedProjectionPlan;
pub(in crate::db) use plan::{PreparedProjectionShape, prepare_projection_shape_from_plan};
pub(in crate::db::executor) use plan::{
    PreparedSlotProjectionValidation, ProjectionValidationRow, validate_prepared_projection_row,
};
pub(in crate::db::executor) use structural::MaterializedProjectionRows;
pub(in crate::db) use structural::project;
pub(in crate::db::executor) use structural::project_distinct;
