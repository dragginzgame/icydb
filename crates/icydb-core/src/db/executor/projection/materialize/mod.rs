//! Module: db::executor::projection::materialize
//! Responsibility: projection materialization module wiring.
//! Does not own: row loops, DISTINCT key storage, or structural page dispatch directly.
//! Boundary: exposes the materialization surface through owner-focused child modules.

mod contracts;
#[cfg(any(test, feature = "sql"))]
mod distinct;
#[cfg(any(test, feature = "sql"))]
mod execute;
#[cfg(any(test, feature = "sql"))]
mod metrics;
mod plan;
#[cfg(any(test, feature = "sql"))]
mod row_view;
#[cfg(any(test, feature = "sql"))]
mod structural;

#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor::projection) use distinct::ProjectionDistinctWindow;
#[cfg(test)]
pub(in crate::db::executor::projection) use execute::project_rows_from_projection;
#[cfg(test)]
pub(in crate::db::executor::projection) use execute::{
    count_borrowed_data_row_views_for_test, count_borrowed_identity_data_row_views_for_test,
    count_borrowed_slot_row_views_for_test,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use metrics::ProjectionMaterializationMetricsRecorder;
#[cfg(test)]
pub(in crate::db) use plan::PreparedProjectionPlan;
pub(in crate::db) use plan::{PreparedProjectionContract, prepare_projection_contract_from_plan};
pub(in crate::db::executor) use plan::{
    PreparedSlotProjectionValidation, ProjectionValidationRow, validate_prepared_projection_row,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) use structural::MaterializedProjectionRows;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use structural::project;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor::projection) use structural::project_distinct;
