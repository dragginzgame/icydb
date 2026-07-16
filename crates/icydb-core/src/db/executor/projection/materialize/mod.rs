//! Module: db::executor::projection::materialize
//! Responsibility: projection materialization module wiring.
//! Does not own: row loops, DISTINCT key storage, or structural page dispatch directly.
//! Boundary: exposes the materialization surface through owner-focused child modules.

mod contracts;
#[cfg(feature = "sql")]
mod distinct;
#[cfg(feature = "sql")]
mod execute;
#[cfg(feature = "sql")]
mod metrics;
mod plan;
#[cfg(feature = "sql")]
mod row_view;
#[cfg(feature = "sql")]
mod structural;

#[cfg(feature = "sql")]
pub(in crate::db::executor::projection) use distinct::ProjectionDistinctWindow;
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor::projection) use execute::project_rows_from_projection;
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor::projection) use execute::{
    count_borrowed_data_row_views_for_test, count_borrowed_identity_data_row_views_for_test,
    count_borrowed_slot_row_views_for_test,
};
#[cfg(feature = "sql")]
pub(in crate::db) use metrics::ProjectionMaterializationMetricsRecorder;
pub(in crate::db) use plan::{PreparedProjectionContract, prepare_projection_contract_from_plan};
pub(in crate::db::executor) use plan::{ProjectionValidationRow, validate_prepared_projection_row};
#[cfg(feature = "sql")]
pub(in crate::db) use structural::MaterializedProjectionRows;
#[cfg(feature = "sql")]
pub(in crate::db) use structural::project;
#[cfg(feature = "sql")]
pub(in crate::db::executor::projection) use structural::project_distinct;
