//! Module: db::executor::projection::materialize
//! Responsibility: projection materialization module wiring.
//! Does not own: row loops, DISTINCT key storage, or structural page dispatch directly.
//! Boundary: exposes the materialization surface through owner-focused child modules.

mod contracts;
#[cfg(feature = "query")]
mod distinct;
#[cfg(feature = "query")]
mod execute;
#[cfg(feature = "query")]
mod metrics;
mod plan;
#[cfg(feature = "query")]
mod row_view;
#[cfg(feature = "query")]
mod structural;

#[cfg(feature = "query")]
pub(in crate::db::executor::projection) use distinct::ProjectionDistinctWindow;
#[cfg(feature = "query")]
pub(in crate::db) use metrics::ProjectionMaterializationMetricsRecorder;
pub(in crate::db) use plan::{PreparedProjectionContract, prepare_projection_contract_from_plan};
pub(in crate::db::executor) use plan::{ProjectionValidationRow, validate_prepared_projection_row};
#[cfg(feature = "query")]
pub(in crate::db) use structural::MaterializedProjectionRows;
#[cfg(feature = "query")]
pub(in crate::db) use structural::project;
#[cfg(feature = "query")]
pub(in crate::db::executor::projection) use structural::project_distinct;
