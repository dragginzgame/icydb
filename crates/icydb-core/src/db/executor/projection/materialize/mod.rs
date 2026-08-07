//! Module: db::executor::projection::materialize
//! Responsibility: projection materialization module wiring.
//! Does not own: row loops, DISTINCT key storage, or structural page dispatch directly.
//! Boundary: exposes the materialization surface through owner-focused child modules.

mod contracts;
mod distinct;
mod execute;
mod metrics;
mod plan;
mod row_view;
mod structural;

pub(in crate::db::executor::projection) use distinct::{
    ProjectionDistinctStrategy, ProjectionDistinctWindow, projection_distinct_strategy,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use metrics::DistinctProjectionMetricsRecorder;
pub(in crate::db) use metrics::ProjectionMaterializationMetricsRecorder;
pub(in crate::db) use plan::{PreparedProjectionContract, prepare_projection_contract_from_plan};
pub(in crate::db::executor) use plan::{ProjectionValidationRow, validate_prepared_projection_row};
pub(in crate::db) use structural::MaterializedProjectionRows;
pub(in crate::db) use structural::project;
pub(in crate::db::executor::projection) use structural::project_distinct;
