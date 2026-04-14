//! Module: db::session::sql::projection
//! Responsibility: session-owned SQL projection labels and payload shaping
//! helpers used by SQL statement result construction.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: keeps outward SQL projection naming, payload types, and
//! SQL-specific row shaping together.

mod labels;
mod payload;
mod runtime;

#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
pub use crate::db::session::sql::projection::runtime::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub(in crate::db) use crate::db::session::sql::projection::runtime::{
    SqlProjectionTextExecutorAttribution, attribute_sql_projection_text_rows_for_canister,
};
pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    labels::{
        annotate_sql_projection_labels_on_execution_descriptor,
        projection_fixed_scales_from_projection_spec, projection_labels_from_fields,
        projection_labels_from_projection_spec, sql_projection_rows_from_kernel_rows,
    },
    payload::{SqlProjectionPayload, grouped_sql_statement_result},
    runtime::execute_sql_projection_rows_for_canister,
};
