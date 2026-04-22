//! Module: db::session::sql::projection
//! Responsibility: session-owned SQL projection labels and payload shaping
//! helpers used by SQL statement result construction.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: keeps outward SQL projection naming, payload types, and
//! SQL-specific row shaping together.

mod labels;
mod payload;
mod runtime;

#[cfg(all(test, feature = "sql", not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::runtime::with_sql_projection_materialization_metrics;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use crate::db::session::sql::projection::runtime::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use crate::db::session::sql::projection::runtime::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    labels::{
        annotate_sql_projection_debug_on_execution_descriptor,
        projection_fixed_scales_from_projection_spec, projection_labels_from_fields,
        projection_labels_from_projection_spec, sql_projection_rows_from_kernel_rows,
    },
    payload::SqlProjectionPayload,
    runtime::execute_sql_projection_rows_for_canister,
};
