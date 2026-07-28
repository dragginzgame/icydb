//! Module: db::session::sql::projection
//! Responsibility: session-owned SQL projection labels and response shaping
//! helpers used by SQL statement result construction.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: keeps outward SQL projection naming and SQL-specific row shaping
//! above the query-owned structural payload.

mod labels;
mod payload;
mod runtime;

#[cfg(feature = "sql-explain")]
pub(in crate::db::session::sql) use crate::db::session::sql::projection::labels::annotate_sql_projection_debug_on_execution_descriptor;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db::session::sql) use crate::db::session::sql::projection::runtime::execute_sql_projection_rows_for_canister_with_direct_data_row_attribution;
#[cfg(all(test, feature = "sql", not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::runtime::with_sql_projection_materialization_metrics;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use crate::db::session::sql::projection::runtime::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    payload::{
        sql_projection_statement_result_from_fallible_value_rows,
        sql_projection_statement_result_from_value_rows,
        sql_statement_result_from_structural_projection_payload,
    },
    runtime::execute_sql_projection_rows_for_canister,
    runtime::execute_sql_projection_rows_for_canister_with_scan_budget,
};
