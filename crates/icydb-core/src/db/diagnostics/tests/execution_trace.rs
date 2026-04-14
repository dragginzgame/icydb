//! Module: db::diagnostics::tests::execution_trace
//! Covers outward execution-trace metrics projection on the diagnostics
//! surface.
//! Does not own: execution-trace implementation details outside the published
//! diagnostics DTO surface.
//! Boundary: keeps diagnostics-surface regressions in the diagnostics owner
//! `tests/` boundary.

use crate::db::{
    access::AccessPlan,
    diagnostics::{ExecutionMetrics, ExecutionOptimization, ExecutionTrace},
    direction::Direction,
};

#[test]
fn execution_trace_metrics_projection_exposes_requested_surface() {
    let access = AccessPlan::by_key(11u64);
    let mut trace = ExecutionTrace::new(&access, Direction::Asc, false);
    trace.set_path_outcome(
        Some(ExecutionOptimization::PrimaryKey),
        5,
        3,
        2,
        42,
        true,
        true,
        7,
        9,
    );

    let metrics = trace.metrics();
    assert_eq!(
        metrics,
        ExecutionMetrics {
            rows_scanned: 5,
            rows_materialized: 3,
            execution_time_micros: 42,
            index_only: true,
        },
        "metrics projection must expose rows_scanned/rows_materialized/execution_time/index_only",
    );
    assert_eq!(
        trace.rows_returned(),
        2,
        "trace should preserve returned-row counters independently from materialization counters",
    );
}
