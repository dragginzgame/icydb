//! Module: db::executor::projection::materialize::structural::distinct_entrypoints
//! Responsibility: structural DISTINCT projection page entrypoints.
//! Does not own: projection expression evaluation or distinct key storage.
//! Boundary: adapts structural pages into bounded DISTINCT row collection.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            ProjectionDistinctWindow,
            distinct::collect_bounded_distinct_projected_rows,
            execute::{project_data_row, project_slot_row},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionShape,
            structural::MaterializedProjectionRows,
        },
        terminal::RowLayout,
    },
    error::InternalError,
};

#[cfg(feature = "sql")]
pub(in crate::db::executor::projection) fn project_distinct(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    window: ProjectionDistinctWindow,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    // Phase 1: choose the structural payload once, then run a bounded
    // DISTINCT projector over that shape. The projector owns the
    // post-projection window so it can stop when LIMIT has been satisfied.
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();

            collect_bounded_distinct_projected_rows(
                window,
                slot_rows,
                || metrics.record_distinct_candidate_row(),
                || metrics.record_distinct_bounded_stop(),
                |row| project_slot_row(prepared_projection, row),
            )
            .map(MaterializedProjectionRows::from_row_views)
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            collect_bounded_distinct_projected_rows(
                window,
                data_rows.iter(),
                || metrics.record_distinct_candidate_row(),
                || metrics.record_distinct_bounded_stop(),
                |row| project_data_row(row_layout, prepared_projection, row, metrics),
            )
            .map(MaterializedProjectionRows::from_row_views)
        },
    )
}
