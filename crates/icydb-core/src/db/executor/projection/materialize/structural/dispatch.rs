//! Module: db::executor::projection::materialize::structural::dispatch
//! Responsibility: non-DISTINCT structural projection page dispatch.
//! Does not own: identity specialization or DISTINCT windowing.
//! Boundary: selects slot-row vs data-row shaping and delegates row loops.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{project_data_rows, project_slot_rows},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionShape,
            structural::{MaterializedProjectionRows, identity::project_identity_page},
        },
        terminal::RowLayout,
    },
    error::InternalError,
};

#[cfg(feature = "sql")]
pub(in crate::db) fn project(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    if prepared_projection.projection_is_model_identity() {
        return project_identity_page(row_layout, prepared_projection, page, metrics);
    }

    // Phase 1: choose the structural payload once, then keep the row loop
    // inside the selected shaping path. Row views become `Vec<Vec<Value>>` only
    // at this structural boundary.
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();

            project_slot_rows(prepared_projection, slot_rows)
                .map(MaterializedProjectionRows::from_row_views)
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            project_data_rows(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
                metrics,
            )
            .map(MaterializedProjectionRows::from_row_views)
        },
    )
}
