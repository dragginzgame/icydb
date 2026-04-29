//! Module: db::executor::projection::materialize::structural::identity
//! Responsibility: model-identity structural projection specialization.
//! Does not own: general projection dispatch or DISTINCT semantics.
//! Boundary: bypasses expression projection only when the plan is identity.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{project_identity_data_rows, project_slot_rows},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionShape,
            structural::MaterializedProjectionRows,
        },
        terminal::RowLayout,
    },
    error::InternalError,
};

// Materialize model-identity projections straight from the structural scan
// payload. Raw data-row pages use the dense row decoder and retained-slot pages
// fall back to direct field movement when another caller still asks for slots.
#[cfg(feature = "sql")]
pub(in crate::db::executor::projection::materialize::structural) fn project_identity_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();

            project_slot_rows(prepared_projection, slot_rows)
                .map(MaterializedProjectionRows::from_row_views)
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            project_identity_data_rows(row_layout, data_rows.as_slice(), metrics)
                .map(MaterializedProjectionRows::from_row_views)
        },
    )
}
