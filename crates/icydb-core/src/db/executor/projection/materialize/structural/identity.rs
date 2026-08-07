//! Module: db::executor::projection::materialize::structural::identity
//! Responsibility: model-identity structural projection specialization.
//! Does not own: general projection dispatch or DISTINCT semantics.
//! Boundary: bypasses expression projection only when the plan is identity.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{project_identity_data_row, project_slot_row},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionContract,
            row_view::RowView,
            structural::MaterializedProjectionRows,
        },
        terminal::RowLayout,
    },
    error::InternalError,
};

// Materialize model-identity projections straight from the structural scan
// payload. Raw data-row pages use the dense row decoder and retained-slot pages
// fall back to direct field movement when another caller still asks for slots.
pub(in crate::db::executor::projection::materialize::structural) fn project_identity_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionContract,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();
            let rows = slot_rows
                .into_iter()
                .map(|row| project_slot_row(prepared_projection, row).map(RowView::into_owned))
                .collect::<Result<Vec<_>, InternalError>>()?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();
            let rows = data_rows
                .iter()
                .map(|row| project_identity_data_row(&row_layout, row, metrics))
                .collect::<Result<Vec<_>, InternalError>>()?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
    )
}
