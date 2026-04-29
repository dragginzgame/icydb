//! Module: db::executor::projection::materialize::structural::identity
//! Responsibility: model-identity structural projection specialization.
//! Does not own: general projection dispatch or DISTINCT semantics.
//! Boundary: bypasses expression projection only when the plan is identity.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{visit_identity_data_row_views, visit_slot_row_views},
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

            let mut rows = Vec::with_capacity(slot_rows.len());
            visit_slot_row_views(prepared_projection, slot_rows, |row_view| {
                rows.push(row_view.into_owned());

                Ok(())
            })?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            let mut rows = Vec::with_capacity(data_rows.len());
            visit_identity_data_row_views(row_layout, data_rows.as_slice(), metrics, |row_view| {
                rows.push(row_view.into_owned());

                Ok(())
            })?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
    )
}
