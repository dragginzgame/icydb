//! Module: db::executor::projection::materialize::structural::dispatch
//! Responsibility: non-DISTINCT structural projection page dispatch.
//! Does not own: identity specialization or DISTINCT windowing.
//! Boundary: selects slot-row vs data-row shaping and delegates row loops.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{project_data_row, project_slot_row},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionContract,
            row_view::RowView,
            structural::{MaterializedProjectionRows, identity::project_identity_page},
        },
        terminal::RowLayout,
    },
    error::InternalError,
};

pub(in crate::db) fn project(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionContract,
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
                .map(|row| {
                    project_data_row(&row_layout, prepared_projection, row, metrics)
                        .map(RowView::into_owned)
                })
                .collect::<Result<Vec<_>, InternalError>>()?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
    )
}
