//! Module: db::executor::projection::materialize::structural::dispatch
//! Responsibility: non-DISTINCT structural projection page dispatch.
//! Does not own: identity specialization or DISTINCT windowing.
//! Boundary: selects slot-row vs data-row shaping and delegates row loops.

use crate::{
    db::executor::{
        StructuralCursorPage,
        projection::materialize::{
            execute::{visit_data_row_views, visit_slot_row_views},
            metrics::ProjectionMaterializationMetricsRecorder,
            plan::PreparedProjectionContract,
            structural::{
                MaterializedProjectionRows, RowViewCollector, identity::project_identity_page,
            },
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

            let mut collector = RowViewCollector::with_capacity(slot_rows.len());
            visit_slot_row_views(prepared_projection, slot_rows, |row_view| {
                collector.push(row_view);

                Ok(())
            })?;

            Ok(collector.finish())
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            let mut collector = RowViewCollector::with_capacity(data_rows.len());
            visit_data_row_views(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
                metrics,
                |row_view| {
                    collector.push(row_view);

                    Ok(())
                },
            )?;

            Ok(collector.finish())
        },
    )
}
