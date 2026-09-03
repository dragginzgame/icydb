//! Module: db::executor::projection::materialize::structural::dispatch
//! Responsibility: non-DISTINCT structural projection page dispatch.
//! Does not own: identity specialization or DISTINCT windowing.
//! Boundary: selects slot-row vs data-row shaping and delegates row loops.

use crate::{
    db::executor::{
        ProductionScalarOutputWork, StructuralCursorPage,
        order::{cursor_boundary_from_data_row, cursor_boundary_from_orderable_row},
        projection::materialize::{
            execute::{project_data_row, project_slot_row},
            plan::PreparedProjectionContract,
            row_view::RowView,
            structural::{MaterializedProjectionRows, identity::project_identity_page},
        },
        terminal::RowLayout,
    },
    db::{cursor::CursorBoundary, query::plan::ResolvedOrder},
    error::InternalError,
};
use std::cell::RefCell;

/// One cursor-page projection admitted row by row against the page envelope.
pub(in crate::db::executor::projection) struct AdmittedProjectionPage {
    rows: MaterializedProjectionRows,
    last_emitted_logical: Option<CursorBoundary>,
    has_more: bool,
}

impl AdmittedProjectionPage {
    pub(in crate::db::executor::projection) fn into_parts(
        self,
    ) -> (MaterializedProjectionRows, Option<CursorBoundary>, bool) {
        (self.rows, self.last_emitted_logical, self.has_more)
    }
}

pub(in crate::db) fn project(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionContract,
    page: StructuralCursorPage,
) -> Result<MaterializedProjectionRows, InternalError> {
    if prepared_projection.projection_is_model_identity() {
        return project_identity_page(row_layout, prepared_projection, page);
    }

    // Phase 1: choose the structural payload once, then keep the row loop
    // inside the selected shaping path. Row views become `Vec<Vec<Value>>` only
    // at this structural boundary.
    page.consume_projection_rows(
        |slot_rows| {
            let rows = slot_rows
                .into_iter()
                .map(|row| project_slot_row(prepared_projection, row).map(RowView::into_owned))
                .collect::<Result<Vec<_>, InternalError>>()?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
        |data_rows| {
            let rows = data_rows
                .iter()
                .map(|row| {
                    project_data_row(&row_layout, prepared_projection, row).map(RowView::into_owned)
                })
                .collect::<Result<Vec<_>, InternalError>>()?;

            Ok(MaterializedProjectionRows::from_value_rows(rows))
        },
    )
}

/// Project and admit one scalar page a row at a time.
///
/// The first row that cannot enter the remaining page envelope is discarded
/// and left beyond the returned logical boundary, so resume cannot skip it.
pub(in crate::db::executor::projection) fn project_admitted_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionContract,
    page: StructuralCursorPage,
    resolved_order: Option<&ResolvedOrder>,
    row_limit: Option<usize>,
    output_work: &mut ProductionScalarOutputWork,
) -> Result<AdmittedProjectionPage, InternalError> {
    let output_work = RefCell::new(output_work);
    page.consume_projection_rows(
        |slot_rows| {
            let source_has_more = row_limit.is_some_and(|limit| slot_rows.len() >= limit);
            let mut rows = Vec::new();
            let mut last_emitted_logical = None;
            let mut output_stopped = false;
            for row in slot_rows.into_iter().take(row_limit.unwrap_or(usize::MAX)) {
                let boundary =
                    resolved_order.map(|order| cursor_boundary_from_orderable_row(&row, order));
                let projected = project_slot_row(prepared_projection, row)?;
                if !output_work.borrow_mut().admit_row(projected.values())? {
                    output_stopped = true;
                    break;
                }
                last_emitted_logical = boundary;
                rows.push(projected.into_owned());
            }

            Ok(AdmittedProjectionPage {
                rows: MaterializedProjectionRows::from_value_rows(rows),
                last_emitted_logical,
                has_more: source_has_more || output_stopped,
            })
        },
        |data_rows| {
            let source_has_more = row_limit.is_some_and(|limit| data_rows.len() >= limit);
            let mut rows = Vec::new();
            let mut last_emitted_logical = None;
            let mut output_stopped = false;
            for row in data_rows.into_iter().take(row_limit.unwrap_or(usize::MAX)) {
                let boundary = resolved_order
                    .map(|order| cursor_boundary_from_data_row(&row, &row_layout, order))
                    .transpose()?;
                let projected = project_data_row(&row_layout, prepared_projection, &row)?;
                if !output_work.borrow_mut().admit_row(projected.values())? {
                    output_stopped = true;
                    break;
                }
                last_emitted_logical = boundary;
                rows.push(projected.into_owned());
            }

            Ok(AdmittedProjectionPage {
                rows: MaterializedProjectionRows::from_value_rows(rows),
                last_emitted_logical,
                has_more: source_has_more || output_stopped,
            })
        },
    )
}
