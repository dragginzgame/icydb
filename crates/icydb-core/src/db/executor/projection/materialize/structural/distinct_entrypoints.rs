//! Module: db::executor::projection::materialize::structural::distinct_entrypoints
//! Responsibility: structural DISTINCT projection page entrypoints.
//! Does not own: projection expression evaluation or distinct key storage.
//! Boundary: adapts structural rows and canonical source boundaries into one DISTINCT strategy.

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ProductionScalarOutputWork, StructuralCursorPage,
            order::{cursor_boundary_from_data_row, cursor_boundary_from_orderable_row},
            projection::materialize::{
                ProjectionDistinctStrategy, ProjectionDistinctWindow,
                distinct::{DistinctProjectedRow, collect_distinct_projected_rows},
                execute::{project_data_row, project_slot_row},
                plan::PreparedProjectionContract,
                structural::MaterializedProjectionRows,
            },
            terminal::RowLayout,
        },
        query::plan::ResolvedOrder,
    },
    error::InternalError,
};
use std::cell::RefCell;

/// Final DISTINCT projection rows plus strategy-owned cursor progress.
pub(in crate::db::executor::projection) struct MaterializedDistinctProjectionPage {
    rows: MaterializedProjectionRows,
    last_emitted_logical: Option<CursorBoundary>,
    has_more: bool,
}

impl MaterializedDistinctProjectionPage {
    pub(in crate::db::executor::projection) fn into_parts(
        self,
    ) -> (MaterializedProjectionRows, Option<CursorBoundary>, bool) {
        (self.rows, self.last_emitted_logical, self.has_more)
    }
}

/// Runtime-only output authority for one DISTINCT projection page.
pub(in crate::db::executor::projection) struct DistinctProjectionRuntime<'a> {
    resolved_order: Option<&'a ResolvedOrder>,
    output_work: Option<&'a mut ProductionScalarOutputWork>,
}

impl<'a> DistinctProjectionRuntime<'a> {
    #[must_use]
    pub(in crate::db::executor::projection) const fn new(
        resolved_order: Option<&'a ResolvedOrder>,
        output_work: Option<&'a mut ProductionScalarOutputWork>,
    ) -> Self {
        Self {
            resolved_order,
            output_work,
        }
    }
}

pub(in crate::db::executor::projection) fn project_distinct(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionContract,
    strategy: ProjectionDistinctStrategy,
    window: ProjectionDistinctWindow,
    page: StructuralCursorPage,
    runtime: DistinctProjectionRuntime<'_>,
) -> Result<MaterializedDistinctProjectionPage, InternalError> {
    let DistinctProjectionRuntime {
        resolved_order,
        output_work,
    } = runtime;
    let output_work = RefCell::new(output_work);
    let projected = page.consume_projection_rows(
        |slot_rows| {
            collect_distinct_projected_rows(
                strategy,
                window,
                slot_rows,
                |row| {
                    output_work
                        .borrow_mut()
                        .as_deref_mut()
                        .map_or(Ok(true), |work| work.admit_row(row.values()))
                },
                |row| {
                    let boundary =
                        resolved_order.map(|order| cursor_boundary_from_orderable_row(&row, order));
                    project_slot_row(prepared_projection, row)
                        .map(|row| DistinctProjectedRow::new(row, boundary))
                },
            )
        },
        |data_rows| {
            collect_distinct_projected_rows(
                strategy,
                window,
                data_rows,
                |row| {
                    output_work
                        .borrow_mut()
                        .as_deref_mut()
                        .map_or(Ok(true), |work| work.admit_row(row.values()))
                },
                |row| {
                    let boundary = resolved_order
                        .map(|order| cursor_boundary_from_data_row(&row, &row_layout, order))
                        .transpose()?;
                    project_data_row(&row_layout, prepared_projection, &row)
                        .map(|row| DistinctProjectedRow::new(row, boundary))
                },
            )
        },
    )?;
    let (rows, last_emitted_logical, has_more) = projected.into_parts();

    Ok(MaterializedDistinctProjectionPage {
        rows: MaterializedProjectionRows::from_row_views(rows),
        last_emitted_logical,
        has_more,
    })
}
