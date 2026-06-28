//! Module: executor::delete::structural_projection
//! Responsibility: SQL structural DELETE RETURNING projection preparation.
//! Does not own: typed delete response rows or commit-window application.
//! Boundary: converts selected structural rows into projection payloads and
//! commit-ready rollback rows.

#[cfg(feature = "sql")]
use crate::{
    db::executor::{
        delete::DeleteProjectionBounds, delete::types::DeleteProjection,
        projection::MaterializedProjectionRows,
    },
    value::Value,
};
use crate::{
    db::{
        Db,
        executor::{
            delete::{
                apply_delete_post_access_rows, prepare_delete_output_from_leaf,
                resolve_delete_candidate_rows_recorded_as,
                types::{DeleteLeaf, PreparedDeleteExecutionState, PreparedDeleteOutput},
            },
            terminal::{KernelRow, RowDecoder},
        },
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
};
#[cfg(feature = "sql")]
use icydb_diagnostic_code::SqlWriteBoundaryCode;

// Package surviving structural delete kernel rows plus rollback rows for
// commit preparation.
#[cfg(feature = "sql")]
fn package_structural_delete_rows(
    rows: Vec<KernelRow>,
) -> Result<DeleteLeaf<DeleteProjection>, InternalError> {
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (data_row, slots) = row.into_data_row_and_slots()?;
        let (key, raw) = data_row;
        let rollback_key = key.to_raw()?;

        // Materialize the RETURNING response from decoded slots before moving
        // the raw row into rollback storage. This keeps rollback ownership
        // single-copy and avoids cloning persisted row bytes for response
        // shaping.
        response_rows.push(
            slots
                .into_iter()
                .map(|value| value.unwrap_or(Value::Null))
                .collect::<Vec<_>>(),
        );
        rollback_rows.push((rollback_key, raw));
    }

    Ok(DeleteLeaf {
        output: DeleteProjection::new(MaterializedProjectionRows::from_value_rows(response_rows)),
        row_count: rollback_rows.len(),
        rollback_rows,
    })
}

// Package surviving structural delete kernel rows into rollback rows only when
// the caller needs affected-row count without response-row materialization.
fn package_structural_delete_count(rows: Vec<KernelRow>) -> Result<DeleteLeaf<()>, InternalError> {
    let row_count = rows.len();
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (data_row, _) = row.into_data_row_and_slots()?;
        let (key, raw) = data_row;
        let rollback_key = key.to_raw()?;

        rollback_rows.push((rollback_key, raw));
    }

    Ok(DeleteLeaf {
        output: (),
        row_count,
        rollback_rows,
    })
}

// Resolve structural delete candidates into kernel rows once, preserving the
// accepted row-layout decode shared by count and RETURNING paths. Scanned-row
// attribution is recorded by the shared delete candidate resolver.
fn resolve_structural_delete_kernel_rows(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<Vec<KernelRow>, InternalError> {
    let row_layout = prepared.authority.entity.row_layout()?;
    let row_decoder = RowDecoder::structural();
    resolve_delete_candidate_rows_recorded_as(store, prepared, |data_row| {
        row_decoder.decode(&row_layout, data_row)
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StructuralDeleteCandidateDiagnostics {
    rows_loaded: usize,
    selected_candidates: usize,
}

impl StructuralDeleteCandidateDiagnostics {
    const fn from_loaded_rows(rows_loaded: usize) -> Self {
        Self {
            rows_loaded,
            selected_candidates: rows_loaded,
        }
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructuralDeleteCandidateBoundCheck {
    FinalProjection,
    PostAccessSelection,
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StructuralDeleteBoundDiagnostics {
    selected_candidates: usize,
    over_limit_at: Option<StructuralDeleteCandidateBoundCheck>,
}

#[cfg(feature = "sql")]
impl StructuralDeleteBoundDiagnostics {
    const fn within_limit(selected_candidates: usize) -> Self {
        Self {
            selected_candidates,
            over_limit_at: None,
        }
    }

    const fn over_limit(
        selected_candidates: usize,
        at: StructuralDeleteCandidateBoundCheck,
    ) -> Self {
        Self {
            selected_candidates,
            over_limit_at: Some(at),
        }
    }

    const fn over_limit_at(self) -> Option<StructuralDeleteCandidateBoundCheck> {
        self.over_limit_at
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StructuralDeleteCandidateBounds {
    max_selected_rows: Option<u32>,
}

#[cfg(feature = "sql")]
impl StructuralDeleteCandidateBounds {
    const fn from_max_selected_rows(max_selected_rows: Option<u32>) -> Self {
        Self { max_selected_rows }
    }

    fn diagnostics_at(
        self,
        selected_candidates: usize,
        at: StructuralDeleteCandidateBoundCheck,
    ) -> StructuralDeleteBoundDiagnostics {
        let Some(max_selected_rows) = self.max_selected_rows else {
            return StructuralDeleteBoundDiagnostics::within_limit(selected_candidates);
        };
        let max_selected_rows = usize::try_from(max_selected_rows).unwrap_or(usize::MAX);
        if selected_candidates <= max_selected_rows {
            return StructuralDeleteBoundDiagnostics::within_limit(selected_candidates);
        }

        StructuralDeleteBoundDiagnostics::over_limit(selected_candidates, at)
    }

    fn validate_at(
        self,
        selected_candidates: usize,
        at: StructuralDeleteCandidateBoundCheck,
    ) -> Result<StructuralDeleteBoundDiagnostics, InternalError> {
        let diagnostics = self.diagnostics_at(selected_candidates, at);
        if diagnostics.over_limit_at().is_none() {
            return Ok(diagnostics);
        }

        Err(InternalError::query_sql_write_boundary(
            SqlWriteBoundaryCode::StagedRowsTooMany,
        ))
    }
}

struct StructuralDeleteCandidateCollection {
    diagnostics: StructuralDeleteCandidateDiagnostics,
    rows: Vec<KernelRow>,
}

impl StructuralDeleteCandidateCollection {
    const fn from_loaded_rows(rows: Vec<KernelRow>) -> Self {
        Self {
            diagnostics: StructuralDeleteCandidateDiagnostics::from_loaded_rows(rows.len()),
            rows,
        }
    }

    fn apply_post_access(
        &mut self,
        prepared: &PreparedDeleteExecutionState,
    ) -> Result<(), InternalError> {
        apply_delete_post_access_rows(prepared, &mut self.rows)?;
        self.diagnostics.selected_candidates = self.rows.len();

        Ok(())
    }

    const fn diagnostics(&self) -> StructuralDeleteCandidateDiagnostics {
        self.diagnostics
    }

    fn into_rows(self) -> Vec<KernelRow> {
        self.rows
    }
}

fn prepare_structural_delete_leaf_from_access<T>(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    max_selected_rows: Option<u32>,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<DeleteLeaf<T>, InternalError>,
) -> Result<DeleteLeaf<T>, InternalError> {
    let rows = resolve_structural_delete_kernel_rows(store, prepared)?;
    let mut collection = StructuralDeleteCandidateCollection::from_loaded_rows(rows);
    collection.apply_post_access(prepared)?;
    let diagnostics = collection.diagnostics();
    debug_assert!(diagnostics.rows_loaded >= diagnostics.selected_candidates);
    #[cfg(feature = "sql")]
    validate_structural_delete_candidate_bounds_at(
        diagnostics.selected_candidates,
        max_selected_rows,
        StructuralDeleteCandidateBoundCheck::PostAccessSelection,
    )?;
    #[cfg(not(feature = "sql"))]
    let _ = max_selected_rows;

    package_rows(collection.into_rows())
}

// Resolve, filter, package, and prepare commit row ops for one structural
// delete output before the outer typed wrapper applies the final commit
// window.
fn prepare_structural_delete_output<C, T>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    max_selected_rows: Option<u32>,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<DeleteLeaf<T>, InternalError>,
) -> Result<Option<PreparedDeleteOutput<T>>, InternalError>
where
    C: CanisterKind,
{
    let structural = prepare_structural_delete_leaf_from_access(
        store,
        prepared,
        max_selected_rows,
        package_rows,
    )?;

    prepare_delete_output_from_leaf(db, store, prepared, structural)
}

#[cfg(feature = "sql")]
fn validate_structural_delete_candidate_bounds_at(
    selected_candidates: usize,
    max_rows: Option<u32>,
    at: StructuralDeleteCandidateBoundCheck,
) -> Result<(), InternalError> {
    StructuralDeleteCandidateBounds::from_max_selected_rows(max_rows)
        .validate_at(selected_candidates, at)?;

    Ok(())
}

#[cfg(feature = "sql")]
fn validate_structural_delete_projection_bounds(
    projection: &DeleteProjection,
    bounds: DeleteProjectionBounds,
) -> Result<(), InternalError> {
    validate_structural_delete_row_count_bounds(projection.row_count(), bounds)
}

#[cfg(feature = "sql")]
fn validate_structural_delete_row_count_bounds(
    row_count: u32,
    bounds: DeleteProjectionBounds,
) -> Result<(), InternalError> {
    let Some(max_rows) = bounds.row_limit() else {
        return Ok(());
    };
    validate_structural_delete_candidate_bounds_at(
        row_count as usize,
        Some(max_rows),
        StructuralDeleteCandidateBoundCheck::FinalProjection,
    )
}

// Prepare one structural delete projection through the shared delete core while
// leaving the final typed commit-window bridge to the API wrapper.
#[cfg(feature = "sql")]
pub(in crate::db::executor::delete) fn prepare_structural_delete_projection_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    bounds: DeleteProjectionBounds,
    validate_precommit: impl FnOnce(&DeleteProjection) -> Result<(), InternalError>,
) -> Result<Option<PreparedDeleteOutput<DeleteProjection>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete output core.
    let Some(prepared_projection) = prepare_structural_delete_output(
        db,
        store,
        prepared,
        bounds.row_limit(),
        package_structural_delete_rows,
    )?
    else {
        let projection = DeleteProjection::new(MaterializedProjectionRows::empty());
        validate_structural_delete_projection_bounds(&projection, bounds)?;
        validate_precommit(&projection)?;

        return Ok(None);
    };
    validate_structural_delete_projection_bounds(&prepared_projection.output, bounds)?;
    validate_precommit(&prepared_projection.output)?;

    Ok(Some(prepared_projection))
}

// Prepare one structural delete count through the shared delete core while
// leaving the final typed commit-window bridge to the API wrapper.
pub(in crate::db::executor::delete) fn prepare_structural_delete_count_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<Option<PreparedDeleteOutput<()>>, InternalError>
where
    C: CanisterKind,
{
    prepare_structural_delete_count_core_with_optional_bounds(db, store, prepared, None)
}

fn prepare_structural_delete_count_core_with_optional_bounds<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    max_rows: Option<u32>,
) -> Result<Option<PreparedDeleteOutput<()>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete-count core.
    let Some(prepared_count) = prepare_structural_delete_output(
        db,
        store,
        prepared,
        max_rows,
        package_structural_delete_count,
    )?
    else {
        return Ok(None);
    };
    #[cfg(not(feature = "sql"))]
    let _ = max_rows;
    #[cfg(feature = "sql")]
    if let Some(max_rows) = max_rows {
        let row_count = u32::try_from(prepared_count.row_count).unwrap_or(u32::MAX);
        validate_structural_delete_row_count_bounds(
            row_count,
            DeleteProjectionBounds::max_rows(max_rows),
        )?;
    }

    Ok(Some(prepared_count))
}

// Prepare one structural delete count with SQL policy row bounds checked before
// the typed commit-window bridge.
#[cfg(feature = "sql")]
pub(in crate::db::executor::delete) fn prepare_structural_delete_count_core_with_bounds<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    bounds: DeleteProjectionBounds,
) -> Result<Option<PreparedDeleteOutput<()>>, InternalError>
where
    C: CanisterKind,
{
    prepare_structural_delete_count_core_with_optional_bounds(
        db,
        store,
        prepared,
        bounds.row_limit(),
    )
}

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::{
        StructuralDeleteCandidateBoundCheck, StructuralDeleteCandidateBounds,
        StructuralDeleteCandidateDiagnostics,
    };

    #[test]
    fn structural_delete_candidate_bounds_report_over_limit_stage() {
        let loaded = StructuralDeleteCandidateDiagnostics::from_loaded_rows(3);
        assert_eq!(loaded.rows_loaded, 3);
        assert_eq!(loaded.selected_candidates, 3);

        let diagnostics = StructuralDeleteCandidateBounds::from_max_selected_rows(Some(2))
            .diagnostics_at(
                loaded.selected_candidates,
                StructuralDeleteCandidateBoundCheck::PostAccessSelection,
            );

        assert_eq!(diagnostics.selected_candidates, 3);
        assert_eq!(
            diagnostics.over_limit_at(),
            Some(StructuralDeleteCandidateBoundCheck::PostAccessSelection),
        );

        let within_limit = StructuralDeleteCandidateBounds::from_max_selected_rows(Some(3))
            .diagnostics_at(
                loaded.selected_candidates,
                StructuralDeleteCandidateBoundCheck::FinalProjection,
            );

        assert_eq!(within_limit.selected_candidates, 3);
        assert_eq!(within_limit.over_limit_at(), None);
    }
}
