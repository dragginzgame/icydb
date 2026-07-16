//! Module: executor::delete::structural_projection
//! Responsibility: SQL structural DELETE RETURNING projection preparation.
//! Does not own: typed delete response rows or commit-window application.
//! Boundary: converts selected structural rows into projection payloads and
//! commit-ready rollback rows.

#[cfg(feature = "sql")]
use crate::{db::executor::projection::MaterializedProjectionRows, value::Value};
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
) -> Result<DeleteLeaf<MaterializedProjectionRows>, InternalError> {
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
        output: MaterializedProjectionRows::from_value_rows(response_rows),
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

fn prepare_structural_delete_leaf_from_access<T>(
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    max_selected_rows: Option<u32>,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<DeleteLeaf<T>, InternalError>,
) -> Result<DeleteLeaf<T>, InternalError> {
    let mut rows = resolve_structural_delete_kernel_rows(store, prepared)?;
    apply_delete_post_access_rows(prepared, &mut rows)?;
    #[cfg(feature = "sql")]
    validate_structural_delete_candidate_bounds(rows.len(), max_selected_rows)?;
    #[cfg(not(feature = "sql"))]
    let _ = max_selected_rows;

    package_rows(rows)
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
fn validate_structural_delete_candidate_bounds(
    selected_candidates: usize,
    max_rows: Option<u32>,
) -> Result<(), InternalError> {
    let Some(max_rows) = max_rows else {
        return Ok(());
    };
    let max_rows = usize::try_from(max_rows).unwrap_or(usize::MAX);
    if selected_candidates <= max_rows {
        return Ok(());
    }

    Err(InternalError::query_sql_write_boundary(
        SqlWriteBoundaryCode::StagedRowsTooMany,
    ))
}

#[cfg(feature = "sql")]
fn validate_structural_delete_projection_bounds(
    projection: &MaterializedProjectionRows,
    max_rows: Option<u32>,
) -> Result<(), InternalError> {
    validate_structural_delete_row_count_bounds(projection.row_count(), max_rows)
}

#[cfg(feature = "sql")]
fn validate_structural_delete_row_count_bounds(
    row_count: u32,
    max_rows: Option<u32>,
) -> Result<(), InternalError> {
    validate_structural_delete_candidate_bounds(row_count as usize, max_rows)
}

// Prepare one structural delete projection through the shared delete core while
// leaving the final typed commit-window bridge to the API wrapper.
#[cfg(feature = "sql")]
pub(in crate::db::executor::delete) fn prepare_structural_delete_projection_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    max_rows: Option<u32>,
    validate_precommit: impl FnOnce(&MaterializedProjectionRows) -> Result<(), InternalError>,
) -> Result<Option<PreparedDeleteOutput<MaterializedProjectionRows>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete output core.
    let Some(prepared_projection) = prepare_structural_delete_output(
        db,
        store,
        prepared,
        max_rows,
        package_structural_delete_rows,
    )?
    else {
        let projection = MaterializedProjectionRows::empty();
        validate_structural_delete_projection_bounds(&projection, max_rows)?;
        validate_precommit(&projection)?;

        return Ok(None);
    };
    validate_structural_delete_projection_bounds(&prepared_projection.output, max_rows)?;
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
        validate_structural_delete_row_count_bounds(row_count, Some(max_rows))?;
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
    max_rows: Option<u32>,
) -> Result<Option<PreparedDeleteOutput<()>>, InternalError>
where
    C: CanisterKind,
{
    prepare_structural_delete_count_core_with_optional_bounds(db, store, prepared, max_rows)
}
