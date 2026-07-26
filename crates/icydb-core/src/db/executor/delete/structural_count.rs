//! Module: executor::delete::structural_count
//! Responsibility: count-only delete preparation over accepted structural
//! rows.
//! Does not own: typed delete response rows or commit-window application.
//! Boundary: preserves accepted historical row decoding while avoiding typed
//! entity materialization for callers that need only affected-row count.

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
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<DeleteLeaf<T>, InternalError>,
) -> Result<DeleteLeaf<T>, InternalError> {
    let mut rows = resolve_structural_delete_kernel_rows(store, prepared)?;
    apply_delete_post_access_rows(prepared, &mut rows)?;

    package_rows(rows)
}

// Resolve, filter, package, and prepare commit row ops for one structural
// delete output before the outer typed wrapper applies the final commit
// window.
fn prepare_structural_delete_output<C, T>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<DeleteLeaf<T>, InternalError>,
) -> Result<Option<PreparedDeleteOutput<T>>, InternalError>
where
    C: CanisterKind,
{
    let structural = prepare_structural_delete_leaf_from_access(store, prepared, package_rows)?;

    prepare_delete_output_from_leaf(db, store, prepared, structural)
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
    prepare_structural_delete_output(db, store, prepared, package_structural_delete_count)
}
