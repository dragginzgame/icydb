//! Module: executor::delete::typed
//! Responsibility: typed delete row decoding, response packaging, and commit
//! preparation.
//! Does not own: route setup, structural SQL projection, or commit-window apply.
//! Boundary: converts selected data rows into typed delete outputs.

use crate::{
    db::{
        Db, PersistedRow,
        data::{DataRow, decode_raw_row_for_entity_key},
        executor::{
            delete::{
                apply_delete_post_access_rows, prepare_delete_commit,
                resolve_delete_candidate_rows_as,
                types::{
                    DeleteRow, PreparedDeleteExecutionState, PreparedTypedDelete, TypedDeleteLeaf,
                },
            },
            plan_metrics::record_rows_scanned_for_path,
            saturating_u32_len,
        },
        registry::StoreHandle,
        response::Row,
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
    types::Id,
};

// Decode typed delete candidates, apply the shared delete post-access flow,
// and then let the caller package the surviving rows.
fn prepare_typed_delete_leaf<E, T>(
    prepared: &PreparedDeleteExecutionState,
    mut rows: Vec<DeleteRow<E>>,
    package_rows: impl FnOnce(Vec<DeleteRow<E>>) -> Result<T, InternalError>,
) -> Result<T, InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: apply typed delete post-access filtering and ordering once.
    apply_delete_post_access_rows(prepared, &mut rows)?;

    // Phase 2: package the already-filtered typed delete rows for the caller.
    package_rows(rows)
}

impl<E> DeleteRow<E>
where
    E: PersistedRow + EntityValue,
{
    fn from_delete_data_row(row: DataRow) -> Result<Self, InternalError> {
        let (key, raw) = row;
        let (_, entity) = decode_raw_row_for_entity_key::<E>(&key, &raw)?;

        Ok(Self {
            key,
            raw: Some(raw),
            entity,
        })
    }
}

// Package surviving typed delete rows into outward response rows plus
// rollback rows for commit preparation.
pub(in crate::db::executor::delete) fn package_typed_delete_rows<E>(
    rows: Vec<DeleteRow<E>>,
) -> Result<TypedDeleteLeaf<Vec<Row<E>>>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let response_id = Id::from_key(row.key.try_key::<E>()?);
        let rollback_row = row
            .raw
            .take()
            .ok_or_else(InternalError::delete_rollback_row_required)?;
        let rollback_key = row.key.to_raw()?;

        response_rows.push(Row::new(response_id, row.entity));
        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(TypedDeleteLeaf {
        output: response_rows,
        row_count: rollback_rows.len(),
        rollback_rows,
    })
}

// Package surviving typed delete rows into rollback rows only when the caller
// needs the affected-row count without response-row materialization.
pub(in crate::db::executor::delete) fn package_typed_delete_count<E>(
    rows: Vec<DeleteRow<E>>,
) -> Result<TypedDeleteLeaf<u32>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let row_count = rows.len();
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let rollback_row = row
            .raw
            .take()
            .ok_or_else(InternalError::delete_rollback_row_required)?;
        let rollback_key = row.key.to_raw()?;

        rollback_rows.push((rollback_key, rollback_row));
    }

    Ok(TypedDeleteLeaf {
        output: saturating_u32_len(row_count),
        row_count,
        rollback_rows,
    })
}

// Resolve, filter, and package one typed delete result before the outer
// entrypoint applies the final commit window.
pub(in crate::db::executor::delete) fn prepare_typed_delete_core<C, E, T>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    package_rows: impl FnOnce(Vec<DeleteRow<E>>) -> Result<TypedDeleteLeaf<T>, InternalError>,
) -> Result<Option<PreparedTypedDelete<T>>, InternalError>
where
    C: CanisterKind,
    E: PersistedRow + EntityValue,
{
    // Phase 1: resolve structural access rows once through the shared executor
    // key-stream seam and record the real candidate count for metrics.
    let (rows, rows_scanned) =
        resolve_delete_candidate_rows_as(store, prepared, DeleteRow::<E>::from_delete_data_row)?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), rows_scanned);

    // Phase 2: run typed delete post-access selection and package the caller's
    // desired output shape alongside rollback rows.
    let typed = prepare_typed_delete_leaf(prepared, rows, package_rows)?;
    if typed.row_count == 0 {
        return Ok(None);
    }

    // Phase 3: prepare relation validation and commit row ops once for the
    // already-selected delete targets.
    let commit = prepare_delete_commit(db, store, &prepared.authority, typed.rollback_rows)?;

    Ok(Some(PreparedTypedDelete {
        output: typed.output,
        commit,
        row_count: typed.row_count,
    }))
}
