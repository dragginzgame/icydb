//! Module: executor::delete::structural_projection
//! Responsibility: SQL structural DELETE RETURNING projection preparation.
//! Does not own: typed delete response rows or commit-window application.
//! Boundary: converts selected structural rows into projection payloads and
//! commit-ready rollback rows.

use crate::{
    db::{
        Db,
        executor::{
            delete::{
                apply_delete_post_access_rows, prepare_delete_commit,
                resolve_delete_candidate_rows_as,
                types::{
                    DeleteCommitApplyFn, DeletePreparation, DeleteProjection, PreparedDeleteCommit,
                    PreparedDeleteExecutionState, PreparedDeleteProjection,
                },
            },
            plan_metrics::record_rows_scanned_for_path,
            projection::MaterializedProjectionRows,
            terminal::{KernelRow, RowDecoder},
        },
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

// Decode structural delete rows, apply the shared delete post-access flow,
// and then let the caller package the surviving kernel rows.
fn prepare_structural_delete_leaf<T>(
    prepared: &PreparedDeleteExecutionState,
    mut rows: Vec<KernelRow>,
    package_rows: impl FnOnce(Vec<KernelRow>) -> Result<T, InternalError>,
) -> Result<T, InternalError> {
    // Phase 1: apply delete-only post-access semantics on the structural row shape.
    apply_delete_post_access_rows(prepared, &mut rows)?;

    // Phase 2: package the already-filtered structural delete rows for the caller.
    package_rows(rows)
}

// Package surviving structural delete kernel rows plus rollback rows for
// commit preparation.
fn package_structural_delete_rows(
    rows: Vec<KernelRow>,
) -> Result<DeletePreparation, InternalError> {
    let mut response_rows = Vec::with_capacity(rows.len());
    let mut rollback_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (data_row, slots) = row.into_parts()?;
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

    Ok(DeletePreparation {
        response_rows: MaterializedProjectionRows::from_value_rows(response_rows),
        rollback_rows,
    })
}

// Resolve, filter, and package one structural delete result before the
// outer typed wrapper applies the final commit window.
fn prepare_structural_delete_projection<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
) -> Result<PreparedDeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve structural access rows once and record the scanned
    // count against the real authority path.
    let row_layout = prepared.authority.entity.row_layout();
    let row_decoder = RowDecoder::structural();
    let (rows, rows_scanned) = resolve_delete_candidate_rows_as(store, prepared, |data_row| {
        row_decoder.decode(&row_layout, data_row)
    })?;
    record_rows_scanned_for_path(prepared.authority.entity.entity_path(), rows_scanned);

    // Phase 2: keep delete filtering, ordering, and rollback packaging on the
    // structural kernel-row boundary.
    let structural =
        prepare_structural_delete_leaf(prepared, rows, package_structural_delete_rows)?;
    if structural.response_rows.len() == 0 {
        return Ok(PreparedDeleteProjection {
            projection: DeleteProjection::new(MaterializedProjectionRows::empty()),
            commit: PreparedDeleteCommit {
                row_ops: Vec::new(),
            },
        });
    }

    // Phase 3: prepare the structural delete commit payload before the typed
    // wrapper enters the mechanical commit-window apply step.
    let commit = prepare_delete_commit(db, store, &prepared.authority, structural.rollback_rows)?;

    Ok(PreparedDeleteProjection {
        projection: DeleteProjection::new(structural.response_rows),
        commit,
    })
}

// Execute one structural delete projection through the shared delete core
// while leaving only the final typed commit-window bridge to the caller.
pub(in crate::db::executor::delete) fn execute_structural_delete_projection_core<C>(
    db: &Db<C>,
    store: StoreHandle,
    prepared: &PreparedDeleteExecutionState,
    apply_delete_commit: DeleteCommitApplyFn<C>,
) -> Result<DeleteProjection, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: run the shared structural delete projection core.
    let prepared_projection = prepare_structural_delete_projection(db, store, prepared)?;
    if prepared_projection.projection.row_count() == 0 {
        return Ok(prepared_projection.projection);
    }

    // Phase 2: apply the already prepared delete commit payload through the
    // caller-provided commit-window bridge.
    apply_delete_commit(
        db,
        prepared.authority.entity,
        prepared_projection.commit.row_ops,
        "delete_row_apply",
    )?;

    Ok(prepared_projection.projection)
}
