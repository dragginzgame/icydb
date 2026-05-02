//! Module: executor::delete::commit
//! Responsibility: delete commit payload preparation and commit-window apply.
//! Does not own: candidate row selection or response shaping.
//! Boundary: validates relation blockers and assembles mechanical row ops.

use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{RawDataKey, RawRow},
        executor::{
            EntityAuthority,
            delete::types::{DeleteExecutionAuthority, PreparedDeleteCommit},
            mutation::{
                commit_delete_row_ops_with_window, commit_delete_row_ops_with_window_for_path,
            },
        },
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue, Storable},
};
use std::collections::BTreeSet;

// Prepare the nongeneric delete commit payload from structural rollback rows.
#[inline(never)]
pub(in crate::db::executor::delete) fn prepare_delete_commit<C>(
    db: &Db<C>,
    _store: StoreHandle,
    authority: &DeleteExecutionAuthority,
    rollback_rows: Vec<(RawDataKey, RawRow)>,
) -> Result<PreparedDeleteCommit, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: reject target deletes that are still strongly referenced.
    let deleted_target_keys = rollback_rows
        .iter()
        .map(|(raw_key, _)| *raw_key)
        .collect::<BTreeSet<_>>();
    db.validate_delete_strong_relations(authority.entity.entity_path(), &deleted_target_keys)?;

    // Phase 2: assemble mechanical delete commit row ops.
    let row_ops = rollback_rows
        .into_iter()
        .map(|(raw_key, raw_row)| {
            Ok(CommitRowOp::new(
                authority.entity.entity_path(),
                raw_key,
                Some(raw_row.into_bytes()),
                None,
                authority.schema_fingerprint,
            ))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    Ok(PreparedDeleteCommit { row_ops })
}

// Bridge the final delete commit apply through the existing typed fallback
// only at the wrapper edge so the structural delete core stays shared.
pub(in crate::db::executor::delete) fn apply_delete_commit_window_for_type<E>(
    db: &Db<E::Canister>,
    authority: EntityAuthority,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    if db.has_runtime_hooks() {
        commit_delete_row_ops_with_window_for_path(
            db,
            authority.entity_path(),
            row_ops,
            apply_phase,
        )
    } else {
        commit_delete_row_ops_with_window::<E>(db, row_ops, apply_phase)
    }
}
