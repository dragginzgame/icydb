use crate::{
    db::{
        CommitApplyGuard, CommitDataOp, CommitGuard, CommitIndexOp, CommitMarker,
        executor::commit_ops::{apply_marker_index_ops, resolve_index_key},
        finish_commit,
        index::{IndexStore, RawIndexEntry, RawIndexKey},
        store::{DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use canic_cdk::structures::Storable;
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// PreparedIndexRollback
///
/// Prevalidated rollback mutation for one index entry.
///

pub(super) struct PreparedIndexRollback {
    pub(super) store: &'static LocalKey<RefCell<IndexStore>>,
    pub(super) key: RawIndexKey,
    pub(super) value: Option<RawIndexEntry>,
}

///
/// PreparedDataRollback
///
/// Prevalidated rollback mutation for one data row.
///

pub(super) struct PreparedDataRollback {
    pub(super) key: RawDataKey,
    pub(super) value: Option<RawRow>,
}

///
/// MarkerDataOpMode
///
/// Commit-marker data-apply behavior for save/delete executors.
///

#[derive(Clone, Copy)]
pub(super) enum MarkerDataOpMode {
    SaveUpsert,
    DeleteRemove,
}

///
/// IndexEntryPresencePolicy
///
/// Validation policy for expected existing index entries when preparing marker ops.
///

#[derive(Clone, Copy)]
pub(super) enum IndexEntryPresencePolicy {
    RequireExisting,
    SaveSemantics,
}

///
/// Prepare index apply stores and rollback entries for commit marker ops.
///
/// This resolves all stores/keys and snapshots current index entries before commit.
/// Missing-entry handling is controlled by `presence_policy`.
///

#[expect(clippy::type_complexity)]
pub(super) fn prepare_index_ops(
    stores: &BTreeMap<&'static str, &'static LocalKey<RefCell<IndexStore>>>,
    ops: &[CommitIndexOp],
    entity_path: &'static str,
    phase_label: &'static str,
    presence_policy: IndexEntryPresencePolicy,
) -> Result<
    (
        Vec<&'static LocalKey<RefCell<IndexStore>>>,
        Vec<PreparedIndexRollback>,
    ),
    InternalError,
> {
    let mut apply_stores = Vec::with_capacity(ops.len());
    let mut rollbacks = Vec::with_capacity(ops.len());

    for op in ops {
        let (store, raw_key) = resolve_index_key(stores, op, entity_path, || {
            let missing_is_error = match presence_policy {
                IndexEntryPresencePolicy::RequireExisting => true,
                IndexEntryPresencePolicy::SaveSemantics => op.value.is_none(),
            };

            if missing_is_error {
                Some(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index op missing entry before {phase_label}: {} ({entity_path})",
                        op.store
                    ),
                ))
            } else {
                None
            }
        })?;
        let existing = store.with_borrow(|s| s.get(&raw_key));

        apply_stores.push(store);
        rollbacks.push(PreparedIndexRollback {
            store,
            key: raw_key,
            value: existing,
        });
    }

    Ok((apply_stores, rollbacks))
}

/// Validate and decode a commit marker data op for a specific executor mode.
///
/// This is a pre-commit structural validation step. It ensures store/key shape
/// and mode-specific payload semantics before marker apply begins.
pub(super) fn validate_marker_data_op(
    op: &CommitDataOp,
    expected_store: &'static str,
    expected_key_len: usize,
    mode: MarkerDataOpMode,
    entity_path: &'static str,
    max_payload_len: Option<usize>,
) -> Result<RawDataKey, InternalError> {
    if op.store != expected_store {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            format!(
                "commit marker references unexpected data store '{}' ({entity_path})",
                op.store
            ),
        ));
    }
    if op.key.len() != expected_key_len {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            format!(
                "commit marker data key length {} does not match {} ({entity_path})",
                op.key.len(),
                expected_key_len
            ),
        ));
    }

    match mode {
        MarkerDataOpMode::SaveUpsert => {
            let Some(value) = &op.value else {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker save missing data payload ({entity_path})"),
                ));
            };
            if let Some(max_payload_len) = max_payload_len
                && value.len() > max_payload_len
            {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker data payload exceeds max size: {} bytes ({entity_path})",
                        value.len()
                    ),
                ));
            }
        }
        MarkerDataOpMode::DeleteRemove => {
            if op.value.is_some() {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker delete includes data payload ({entity_path})"),
                ));
            }
        }
    }

    Ok(RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice())))
}

/// Apply rollback mutations for index entries using raw bytes.
pub(super) fn apply_index_rollbacks(ops: Vec<PreparedIndexRollback>) {
    for op in ops {
        op.store.with_borrow_mut(|s| {
            if let Some(value) = op.value {
                s.insert(op.key, value);
            } else {
                s.remove(&op.key);
            }
        });
    }
}

/// Apply commit marker data ops using prevalidated marker semantics.
pub(super) fn apply_marker_data_ops(
    ops: &[CommitDataOp],
    store: &'static LocalKey<RefCell<DataStore>>,
    mode: MarkerDataOpMode,
    entity_path: &'static str,
) {
    // SAFETY / INVARIANT:
    // All structural and semantic invariants for these marker ops are fully
    // validated during planning *before* the commit marker is persisted.
    // After marker creation, apply is required to be infallible or trap.
    for op in ops {
        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
        match mode {
            MarkerDataOpMode::SaveUpsert => {
                assert!(
                    op.value.is_some(),
                    "invariant violation: commit marker save missing data payload ({entity_path})",
                );
                let value = op.value.as_ref().expect("checked above");
                let raw_value = RawRow::from_bytes(Cow::Borrowed(value.as_slice()));
                store.with_borrow_mut(|s| s.insert(raw_key, raw_value));
            }
            MarkerDataOpMode::DeleteRemove => {
                assert!(
                    op.value.is_none(),
                    "invariant violation: commit marker delete includes data payload ({entity_path})",
                );
                store.with_borrow_mut(|s| s.remove(&raw_key));
            }
        }
    }
}

/// Apply rollback mutations for data rows.
pub(super) fn apply_data_rollbacks(
    store: &'static LocalKey<RefCell<DataStore>>,
    ops: Vec<PreparedDataRollback>,
) {
    for op in ops {
        store.with_borrow_mut(|s| {
            if let Some(value) = op.value {
                s.insert(op.key, value);
            } else {
                s.remove(&op.key);
            }
        });
    }
}

///
/// PreparedMarkerApply
///
/// Fully prepared commit-apply payload shared by save/delete executors.
///

pub(super) struct PreparedMarkerApply {
    pub(super) index_apply_stores: Vec<&'static LocalKey<RefCell<IndexStore>>>,
    pub(super) index_rollback_ops: Vec<PreparedIndexRollback>,
    pub(super) data_store: &'static LocalKey<RefCell<DataStore>>,
    pub(super) data_rollback_ops: Vec<PreparedDataRollback>,
    pub(super) data_mode: MarkerDataOpMode,
    pub(super) entity_path: &'static str,
}

/// Validate index op/store cardinality before entering the commit window.
pub(super) fn validate_index_apply_stores_len(
    marker: &CommitMarker,
    stores_len: usize,
    entity_path: &'static str,
) -> Result<(), InternalError> {
    if stores_len != marker.index_ops.len() {
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            format!(
                "commit marker index ops length mismatch: {} ops vs {} stores ({entity_path})",
                marker.index_ops.len(),
                stores_len,
            ),
        ));
    }

    Ok(())
}

/// Execute the shared commit-window apply skeleton for save/delete executors.
///
/// This helper centralizes commit marker application scaffolding:
/// - open `CommitApplyGuard`
/// - run operation-specific mechanical apply logic
/// - finalize the apply guard
///
/// Durable correctness remains owned by commit markers + recovery replay.
pub(super) fn finish_commit_with_apply_guard(
    commit: CommitGuard,
    apply_phase: &'static str,
    apply: impl FnOnce(
        &[CommitIndexOp],
        &[CommitDataOp],
        &mut CommitApplyGuard,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    finish_commit(commit, |guard| {
        let mut apply_guard = CommitApplyGuard::new(apply_phase);
        apply(
            &guard.marker.index_ops,
            &guard.marker.data_ops,
            &mut apply_guard,
        )?;
        apply_guard.finish()?;

        Ok(())
    })
}

/// Apply prevalidated commit marker ops with shared rollback scaffolding.
pub(super) fn apply_prepared_marker_ops(
    commit: CommitGuard,
    apply_phase: &'static str,
    prepared: PreparedMarkerApply,
    on_index_applied: impl FnOnce(),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    let PreparedMarkerApply {
        index_apply_stores,
        index_rollback_ops,
        data_store,
        data_rollback_ops,
        data_mode,
        entity_path,
    } = prepared;

    finish_commit_with_apply_guard(
        commit,
        apply_phase,
        |marker_index_ops, marker_data_ops, apply_guard| {
            // Commit boundary: apply marker index mutations mechanically.
            apply_guard.record_rollback(move || apply_index_rollbacks(index_rollback_ops));
            apply_marker_index_ops(marker_index_ops, index_apply_stores);
            on_index_applied();

            // Commit boundary: apply marker data mutations mechanically.
            apply_guard
                .record_rollback(move || apply_data_rollbacks(data_store, data_rollback_ops));
            apply_marker_data_ops(marker_data_ops, data_store, data_mode, entity_path);
            on_data_applied();

            Ok(())
        },
    )
}
