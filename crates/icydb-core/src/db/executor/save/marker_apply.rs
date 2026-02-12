use crate::{
    db::{
        CommitDataOp, CommitIndexOp,
        executor::{
            mutation::{
                IndexEntryPresencePolicy, MarkerDataOpMode, PreparedDataRollback,
                PreparedIndexRollback, apply_data_rollbacks as apply_data_rollbacks_mutation,
                apply_index_rollbacks as apply_index_rollbacks_mutation,
                apply_marker_data_ops as apply_marker_data_ops_mutation, prepare_index_ops,
            },
            save::SaveExecutor,
        },
        index::{IndexKey, IndexStore, plan::IndexApplyPlan},
        store::{DataKey, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue, Path, Storable},
};
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, thread::LocalKey};

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    // ======================================================================
    // Commit-marker apply (mechanical)
    // ======================================================================

    /// Precompute index mutation metrics before the commit marker is persisted.
    pub(super) fn plan_index_metrics(
        old: Option<&E>,
        new: &E,
    ) -> Result<(usize, usize), InternalError> {
        let mut removes = 0usize;
        let mut inserts = 0usize;

        for index in E::INDEXES {
            if let Some(old) = old
                && IndexKey::new(old, index)?.is_some()
            {
                removes = removes.saturating_add(1);
            }
            if IndexKey::new(new, index)?.is_some() {
                inserts = inserts.saturating_add(1);
            }
        }

        Ok((removes, inserts))
    }

    /// Resolve commit index ops into stores and capture rollback entries.
    #[allow(clippy::type_complexity)]
    pub(super) fn prepare_index_save_ops(
        plans: &[IndexApplyPlan],
        ops: &[CommitIndexOp],
    ) -> Result<
        (
            Vec<&'static LocalKey<RefCell<IndexStore>>>,
            Vec<PreparedIndexRollback>,
        ),
        InternalError,
    > {
        // Phase 1: map index store paths to store handles.
        let mut stores = BTreeMap::new();
        for plan in plans {
            stores.insert(plan.index.store, plan.store);
        }
        prepare_index_ops(
            &stores,
            ops,
            E::PATH,
            "save",
            IndexEntryPresencePolicy::SaveSemantics,
        )
    }

    /// Validate commit data ops and prepare rollback rows for the save.
    pub(super) fn prepare_data_save_ops(
        ops: &[CommitDataOp],
        old_row: Option<RawRow>,
    ) -> Result<Vec<PreparedDataRollback>, InternalError> {
        if ops.len() != 1 {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker save expects 1 data op, found {} ({})",
                    ops.len(),
                    E::PATH
                ),
            ));
        }

        let op = &ops[0];
        if op.store != E::DataStore::PATH {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker references unexpected data store '{}' ({})",
                    op.store,
                    E::PATH
                ),
            ));
        }
        if op.key.len() != DataKey::STORED_SIZE_USIZE {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data key length {} does not match {} ({})",
                    op.key.len(),
                    DataKey::STORED_SIZE_USIZE,
                    E::PATH
                ),
            ));
        }
        let Some(value) = &op.value else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!("commit marker save missing data payload ({})", E::PATH),
            ));
        };
        if value.len() > crate::db::store::MAX_ROW_BYTES as usize {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data payload exceeds max size: {} bytes ({})",
                    value.len(),
                    E::PATH
                ),
            ));
        }

        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
        Ok(vec![PreparedDataRollback {
            key: raw_key,
            value: old_row,
        }])
    }

    /// Apply rollback mutations for index entries using raw bytes.
    pub(super) fn apply_index_rollbacks(ops: Vec<PreparedIndexRollback>) {
        apply_index_rollbacks_mutation(ops);
    }

    /// Apply commit marker data ops to the data store.
    pub(super) fn apply_marker_data_ops(
        ops: &[CommitDataOp],
        store: &'static LocalKey<RefCell<DataStore>>,
    ) {
        apply_marker_data_ops_mutation(ops, store, MarkerDataOpMode::SaveUpsert, E::PATH);
    }

    /// Apply rollback mutations for saved rows.
    pub(super) fn apply_data_rollbacks(
        store: &'static LocalKey<RefCell<DataStore>>,
        ops: Vec<PreparedDataRollback>,
    ) {
        apply_data_rollbacks_mutation(store, ops);
    }
}
