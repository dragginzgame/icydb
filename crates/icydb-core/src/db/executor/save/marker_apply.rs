use crate::{
    db::{
        CommitDataOp, CommitIndexOp,
        executor::{
            mutation::{
                IndexEntryPresencePolicy, MarkerDataOpMode, PreparedDataRollback,
                PreparedIndexRollback, prepare_index_ops, validate_marker_data_op,
            },
            save::SaveExecutor,
        },
        index::{IndexKey, IndexStore, plan::IndexApplyPlan},
        store::{DataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue, Path},
};
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

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
        let raw_key = validate_marker_data_op(
            op,
            E::Store::PATH,
            DataKey::STORED_SIZE_USIZE,
            MarkerDataOpMode::SaveUpsert,
            E::PATH,
            Some(crate::db::store::MAX_ROW_BYTES as usize),
        )?;
        Ok(vec![PreparedDataRollback {
            key: raw_key,
            value: old_row,
        }])
    }
}
