//! Module: db::commit::apply
//! Responsibility: apply precomputed row/index mutations to stores.
//! Does not own: mutation preparation, commit-marker durability, or recovery orchestration.
//! Boundary: commit::{prepared_op,prepare,rebuild} -> commit::apply (one-way).

use crate::{
    db::{
        commit::{PreparedIndexMutation, PreparedRowCommitOp},
        data::DataStore,
        index::IndexStore,
        positioned_overlay::JournalOverlayPosition,
    },
    error::InternalError,
};

impl PreparedIndexMutation {
    /// Preflight one positioned live index publication.
    pub(crate) fn preflight_positioned(
        &self,
        position: JournalOverlayPosition,
    ) -> Result<(), InternalError> {
        self.index_store
            .with_borrow(|store| store.preflight_positioned_journal_entry(&self.key, position))
    }

    /// Apply one precomputed index mutation infallibly.
    pub(crate) fn apply(self) {
        self.index_store.with_borrow_mut(|store| {
            if let Some(value) = self.value {
                store.insert(self.key, value);
            } else {
                store.remove(&self.key);
            }
        });
    }

    /// Fold one already-prepared recovered mutation into canonical stable index storage.
    pub(in crate::db) fn fold_recovered(self) -> Result<(), crate::error::InternalError> {
        self.index_store
            .with_borrow_mut(|store| store.fold_recovered_journal_entry(self.key, self.value))
    }
}

impl PreparedRowCommitOp {
    /// Preflight every live position owned by this journaled row transition.
    pub(crate) fn preflight_positioned(
        &self,
        position: JournalOverlayPosition,
    ) -> Result<(), InternalError> {
        for index_op in &self.index_ops {
            index_op.preflight_positioned(position)?;
        }
        self.data_store
            .with_borrow(|store| store.preflight_positioned_journal_entry(&self.data_key, position))
    }

    /// Prove every canonical owner used by recovered row publication.
    pub(in crate::db) fn preflight_fold_recovered(&self) -> Result<(), InternalError> {
        self.data_store
            .with_borrow(DataStore::preflight_fold_recovered_journal)?;
        self.data_index_store
            .with_borrow(IndexStore::preflight_fold_recovered_journal)?;
        for index_op in &self.index_ops {
            index_op
                .index_store
                .with_borrow(IndexStore::preflight_fold_recovered_journal)?;
        }
        Ok(())
    }

    /// Apply one prepared row and publish its already-preflighted live positions.
    pub(crate) fn apply_positioned(
        self,
        position: JournalOverlayPosition,
    ) -> Result<(), InternalError> {
        for index_op in self.index_ops {
            index_op.index_store.with_borrow_mut(|store| {
                store
                    .publish_preflighted_journal_entry(index_op.key, index_op.value, position)
                    .map(|_| ())
            })?;
        }

        let data_generation = self.data_store.with_borrow_mut(|store| {
            store
                .publish_preflighted_journal_entry(
                    self.data_key,
                    self.data_value.map(|value| value.as_raw_row().clone()),
                    position,
                )
                .map(|_| store.generation())
        })?;
        self.data_index_store.with_borrow_mut(|store| {
            store.mark_prefix_cardinality_data_generation(data_generation);
        });
        Ok(())
    }

    /// Fold one already-preflighted row transition into canonical storage.
    pub(in crate::db) fn fold_recovered(self) -> Result<(), InternalError> {
        for index_op in self.index_ops {
            index_op.fold_recovered()?;
        }
        let data_generation = self
            .data_store
            .with_borrow_mut(|store| match self.data_value {
                Some(value) => store
                    .fold_recovered_journal_put(self.data_key, value.as_raw_row().clone())
                    .map(|_| store.generation()),
                None => store
                    .fold_recovered_journal_delete(&self.data_key)
                    .map(|_| store.generation()),
            })?;
        self.data_index_store.with_borrow_mut(|store| {
            store.mark_prefix_cardinality_data_generation(data_generation);
        });
        Ok(())
    }

    /// Apply the prepared row operation infallibly.
    pub(crate) fn apply(self) {
        // Phase 1: apply all index mutations first so rollback snapshots can
        // mirror this order exactly in reverse.
        for index_op in self.index_ops {
            index_op.apply();
        }

        // Phase 2: apply the authoritative row-store mutation.
        let data_generation = self.data_store.with_borrow_mut(|store| {
            if let Some(value) = self.data_value {
                store.insert(self.data_key, value);
            } else {
                store.remove(&self.data_key);
            }
            store.generation()
        });

        self.data_index_store.with_borrow_mut(|store| {
            store.mark_prefix_cardinality_data_generation(data_generation);
        });
    }
}
