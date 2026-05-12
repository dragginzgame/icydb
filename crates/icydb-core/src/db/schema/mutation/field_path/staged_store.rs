use super::*;

///
/// SchemaFieldPathIndexStagedStore
///
/// In-memory staged store image for one field-path index rebuild. This is the
/// first writer-facing shape after staged rebuild validation: it binds raw
/// index entries to the accepted store identity without mutating `IndexStore`
/// or making rebuilt state runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory index-store writes before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStore {
    store: String,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    validation: SchemaFieldPathIndexStagedValidation,
    report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory index-store writes before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStore {
    pub(in crate::db::schema) fn from_rebuild(
        rebuild: &SchemaFieldPathIndexStagedRebuild,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> Result<Self, SchemaMutationRunnerRejection> {
        let validation = rebuild.validate().map_err(|_| {
            SchemaMutationRunnerRejection::validation_failed(
                RebuildRequirement::IndexRebuildRequired,
            )
        })?;
        let report = rebuild.validated_runner_report(execution_plan)?;

        Ok(Self {
            store: rebuild.target().store().to_string(),
            entries: rebuild.entries().to_vec(),
            validation,
            report,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(&self) -> SchemaFieldPathIndexStagedValidation {
        self.validation
    }

    #[must_use]
    pub(in crate::db::schema) const fn report(&self) -> &SchemaMutationRunnerReport {
        &self.report
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.validation.store_visibility()
    }

    #[must_use]
    pub(in crate::db::schema) fn physical_work_allows_publication(&self) -> bool {
        self.report.physical_work_allows_publication()
    }

    #[must_use]
    pub(in crate::db::schema) fn write_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreWriter,
    ) -> SchemaFieldPathIndexStagedStoreWriteReport {
        for entry in &self.entries {
            writer.write_staged_entry(&self.store, entry.key(), entry.entry());
        }

        SchemaFieldPathIndexStagedStoreWriteReport {
            store: self.store.clone(),
            intended_entries: self.entries.len(),
            store_visibility: self.store_visibility(),
            runner_report: self.report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn write_batch(
        &self,
        reader: &impl SchemaFieldPathIndexStagedStoreReadView,
    ) -> SchemaFieldPathIndexStagedStoreWriteBatch {
        let rollback_snapshots = self
            .entries
            .iter()
            .map(|entry| SchemaFieldPathIndexStagedStoreRollbackSnapshot {
                store: self.store.clone(),
                key: entry.key().clone(),
                previous_entry: reader.read_staged_entry(&self.store, entry.key()),
            })
            .collect();

        SchemaFieldPathIndexStagedStoreWriteBatch {
            store: self.store.clone(),
            entries: self.entries.clone(),
            rollback_snapshots,
            store_visibility: self.store_visibility(),
            runner_report: self.report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn discard(self) -> SchemaFieldPathIndexStagedDiscardReport {
        let store_visibility = self.store_visibility();
        let discarded_entries = self.entries.len();

        SchemaFieldPathIndexStagedDiscardReport {
            store: self.store,
            discarded_entries,
            store_visibility,
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreReadView
///
/// Read view used to snapshot existing physical index entries before staged
/// write intents are accepted. The view is intentionally narrower than
/// `IndexStore` so schema mutation runners can prepare rollback data without
/// taking ownership of physical storage.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback snapshots before physical stores are mutated"
)]
pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry>;
}

///
/// SchemaFieldPathIndexStagedStoreWriter
///
/// Write-intent sink for one validated staged field-path index-store buffer.
/// The contract exposes accepted store identity and raw index entries without
/// handing out a physical `IndexStore`; concrete physical mutation remains
/// deferred until the staged-store isolation boundary exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages a physical writer adapter contract before IndexStore mutation exists"
)]
pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry);
}

///
/// SchemaFieldPathIndexStagedStoreWriteReport
///
/// Typed report after a staged-store writer has accepted all write intents from
/// an in-memory field-path rebuild buffer. This is still staged-only: it does
/// not imply physical `IndexStore` mutation, runtime invalidation, or snapshot
/// publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages writer diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreWriteReport {
    store: String,
    intended_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages writer diagnostics before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreWriteReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn intended_entries(&self) -> usize {
        self.intended_entries
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }
}

///
/// SchemaFieldPathIndexStagedStoreWriteBatch
///
/// Isolated staged write batch for one field-path index rebuild. It carries the
/// accepted raw write intents plus rollback snapshots captured from the
/// physical read view, but still leaves actual `IndexStore` mutation to a later
/// isolated-store runner.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback-aware write batches before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreWriteBatch {
    store: String,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    rollback_snapshots: Vec<SchemaFieldPathIndexStagedStoreRollbackSnapshot>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages rollback-aware write batches before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreWriteBatch {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn rollback_snapshots(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStoreRollbackSnapshot] {
        self.rollback_snapshots.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn write_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreWriter,
    ) -> SchemaFieldPathIndexStagedStoreWriteReport {
        for entry in &self.entries {
            writer.write_staged_entry(&self.store, entry.key(), entry.entry());
        }

        SchemaFieldPathIndexStagedStoreWriteReport {
            store: self.store.clone(),
            intended_entries: self.entries.len(),
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn rollback_plan(
        &self,
    ) -> SchemaFieldPathIndexStagedStoreRollbackPlan {
        let actions = self
            .rollback_snapshots
            .iter()
            .rev()
            .map(SchemaFieldPathIndexStagedStoreRollbackAction::from_snapshot)
            .collect();

        SchemaFieldPathIndexStagedStoreRollbackPlan {
            store: self.store.clone(),
            actions,
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreRollbackSnapshot
///
/// Previous physical entry captured before one staged raw index write. A later
/// physical rollback phase can replay these snapshots in reverse write order.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback snapshots before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreRollbackSnapshot {
    store: String,
    key: RawIndexKey,
    previous_entry: Option<RawIndexEntry>,
}

#[allow(
    dead_code,
    reason = "0.153 stages rollback snapshots before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreRollbackSnapshot {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexKey {
        &self.key
    }

    #[must_use]
    pub(in crate::db::schema) const fn previous_entry(&self) -> Option<&RawIndexEntry> {
        self.previous_entry.as_ref()
    }
}

///
/// SchemaFieldPathIndexStagedStoreRollbackPlan
///
/// Reverse-order rollback plan for one staged field-path index write batch.
/// The plan names the restore/remove actions needed to undo staged physical
/// writes, but does not execute them against `IndexStore`.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback plans before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreRollbackPlan {
    store: String,
    actions: Vec<SchemaFieldPathIndexStagedStoreRollbackAction>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages rollback plans before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreRollbackPlan {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn actions(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStoreRollbackAction] {
        self.actions.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn rollback_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreRollbackWriter,
    ) -> SchemaFieldPathIndexStagedStoreRollbackReport {
        let mut restored_entries = 0usize;
        let mut removed_entries = 0usize;

        for action in &self.actions {
            match action {
                SchemaFieldPathIndexStagedStoreRollbackAction::Restore { store, key, entry } => {
                    writer.restore_staged_entry(store, key, entry);
                    restored_entries = restored_entries.saturating_add(1);
                }
                SchemaFieldPathIndexStagedStoreRollbackAction::Remove { store, key } => {
                    writer.remove_staged_entry(store, key);
                    removed_entries = removed_entries.saturating_add(1);
                }
            }
        }

        SchemaFieldPathIndexStagedStoreRollbackReport {
            store: self.store.clone(),
            actions_applied: self.actions.len(),
            restored_entries,
            removed_entries,
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreRollbackWriter
///
/// Rollback-action sink for staged physical field-path index writes. The sink
/// accepts typed restore/remove actions from rollback plans without exposing
/// schema mutation code to a concrete `IndexStore`.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback writer contracts before physical stores are mutated"
)]
pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreRollbackWriter {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry);

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey);
}

///
/// SchemaFieldPathIndexStagedStoreRollbackReport
///
/// Typed diagnostics after a rollback plan has been accepted by a rollback
/// writer. This reports staged rollback intent only; publication remains
/// blocked until physical execution, validation, invalidation, and snapshot
/// publication are all wired.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreRollbackReport {
    store: String,
    actions_applied: usize,
    restored_entries: usize,
    removed_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages rollback diagnostics before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreRollbackReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn actions_applied(&self) -> usize {
        self.actions_applied
    }

    #[must_use]
    pub(in crate::db::schema) const fn restored_entries(&self) -> usize {
        self.restored_entries
    }

    #[must_use]
    pub(in crate::db::schema) const fn removed_entries(&self) -> usize {
        self.removed_entries
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }
}

/// SchemaFieldPathIndexStagedStoreRollbackAction
///
/// One physical rollback action derived from a prior-entry snapshot. `Restore`
/// replaces an overwritten entry; `Remove` deletes a key that did not exist
/// before staged physical writes.
///

#[allow(
    dead_code,
    reason = "0.153 stages rollback actions before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStoreRollbackAction {
    Restore {
        store: String,
        key: RawIndexKey,
        entry: RawIndexEntry,
    },
    Remove {
        store: String,
        key: RawIndexKey,
    },
}

#[allow(
    dead_code,
    reason = "0.153 stages rollback actions before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStoreRollbackAction {
    #[must_use]
    fn from_snapshot(snapshot: &SchemaFieldPathIndexStagedStoreRollbackSnapshot) -> Self {
        match &snapshot.previous_entry {
            Some(entry) => Self::Restore {
                store: snapshot.store.clone(),
                key: snapshot.key.clone(),
                entry: entry.clone(),
            },
            None => Self::Remove {
                store: snapshot.store.clone(),
                key: snapshot.key.clone(),
            },
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        match self {
            Self::Restore { store, .. } | Self::Remove { store, .. } => store.as_str(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexKey {
        match self {
            Self::Restore { key, .. } | Self::Remove { key, .. } => key,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn restore_entry(&self) -> Option<&RawIndexEntry> {
        match self {
            Self::Restore { entry, .. } => Some(entry),
            Self::Remove { .. } => None,
        }
    }
}

///
/// SchemaFieldPathIndexStagedDiscardReport
///
/// Typed cleanup report for discarding one in-memory staged field-path rebuild
/// buffer. Physical rollback will later use a store-backed version of this
/// boundary; this shape keeps staged cleanup explicit before stores are mutated.
///

#[allow(
    dead_code,
    reason = "0.153 stages rebuild rollback diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedDiscardReport {
    store: String,
    discarded_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages rebuild rollback diagnostics before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedDiscardReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn discarded_entries(&self) -> usize {
        self.discarded_entries
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }
}
