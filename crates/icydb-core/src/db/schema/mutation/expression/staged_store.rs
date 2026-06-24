use super::*;

///
/// SchemaExpressionIndexStagedStore
///
/// In-memory staged store image for one expression-index rebuild. This is the
/// first writer-facing shape after staged rebuild validation: it binds raw
/// index entries to the accepted store identity without mutating `IndexStore`
/// or making rebuilt state runtime-visible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStore {
    store: String,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    validation: SchemaExpressionIndexStagedValidation,
    report: SchemaMutationRunnerReport,
}

impl SchemaExpressionIndexStagedStore {
    pub(in crate::db::schema) fn from_rebuild(
        rebuild: &SchemaExpressionIndexStagedRebuild,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> Result<Self, SchemaMutationRunnerRejection> {
        let validation = rebuild.validate().map_err(|_| {
            SchemaMutationRunnerRejection::validation_failed(RebuildRequirement::IndexRebuild)
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
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaExpressionIndexStagedEntry] {
        self.entries.as_slice()
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
        writer: &mut impl SchemaExpressionIndexStagedStoreWriter,
    ) -> SchemaExpressionIndexStagedStoreWriteReport {
        for entry in &self.entries {
            writer.write_staged_entry(&self.store, entry.key(), entry.entry());
        }

        SchemaExpressionIndexStagedStoreWriteReport {
            store: self.store.clone(),
            intended_entries: self.entries.len(),
            store_visibility: self.store_visibility(),
            runner_report: self.report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn write_batch(
        &self,
        reader: &impl SchemaExpressionIndexStagedStoreReadView,
    ) -> SchemaExpressionIndexStagedStoreWriteBatch {
        let rollback_snapshots = self
            .entries
            .iter()
            .map(|entry| SchemaExpressionIndexStagedStoreRollbackSnapshot {
                store: self.store.clone(),
                key: entry.key().clone(),
                previous_entry: reader.read_staged_entry(&self.store, entry.key()),
            })
            .collect();

        SchemaExpressionIndexStagedStoreWriteBatch {
            store: self.store.clone(),
            entries: self.entries.clone(),
            rollback_snapshots,
            store_visibility: self.store_visibility(),
            runner_report: self.report.clone(),
        }
    }
}

///
/// SchemaExpressionIndexStagedStoreReadView
///
/// Read view used to snapshot existing physical index entries before staged
/// write intents are accepted.
///

pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexStoreKey) -> Option<IndexEntryValue>;
}

///
/// SchemaExpressionIndexStagedStoreWriter
///
/// Write-intent sink for one validated staged expression index-store buffer.
///

pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey, entry: &IndexEntryValue);
}

///
/// SchemaExpressionIndexStagedStoreWriteReport
///
/// Typed report after a staged-store writer has accepted all write intents
/// from an in-memory expression rebuild buffer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreWriteReport {
    store: String,
    intended_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaExpressionIndexStagedStoreWriteReport {
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
/// SchemaExpressionIndexStagedStoreWriteBatch
///
/// Isolated staged write batch for one expression-index rebuild. It carries the
/// accepted raw write intents plus rollback snapshots captured from the
/// physical read view.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreWriteBatch {
    store: String,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    rollback_snapshots: Vec<SchemaExpressionIndexStagedStoreRollbackSnapshot>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaExpressionIndexStagedStoreWriteBatch {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaExpressionIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn rollback_snapshots(
        &self,
    ) -> &[SchemaExpressionIndexStagedStoreRollbackSnapshot] {
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
    pub(in crate::db::schema) fn rollback_plan(
        &self,
    ) -> SchemaExpressionIndexStagedStoreRollbackPlan {
        let actions = self
            .rollback_snapshots
            .iter()
            .rev()
            .map(SchemaExpressionIndexStagedStoreRollbackAction::from_snapshot)
            .collect();

        SchemaExpressionIndexStagedStoreRollbackPlan {
            store: self.store.clone(),
            actions,
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
    }
}

///
/// SchemaExpressionIndexStagedStoreRollbackSnapshot
///
/// Previous physical entry captured before one staged raw index write.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackSnapshot {
    store: String,
    key: RawIndexStoreKey,
    previous_entry: Option<IndexEntryValue>,
}

impl SchemaExpressionIndexStagedStoreRollbackSnapshot {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }

    #[must_use]
    pub(in crate::db::schema) const fn previous_entry(&self) -> Option<&IndexEntryValue> {
        self.previous_entry.as_ref()
    }
}

///
/// SchemaExpressionIndexStagedStoreRollbackPlan
///
/// Reverse-order rollback plan for one staged expression-index write batch.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackPlan {
    store: String,
    actions: Vec<SchemaExpressionIndexStagedStoreRollbackAction>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaExpressionIndexStagedStoreRollbackPlan {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn actions(
        &self,
    ) -> &[SchemaExpressionIndexStagedStoreRollbackAction] {
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
        writer: &mut impl SchemaExpressionIndexStagedStoreRollbackWriter,
    ) -> SchemaExpressionIndexStagedStoreRollbackReport {
        let mut restored_entries = 0usize;
        let mut removed_entries = 0usize;

        for action in &self.actions {
            match action {
                SchemaExpressionIndexStagedStoreRollbackAction::Restore { store, key, entry } => {
                    writer.restore_staged_entry(store, key, entry);
                    restored_entries = restored_entries.saturating_add(1);
                }
                SchemaExpressionIndexStagedStoreRollbackAction::Remove { store, key } => {
                    writer.remove_staged_entry(store, key);
                    removed_entries = removed_entries.saturating_add(1);
                }
            }
        }

        SchemaExpressionIndexStagedStoreRollbackReport {
            store: self.store.clone(),
            actions_applied: self.actions.len(),
            restored_entries,
            removed_entries,
        }
    }
}

///
/// SchemaExpressionIndexStagedStoreRollbackWriter
///
/// Rollback-action sink for staged physical expression-index writes.
///

pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreRollbackWriter {
    fn restore_staged_entry(
        &mut self,
        store: &str,
        key: &RawIndexStoreKey,
        entry: &IndexEntryValue,
    );

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey);
}

///
/// SchemaExpressionIndexStagedStoreRollbackReport
///
/// Typed diagnostics after a rollback plan has been accepted by a rollback
/// writer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackReport {
    store: String,
    actions_applied: usize,
    restored_entries: usize,
    removed_entries: usize,
}

impl SchemaExpressionIndexStagedStoreRollbackReport {
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
}

/// SchemaExpressionIndexStagedStoreRollbackAction
///
/// One physical rollback action derived from a prior-entry snapshot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaExpressionIndexStagedStoreRollbackAction {
    Restore {
        store: String,
        key: RawIndexStoreKey,
        entry: IndexEntryValue,
    },
    Remove {
        store: String,
        key: RawIndexStoreKey,
    },
}

impl SchemaExpressionIndexStagedStoreRollbackAction {
    #[must_use]
    fn from_snapshot(snapshot: &SchemaExpressionIndexStagedStoreRollbackSnapshot) -> Self {
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
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        match self {
            Self::Restore { key, .. } | Self::Remove { key, .. } => key,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn restore_entry(&self) -> Option<&IndexEntryValue> {
        match self {
            Self::Restore { entry, .. } => Some(entry),
            Self::Remove { .. } => None,
        }
    }
}
