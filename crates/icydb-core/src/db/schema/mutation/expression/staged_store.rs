use super::*;

///
/// SchemaExpressionIndexStagedStore
///
/// In-memory staged store image for one expression-index rebuild. This is the
/// first writer-facing shape after staged rebuild validation: it binds raw
/// index entries to the accepted store identity without mutating `IndexStore`
/// or making rebuilt state runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression index-store writes before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStore {
    store: String,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    validation: SchemaExpressionIndexStagedValidation,
    report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression index-store writes before physical stores are mutated"
)]
impl SchemaExpressionIndexStagedStore {
    pub(in crate::db::schema) fn from_rebuild(
        rebuild: &SchemaExpressionIndexStagedRebuild,
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
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaExpressionIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(&self) -> SchemaExpressionIndexStagedValidation {
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

    #[must_use]
    pub(in crate::db::schema) fn discard(self) -> SchemaExpressionIndexStagedDiscardReport {
        let store_visibility = self.store_visibility();
        let discarded_entries = self.entries.len();

        SchemaExpressionIndexStagedDiscardReport {
            store: self.store,
            discarded_entries,
            store_visibility,
        }
    }
}

///
/// SchemaExpressionIndexStagedStoreReadView
///
/// Read view used to snapshot existing physical index entries before staged
/// write intents are accepted.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback snapshots before physical stores are mutated"
)]
pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry>;
}

///
/// SchemaExpressionIndexStagedStoreWriter
///
/// Write-intent sink for one validated staged expression index-store buffer.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression physical writer adapter contract before IndexStore mutation exists"
)]
pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry);
}

///
/// SchemaExpressionIndexStagedStoreWriteReport
///
/// Typed report after a staged-store writer has accepted all write intents
/// from an in-memory expression rebuild buffer.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression writer diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreWriteReport {
    store: String,
    intended_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression writer diagnostics before physical stores are mutated"
)]
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

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback-aware write batches before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreWriteBatch {
    store: String,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    rollback_snapshots: Vec<SchemaExpressionIndexStagedStoreRollbackSnapshot>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback-aware write batches before physical stores are mutated"
)]
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
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
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

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback snapshots before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackSnapshot {
    store: String,
    key: RawIndexKey,
    previous_entry: Option<RawIndexEntry>,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback snapshots before physical stores are mutated"
)]
impl SchemaExpressionIndexStagedStoreRollbackSnapshot {
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
/// SchemaExpressionIndexStagedStoreRollbackPlan
///
/// Reverse-order rollback plan for one staged expression-index write batch.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback plans before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackPlan {
    store: String,
    actions: Vec<SchemaExpressionIndexStagedStoreRollbackAction>,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback plans before physical stores are mutated"
)]
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
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.clone(),
        }
    }
}

///
/// SchemaExpressionIndexStagedStoreRollbackWriter
///
/// Rollback-action sink for staged physical expression-index writes.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback writer contracts before physical stores are mutated"
)]
pub(in crate::db::schema) trait SchemaExpressionIndexStagedStoreRollbackWriter {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry);

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey);
}

///
/// SchemaExpressionIndexStagedStoreRollbackReport
///
/// Typed diagnostics after a rollback plan has been accepted by a rollback
/// writer.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedStoreRollbackReport {
    store: String,
    actions_applied: usize,
    restored_entries: usize,
    removed_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback diagnostics before physical stores are mutated"
)]
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

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }
}

/// SchemaExpressionIndexStagedStoreRollbackAction
///
/// One physical rollback action derived from a prior-entry snapshot.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rollback actions before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaExpressionIndexStagedStoreRollbackAction {
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
    reason = "0.157 stages expression rollback actions before physical stores are mutated"
)]
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
/// SchemaExpressionIndexStagedDiscardReport
///
/// Typed cleanup report for discarding one in-memory staged expression rebuild
/// buffer.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild rollback diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedDiscardReport {
    store: String,
    discarded_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild rollback diagnostics before physical stores are mutated"
)]
impl SchemaExpressionIndexStagedDiscardReport {
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
