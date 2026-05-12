use super::*;

///
/// SchemaFieldPathIndexRebuildRow
///
/// One authoritative row exposed to the field-path rebuild staging primitive.
/// The row is already decoded behind a canonical slot-reader contract; staging
/// must only derive accepted index keys from these row slots.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild row inputs before physical runners own row iteration"
)]
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRebuildRow<'a> {
    storage_key: StorageKey,
    slots: &'a dyn CanonicalSlotReader,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild row inputs before physical runners own row iteration"
)]
impl<'a> SchemaFieldPathIndexRebuildRow<'a> {
    #[must_use]
    pub(in crate::db::schema) const fn new(
        storage_key: StorageKey,
        slots: &'a dyn CanonicalSlotReader,
    ) -> Self {
        Self { storage_key, slots }
    }

    #[must_use]
    pub(in crate::db::schema) const fn storage_key(self) -> StorageKey {
        self.storage_key
    }

    #[must_use]
    pub(in crate::db::schema) const fn slots(self) -> &'a dyn CanonicalSlotReader {
        self.slots
    }
}

///
/// SchemaFieldPathIndexStagedEntry
///
/// One raw index-store entry produced during staged field-path rebuild work.
/// It remains in memory until later runner phases validate and publish it.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory rebuild entries before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedEntry {
    key: RawIndexKey,
    entry: RawIndexEntry,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory rebuild entries before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedEntry {
    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexKey {
        &self.key
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry(&self) -> &RawIndexEntry {
        &self.entry
    }
}

///
/// SchemaFieldPathIndexStagedRebuild
///
/// In-memory staged field-path index state. This is not a published store and
/// must not be made planner-visible until validation and publication complete.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory field-path rebuild output before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedRebuild {
    target: SchemaFieldPathIndexRebuildTarget,
    pub(super) entries: Vec<SchemaFieldPathIndexStagedEntry>,
    source_rows: usize,
    pub(super) skipped_rows: usize,
    pub(super) store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory field-path rebuild output before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedRebuild {
    pub(in crate::db::schema) fn from_rows<'a>(
        entity_path: &str,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
    ) -> Result<Self, InternalError> {
        let mut entries = Vec::new();
        let mut source_rows = 0usize;
        let mut skipped_rows = 0usize;

        for row in rows {
            source_rows = source_rows.saturating_add(1);
            let Some(key) = IndexKey::new_from_slots_with_field_path_rebuild_target(
                entity_tag,
                row.storage_key(),
                &target,
                row.slots(),
            )?
            else {
                skipped_rows = skipped_rows.saturating_add(1);
                continue;
            };
            let entry = IndexEntry::new(row.storage_key());
            let raw_entry = RawIndexEntry::try_from(&entry)
                .map_err(|err| err.into_commit_internal_error(entity_path, target.name()))?;

            entries.push(SchemaFieldPathIndexStagedEntry {
                key: key.to_raw(),
                entry: raw_entry,
            });
        }

        entries.sort_by(|left, right| left.key.cmp(&right.key));

        Ok(Self {
            target,
            entries,
            source_rows,
            skipped_rows,
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn target(&self) -> &SchemaFieldPathIndexRebuildTarget {
        &self.target
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn source_rows(&self) -> usize {
        self.source_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn skipped_rows(&self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    pub(in crate::db::schema) fn validate(
        &self,
    ) -> Result<SchemaFieldPathIndexStagedValidation, SchemaFieldPathIndexStagedValidationError>
    {
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexStagedValidationError::PublishedVisibility);
        }

        let expected_entries = self
            .source_rows
            .checked_sub(self.skipped_rows)
            .ok_or(SchemaFieldPathIndexStagedValidationError::SkippedRowsExceedSourceRows)?;
        if expected_entries != self.entries.len() {
            return Err(SchemaFieldPathIndexStagedValidationError::EntryCountMismatch);
        }

        if !self
            .entries
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
        {
            return Err(SchemaFieldPathIndexStagedValidationError::UnsortedOrDuplicateEntries);
        }

        Ok(SchemaFieldPathIndexStagedValidation {
            entry_count: self.entries.len(),
            source_rows: self.source_rows,
            skipped_rows: self.skipped_rows,
            store_visibility: self.store_visibility,
        })
    }

    pub(in crate::db::schema) fn validated_runner_report(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> Result<SchemaMutationRunnerReport, SchemaMutationRunnerRejection> {
        let step_count = match execution_plan.execution_gate() {
            SchemaMutationExecutionGate::AwaitingPhysicalWork {
                requirement: RebuildRequirement::IndexRebuildRequired,
                step_count,
            } => step_count,
            SchemaMutationExecutionGate::AwaitingPhysicalWork { requirement, .. }
            | SchemaMutationExecutionGate::Rejected { requirement } => {
                return Err(SchemaMutationRunnerRejection::unsupported_requirement(
                    requirement,
                ));
            }
            SchemaMutationExecutionGate::ReadyToPublish => {
                return Err(SchemaMutationRunnerRejection::unsupported_requirement(
                    RebuildRequirement::NoRebuildRequired,
                ));
            }
        };

        let validation = self.validate().map_err(|_| {
            SchemaMutationRunnerRejection::validation_failed(
                RebuildRequirement::IndexRebuildRequired,
            )
        })?;

        Ok(SchemaMutationRunnerReport::field_path_index_staged(
            step_count,
            execution_plan.runner_capabilities(),
            validation,
        ))
    }
}

///
/// SchemaFieldPathIndexStagedValidationError
///
/// Fail-closed validation result for staged field-path rebuild output. Later
/// runner phases must validate staged output before any store publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedValidationError {
    PublishedVisibility,
    SkippedRowsExceedSourceRows,
    EntryCountMismatch,
    UnsortedOrDuplicateEntries,
}

///
/// SchemaFieldPathIndexStagedValidation
///
/// Positive validation report for one in-memory field-path rebuild. The report
/// intentionally stays small until physical runner diagnostics consume it.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedValidation {
    entry_count: usize,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedValidation {
    #[must_use]
    pub(in crate::db::schema) const fn entry_count(self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn source_rows(self) -> usize {
        self.source_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn skipped_rows(self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }
}

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

///
/// SchemaFieldPathIndexIsolatedIndexStoreWriter
///
/// Adapter from staged field-path index write and rollback contracts into a
/// caller-provided isolated `IndexStore`. The adapter marks the supplied store
/// as building and keeps schema mutation visibility staged-only; callers must
/// not pass a runtime-visible store until publication and invalidation wiring
/// exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore mutation before publication exists"
)]
pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    store: String,
    pub(super) index_store: &'a mut IndexStore,
    generation_before: u64,
    pub(super) store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore mutation before publication exists"
)]
impl<'a> SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    pub(in crate::db::schema) fn new(store: &str, index_store: &'a mut IndexStore) -> Self {
        let generation_before = index_store.generation();
        index_store.mark_building();

        Self {
            store: store.to_string(),
            index_store,
            generation_before,
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation(&self) -> u64 {
        self.index_store.generation()
    }

    #[must_use]
    pub(in crate::db::schema) fn len(&self) -> u64 {
        self.index_store.len()
    }

    #[must_use]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.index_store.get(key)
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_store.state()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    pub(in crate::db::schema) fn validate_batch(
        &self,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
    ) -> Result<
        SchemaFieldPathIndexIsolatedIndexStoreValidation,
        SchemaFieldPathIndexIsolatedIndexStoreValidationError,
    > {
        if self.store != batch.store() {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreMismatch);
        }
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::PublishedVisibility);
        }
        if self.index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreNotBuilding);
        }

        let expected_entry_count =
            u64::try_from(batch.entries().len()).expect("staged entry count should fit in u64");
        if self.index_store.len() != expected_entry_count {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch);
        }

        for entry in batch.entries() {
            let Some(index_entry) = self.index_store.get(entry.key()) else {
                return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::MissingEntry);
            };
            if index_entry != *entry.entry() {
                return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryMismatch);
            }
        }

        Ok(SchemaFieldPathIndexIsolatedIndexStoreValidation {
            store: self.store.clone(),
            entry_count: batch.entries().len(),
            generation_before: self.generation_before,
            generation_after: self.index_store.generation(),
            index_state: self.index_store.state(),
            store_visibility: self.store_visibility,
            runner_report: batch.runner_report().clone(),
        })
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.accepts_store(store)
            .then(|| self.index_store.get(key))
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter
    for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_>
{
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        if self.accepts_store(store) {
            self.index_store.remove(key);
        }
    }
}

///
/// SchemaFieldPathIndexIsolatedIndexStoreValidationError
///
/// Fail-closed validation reasons for an isolated `IndexStore` after staged
/// writes have been applied. These checks keep a physical staged store from
/// becoming a publication candidate unless it exactly matches the staged batch.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexIsolatedIndexStoreValidationError {
    StoreMismatch,
    PublishedVisibility,
    StoreNotBuilding,
    EntryCountMismatch,
    MissingEntry,
    EntryMismatch,
}

///
/// SchemaFieldPathIndexIsolatedIndexStoreValidation
///
/// Positive validation report for an isolated `IndexStore` after staged
/// field-path index writes have landed. The report records generation movement
/// and building-state visibility, but does not mark the store publishable.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreValidation {
    store: String,
    entry_count: usize,
    generation_before: u64,
    generation_after: u64,
    index_state: IndexState,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
impl SchemaFieldPathIndexIsolatedIndexStoreValidation {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_after(&self) -> u64 {
        self.generation_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
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
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_isolated_index_store_validation(
            self,
        )
    }
}

///
/// SchemaMutationRuntimeInvalidationSink
///
/// Sink for runtime schema invalidation after staged physical index work has
/// validated. This is intentionally narrower than cache/planner internals so
/// runner code can record the invalidation boundary before publication wiring
/// exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
pub(in crate::db::schema) trait SchemaMutationRuntimeInvalidationSink {
    fn invalidate_runtime_schema(
        &mut self,
        store: &str,
        before: &SchemaMutationRuntimeEpoch,
        after: &SchemaMutationRuntimeEpoch,
    );
}

///
/// SchemaFieldPathIndexRuntimeInvalidationPlan
///
/// Runtime invalidation plan for one validated staged field-path index store.
/// It binds physical validation to the accepted before/after schema epochs
/// while keeping store visibility staged-only.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationPlan {
    store: String,
    entry_count: usize,
    publication_identity: SchemaMutationPublicationIdentity,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
impl SchemaFieldPathIndexRuntimeInvalidationPlan {
    pub(in crate::db::schema) fn from_isolated_index_store_validation(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
        input: &SchemaMutationRunnerInput<'_>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            store: validation.store().to_string(),
            entry_count: validation.entry_count(),
            publication_identity: SchemaMutationPublicationIdentity::from_input(
                input,
                validation.store_visibility(),
            )?,
            store_visibility: validation.store_visibility(),
            runner_report: validation.runner_report().clone(),
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
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
    pub(in crate::db::schema) fn requires_invalidation(&self) -> bool {
        self.publication_identity.changes_epoch()
    }

    #[must_use]
    pub(in crate::db::schema) fn invalidate_runtime_state(
        &self,
        sink: &mut impl SchemaMutationRuntimeInvalidationSink,
    ) -> SchemaFieldPathIndexRuntimeInvalidationReport {
        let invalidated_epochs = usize::from(self.requires_invalidation());
        if self.requires_invalidation() {
            sink.invalidate_runtime_schema(
                &self.store,
                self.publication_identity.before_epoch(),
                self.publication_identity.after_epoch(),
            );
        }

        SchemaFieldPathIndexRuntimeInvalidationReport {
            store: self.store.clone(),
            entry_count: self.entry_count,
            publication_identity: self.publication_identity.clone(),
            invalidated_epochs,
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.with_runtime_state_invalidated(),
        }
    }
}

///
/// SchemaFieldPathIndexRuntimeInvalidationReport
///
/// Positive report after runtime invalidation has accepted one validated staged
/// field-path index store. This advances runner diagnostics through
/// `InvalidateRuntimeState` but remains non-publishable while store visibility
/// is staged-only and snapshot publication has not occurred.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationReport {
    store: String,
    entry_count: usize,
    publication_identity: SchemaMutationPublicationIdentity,
    invalidated_epochs: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
impl SchemaFieldPathIndexRuntimeInvalidationReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn invalidated_epochs(&self) -> usize {
        self.invalidated_epochs
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
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_runtime_invalidation_report(self)
    }
}

///
/// SchemaMutationAcceptedSnapshotPublicationSink
///
/// Sink for publishing the accepted-after schema snapshot after staged physical
/// work has validated and runtime state has been invalidated. This keeps the
/// runner publication handoff mockable until real schema-store publication is
/// wired.
///

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
pub(in crate::db::schema) trait SchemaMutationAcceptedSnapshotPublicationSink {
    fn publish_accepted_schema(
        &mut self,
        store: &str,
        accepted_after: &PersistedSchemaSnapshot,
        before: &SchemaMutationRuntimeEpoch,
        after: &SchemaMutationRuntimeEpoch,
    );
}

///
/// SchemaFieldPathIndexSnapshotPublicationPlanError
///
/// Fail-closed reasons for constructing a staged field-path index snapshot
/// publication plan after runtime invalidation.
///

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexSnapshotPublicationPlanError {
    RuntimeStateNotInvalidated,
    AcceptedSnapshotIdentity,
}

///
/// SchemaFieldPathIndexSnapshotPublicationPlan
///
/// Publication handoff for one validated and invalidated staged field-path
/// index store. The plan publishes through a sink and reports the final runner
/// publication phase without directly mutating the schema store.
///

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationPlan {
    store: String,
    entry_count: usize,
    accepted_after: PersistedSchemaSnapshot,
    publication_identity: SchemaMutationPublicationIdentity,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
impl SchemaFieldPathIndexSnapshotPublicationPlan {
    pub(in crate::db::schema) fn from_runtime_invalidation_report(
        report: &SchemaFieldPathIndexRuntimeInvalidationReport,
        input: &SchemaMutationRunnerInput<'_>,
    ) -> Result<Self, SchemaFieldPathIndexSnapshotPublicationPlanError> {
        if !report
            .runner_report()
            .has_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState)
        {
            return Err(
                SchemaFieldPathIndexSnapshotPublicationPlanError::RuntimeStateNotInvalidated,
            );
        }

        let publication_identity = SchemaMutationPublicationIdentity::from_input(
            input,
            SchemaMutationStoreVisibility::Published,
        )
        .map_err(|_| SchemaFieldPathIndexSnapshotPublicationPlanError::AcceptedSnapshotIdentity)?;

        Ok(Self {
            store: report.store().to_string(),
            entry_count: report.entry_count(),
            accepted_after: input.accepted_after().clone(),
            publication_identity,
            runner_report: report.runner_report().clone(),
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn publish_snapshot(
        &self,
        sink: &mut impl SchemaMutationAcceptedSnapshotPublicationSink,
    ) -> SchemaFieldPathIndexSnapshotPublicationReport {
        sink.publish_accepted_schema(
            &self.store,
            &self.accepted_after,
            self.publication_identity.before_epoch(),
            self.publication_identity.after_epoch(),
        );

        SchemaFieldPathIndexSnapshotPublicationReport {
            store: self.store.clone(),
            entry_count: self.entry_count,
            accepted_after: self.accepted_after.clone(),
            publication_identity: self.publication_identity.clone(),
            store_visibility: SchemaMutationStoreVisibility::Published,
            runner_report: self.runner_report.with_snapshot_published(),
        }
    }
}

///
/// SchemaFieldPathIndexSnapshotPublicationReport
///
/// Positive report after the accepted-after snapshot publication handoff has
/// been accepted by a sink. The report is publishable because validation,
/// invalidation, snapshot publication, and published store visibility are all
/// represented in the runner diagnostics.
///

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationReport {
    store: String,
    entry_count: usize,
    accepted_after: PersistedSchemaSnapshot,
    publication_identity: SchemaMutationPublicationIdentity,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
impl SchemaFieldPathIndexSnapshotPublicationReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
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
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_snapshot_publication_report(self)
    }
}

///
/// SchemaFieldPathIndexPublishedStoreError
///
/// Fail-closed reasons for promoting a validated staged field-path index store
/// to published `IndexStore` visibility.
///

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexPublishedStoreError {
    StoreMismatch,
    PhysicalStateNotValidated,
    SnapshotNotPublished,
    StoreNotBuilding,
    EntryCountMismatch,
}

///
/// SchemaFieldPathIndexPublishedStorePlan
///
/// Final physical publication plan for one validated field-path `IndexStore`.
/// It is constructible only after isolated physical validation and accepted
/// snapshot publication agree on the same accepted store.
///

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStorePlan {
    store: String,
    entry_count: usize,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
impl SchemaFieldPathIndexPublishedStorePlan {
    pub(in crate::db::schema) fn from_validated_publication(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
        publication_report: &SchemaFieldPathIndexSnapshotPublicationReport,
    ) -> Result<Self, SchemaFieldPathIndexPublishedStoreError> {
        if validation.store() != publication_report.store() {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreMismatch);
        }
        if !validation
            .runner_report()
            .has_completed_phase(SchemaMutationRunnerPhase::ValidatePhysicalState)
        {
            return Err(SchemaFieldPathIndexPublishedStoreError::PhysicalStateNotValidated);
        }
        if !publication_report
            .runner_report()
            .physical_work_allows_publication()
        {
            return Err(SchemaFieldPathIndexPublishedStoreError::SnapshotNotPublished);
        }
        if validation.index_state() != IndexState::Building {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreNotBuilding);
        }
        if validation.entry_count() != publication_report.entry_count() {
            return Err(SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch);
        }

        Ok(Self {
            store: validation.store().to_string(),
            entry_count: validation.entry_count(),
            publication_report: publication_report.clone(),
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    pub(in crate::db::schema) fn publish_index_store(
        &self,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexPublishedStoreReport, SchemaFieldPathIndexPublishedStoreError>
    {
        if index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreNotBuilding);
        }

        let entry_count = usize::try_from(index_store.len())
            .map_err(|_| SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch)?;
        if entry_count != self.entry_count {
            return Err(SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch);
        }

        let generation_before = index_store.generation();
        index_store.mark_ready();

        Ok(SchemaFieldPathIndexPublishedStoreReport {
            store: self.store.clone(),
            entry_count,
            generation_before,
            generation_after: index_store.generation(),
            index_state: index_store.state(),
            store_visibility: SchemaMutationStoreVisibility::Published,
            publication_report: self.publication_report.clone(),
        })
    }
}

///
/// SchemaFieldPathIndexPublishedStoreReport
///
/// Positive report after a validated isolated field-path `IndexStore` has been
/// promoted to ready, planner-visible physical state.
///

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStoreReport {
    store: String,
    entry_count: usize,
    generation_before: u64,
    generation_after: u64,
    index_state: IndexState,
    store_visibility: SchemaMutationStoreVisibility,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
impl SchemaFieldPathIndexPublishedStoreReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_after(&self) -> u64 {
        self.generation_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        self.publication_report.runner_report()
    }

    #[must_use]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        self.publication_report.publication_readiness()
    }
}

///
/// SchemaFieldPathIndexRunnerError
///
/// Fail-closed field-path runner orchestration errors. These classify the
/// phase that prevented publication without matching lower-level error text.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexRunnerError {
    UnsupportedExecutionPlan,
    TargetMismatch,
    StageRowsFailed,
    StagedStoreRejected,
    IsolatedStoreValidationFailed,
    RuntimeInvalidationIdentity,
    SnapshotPublicationRejected,
    PublishedStoreRejected,
}

///
/// SchemaFieldPathIndexRunnerReport
///
/// End-to-end runner report for one accepted field-path index rebuild. It
/// binds the staged write, isolated physical validation, runtime invalidation,
/// and accepted snapshot publication handoff into one typed result.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerReport {
    store: String,
    write_report: SchemaFieldPathIndexStagedStoreWriteReport,
    validation: SchemaFieldPathIndexIsolatedIndexStoreValidation,
    invalidation_report: SchemaFieldPathIndexRuntimeInvalidationReport,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
    published_store_report: SchemaFieldPathIndexPublishedStoreReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
impl SchemaFieldPathIndexRunnerReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn write_report(
        &self,
    ) -> &SchemaFieldPathIndexStagedStoreWriteReport {
        &self.write_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(
        &self,
    ) -> &SchemaFieldPathIndexIsolatedIndexStoreValidation {
        &self.validation
    }

    #[must_use]
    pub(in crate::db::schema) const fn invalidation_report(
        &self,
    ) -> &SchemaFieldPathIndexRuntimeInvalidationReport {
        &self.invalidation_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn published_store_report(
        &self,
    ) -> &SchemaFieldPathIndexPublishedStoreReport {
        &self.published_store_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        self.published_store_report.runner_report()
    }

    #[must_use]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        self.publication_report.publication_readiness()
    }
}

///
/// SchemaFieldPathIndexRunner
///
/// Narrow physical runner for one accepted field-path index rebuild. The
/// runner only accepts the schema-owned field-path execution shape and keeps
/// row staging, isolated store mutation, runtime invalidation, and snapshot
/// publication handoff in a fixed order.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunner;

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
impl SchemaFieldPathIndexRunner {
    pub(in crate::db::schema) fn run<'a>(
        input: &SchemaMutationRunnerInput<'_>,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
        index_store: &mut IndexStore,
        invalidation_sink: &mut impl SchemaMutationRuntimeInvalidationSink,
        publication_sink: &mut impl SchemaMutationAcceptedSnapshotPublicationSink,
    ) -> Result<SchemaFieldPathIndexRunnerReport, SchemaFieldPathIndexRunnerError> {
        Self::validate_execution_plan(input.execution_plan(), &target)?;

        let staged = SchemaFieldPathIndexStagedRebuild::from_rows(
            input.accepted_after().entity_path(),
            entity_tag,
            target,
            rows,
        )
        .map_err(|_| SchemaFieldPathIndexRunnerError::StageRowsFailed)?;
        let staged_store =
            SchemaFieldPathIndexStagedStore::from_rebuild(&staged, input.execution_plan())
                .map_err(|_| SchemaFieldPathIndexRunnerError::StagedStoreRejected)?;
        let store = staged_store.store().to_string();
        let (write_report, validation) = {
            let mut writer = SchemaFieldPathIndexIsolatedIndexStoreWriter::new(&store, index_store);
            let batch = staged_store.write_batch(&writer);
            let write_report = batch.write_to(&mut writer);
            let validation = writer
                .validate_batch(&batch)
                .map_err(|_| SchemaFieldPathIndexRunnerError::IsolatedStoreValidationFailed)?;

            (write_report, validation)
        };
        let invalidation_plan =
            SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
                &validation,
                input,
            )
            .map_err(|_| SchemaFieldPathIndexRunnerError::RuntimeInvalidationIdentity)?;
        let invalidation_report = invalidation_plan.invalidate_runtime_state(invalidation_sink);
        let publication_plan =
            SchemaFieldPathIndexSnapshotPublicationPlan::from_runtime_invalidation_report(
                &invalidation_report,
                input,
            )
            .map_err(|_| SchemaFieldPathIndexRunnerError::SnapshotPublicationRejected)?;
        let publication_report = publication_plan.publish_snapshot(publication_sink);
        let published_store_plan =
            SchemaFieldPathIndexPublishedStorePlan::from_validated_publication(
                &validation,
                &publication_report,
            )
            .map_err(|_| SchemaFieldPathIndexRunnerError::PublishedStoreRejected)?;
        let published_store_report = published_store_plan
            .publish_index_store(index_store)
            .map_err(|_| SchemaFieldPathIndexRunnerError::PublishedStoreRejected)?;

        Ok(SchemaFieldPathIndexRunnerReport {
            store,
            write_report,
            validation,
            invalidation_report,
            publication_report,
            published_store_report,
        })
    }

    fn validate_execution_plan(
        execution_plan: &SchemaMutationExecutionPlan,
        target: &SchemaFieldPathIndexRebuildTarget,
    ) -> Result<(), SchemaFieldPathIndexRunnerError> {
        match execution_plan.steps() {
            [
                SchemaMutationExecutionStep::BuildFieldPathIndex {
                    target: planned_target,
                },
                SchemaMutationExecutionStep::ValidatePhysicalWork,
                SchemaMutationExecutionStep::InvalidateRuntimeState,
            ] if planned_target == target => Ok(()),
            [
                SchemaMutationExecutionStep::BuildFieldPathIndex { .. },
                SchemaMutationExecutionStep::ValidatePhysicalWork,
                SchemaMutationExecutionStep::InvalidateRuntimeState,
            ] => Err(SchemaFieldPathIndexRunnerError::TargetMismatch),
            _ => Err(SchemaFieldPathIndexRunnerError::UnsupportedExecutionPlan),
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreOverlay
///
/// Isolated in-memory store overlay for staged field-path index writes. The
/// overlay implements the staged read, write, and rollback contracts without
/// exposing or mutating a runtime-visible `IndexStore`.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated physical-store overlays before IndexStore mutation exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlay {
    store: String,
    entries: BTreeMap<RawIndexKey, RawIndexEntry>,
    pub(super) store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated physical-store overlays before IndexStore mutation exists"
)]
impl SchemaFieldPathIndexStagedStoreOverlay {
    #[must_use]
    pub(in crate::db::schema) fn new(store: &str) -> Self {
        Self {
            store: store.to_string(),
            entries: BTreeMap::new(),
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn from_entries(
        store: &str,
        entries: impl IntoIterator<Item = (RawIndexKey, RawIndexEntry)>,
    ) -> Self {
        Self {
            store: store.to_string(),
            entries: entries.into_iter().collect(),
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub(in crate::db::schema) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexKey) -> Option<&RawIndexEntry> {
        self.entries.get(key)
    }

    #[must_use]
    pub(in crate::db::schema) fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    pub(in crate::db::schema) fn validate_batch(
        &self,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
    ) -> Result<
        SchemaFieldPathIndexStagedStoreOverlayValidation,
        SchemaFieldPathIndexStagedStoreOverlayValidationError,
    > {
        if self.store != batch.store() {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::StoreMismatch);
        }
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::PublishedVisibility);
        }
        if self.entries.len() != batch.entries().len() {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryCountMismatch);
        }

        for entry in batch.entries() {
            let Some(overlay_entry) = self.entries.get(entry.key()) else {
                return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::MissingEntry);
            };
            if overlay_entry != entry.entry() {
                return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryMismatch);
            }
        }

        Ok(SchemaFieldPathIndexStagedStoreOverlayValidation {
            store: self.store.clone(),
            entry_count: self.entries.len(),
            store_visibility: self.store_visibility,
            runner_report: batch.runner_report().clone(),
        })
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexStagedStoreOverlay {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.accepts_store(store)
            .then(|| self.entries.get(key).cloned())
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        if self.accepts_store(store) {
            self.entries.remove(key);
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidationError
///
/// Fail-closed validation reasons for isolated staged-store overlays. These
/// checks keep overlay state from being treated as publication-ready unless it
/// exactly matches the validated staged write batch.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStoreOverlayValidationError {
    StoreMismatch,
    PublishedVisibility,
    EntryCountMismatch,
    MissingEntry,
    EntryMismatch,
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidation
///
/// Positive validation report for an isolated staged-store overlay after it has
/// accepted one staged write batch. This report remains staged-only and does
/// not imply publication readiness.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlayValidation {
    store: String,
    entry_count: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
impl SchemaFieldPathIndexStagedStoreOverlayValidation {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
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
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_overlay_validation(self)
    }
}

///
/// SchemaFieldPathIndexStagedStorePublicationBlocker
///
/// Remaining publication barriers after a staged field-path index overlay has
/// been validated. 0.153 keeps these explicit so overlay validation cannot be
/// mistaken for accepted snapshot publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages publication blockers before staged stores can be published"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStorePublicationBlocker {
    StoreStillStaged,
    PhysicalStateNotValidated,
    RuntimeStateNotInvalidated,
    SnapshotNotPublished,
}

///
/// SchemaFieldPathIndexStagedStorePublicationReadiness
///
/// Fail-closed publication readiness for one validated staged field-path index
/// overlay. A readiness report with blockers is diagnostic only; publication
/// remains disallowed until the store is published and all runner phases have
/// completed.
///

#[allow(
    dead_code,
    reason = "0.153 stages publication readiness before staged stores can be published"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStorePublicationReadiness {
    store: String,
    entry_count: usize,
    blockers: Vec<SchemaFieldPathIndexStagedStorePublicationBlocker>,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages publication readiness before staged stores can be published"
)]
impl SchemaFieldPathIndexStagedStorePublicationReadiness {
    #[must_use]
    fn from_overlay_validation(
        validation: &SchemaFieldPathIndexStagedStoreOverlayValidation,
    ) -> Self {
        Self::from_validated_parts(
            validation.store(),
            validation.entry_count(),
            validation.store_visibility(),
            validation.runner_report(),
        )
    }

    #[must_use]
    fn from_isolated_index_store_validation(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
    ) -> Self {
        Self::from_validated_parts(
            validation.store(),
            validation.entry_count(),
            validation.store_visibility(),
            validation.runner_report(),
        )
    }

    #[must_use]
    fn from_runtime_invalidation_report(
        report: &SchemaFieldPathIndexRuntimeInvalidationReport,
    ) -> Self {
        Self::from_validated_parts(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_snapshot_publication_report(
        report: &SchemaFieldPathIndexSnapshotPublicationReport,
    ) -> Self {
        Self::from_validated_parts(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_validated_parts(
        store: &str,
        entry_count: usize,
        store_visibility: SchemaMutationStoreVisibility,
        runner_report: &SchemaMutationRunnerReport,
    ) -> Self {
        let mut blockers = Vec::new();

        if store_visibility != SchemaMutationStoreVisibility::Published {
            blockers.push(SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged);
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::ValidatePhysicalState) {
            blockers
                .push(SchemaFieldPathIndexStagedStorePublicationBlocker::PhysicalStateNotValidated);
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState) {
            blockers.push(
                SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            );
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot) {
            blockers.push(SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished);
        }

        Self {
            store: store.to_string(),
            entry_count,
            blockers,
            runner_report: runner_report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn blockers(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStorePublicationBlocker] {
        self.blockers.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn allows_publication(&self) -> bool {
        self.blockers.is_empty() && self.runner_report.physical_work_allows_publication()
    }
}

///
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
