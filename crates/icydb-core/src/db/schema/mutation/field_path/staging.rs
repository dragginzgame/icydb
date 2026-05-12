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
    pub(in crate::db::schema) entries: Vec<SchemaFieldPathIndexStagedEntry>,
    source_rows: usize,
    pub(in crate::db::schema) skipped_rows: usize,
    pub(in crate::db::schema) store_visibility: SchemaMutationStoreVisibility,
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
