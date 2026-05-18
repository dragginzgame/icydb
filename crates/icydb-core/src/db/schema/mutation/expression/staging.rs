use super::*;

///
/// SchemaExpressionIndexRebuildRow
///
/// One authoritative row exposed to the expression-index rebuild staging
/// primitive. The row is already decoded behind a canonical slot-reader
/// contract; staging derives accepted expression keys from these row slots.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild row inputs before physical runners own row iteration"
)]
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct SchemaExpressionIndexRebuildRow<'a> {
    storage_key: StorageKey,
    slots: &'a dyn CanonicalSlotReader,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild row inputs before physical runners own row iteration"
)]
impl<'a> SchemaExpressionIndexRebuildRow<'a> {
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
/// SchemaExpressionIndexStagedEntry
///
/// One raw index-store entry produced during staged expression-index rebuild
/// work. It remains in memory until later runner phases validate and publish
/// it.
///

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression rebuild entries before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedEntry {
    key: RawIndexKey,
    entry: RawIndexEntry,
}

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression rebuild entries before physical runners publish stores"
)]
impl SchemaExpressionIndexStagedEntry {
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
/// SchemaExpressionIndexStagedRebuild
///
/// In-memory staged expression-index state. This is not a published store and
/// must not be made planner-visible until validation and publication complete.
///

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression rebuild output before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedRebuild {
    target: SchemaExpressionIndexRebuildTarget,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.157 stages in-memory expression rebuild output before physical runners publish stores"
)]
impl SchemaExpressionIndexStagedRebuild {
    pub(in crate::db::schema) fn from_rows<'a>(
        entity_path: &str,
        entity_tag: EntityTag,
        target: SchemaExpressionIndexRebuildTarget,
        predicate_program: Option<&PredicateProgram>,
        rows: impl IntoIterator<Item = SchemaExpressionIndexRebuildRow<'a>>,
    ) -> Result<Self, InternalError> {
        let mut entries = Vec::new();
        let mut source_rows = 0usize;
        let mut skipped_rows = 0usize;

        for row in rows {
            source_rows = source_rows.saturating_add(1);
            if let Some(predicate_program) = predicate_program
                && !predicate_program.eval_with_structural_slot_reader(row.slots())?
            {
                skipped_rows = skipped_rows.saturating_add(1);
                continue;
            }
            let Some(key) = IndexKey::new_from_slots_with_expression_rebuild_target(
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

            entries.push(SchemaExpressionIndexStagedEntry {
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
    pub(in crate::db::schema) const fn target(&self) -> &SchemaExpressionIndexRebuildTarget {
        &self.target
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaExpressionIndexStagedEntry] {
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
    ) -> Result<SchemaExpressionIndexStagedValidation, SchemaExpressionIndexStagedValidationError>
    {
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaExpressionIndexStagedValidationError::PublishedVisibility);
        }

        let expected_entries = self
            .source_rows
            .checked_sub(self.skipped_rows)
            .ok_or(SchemaExpressionIndexStagedValidationError::SkippedRowsExceedSourceRows)?;
        if expected_entries != self.entries.len() {
            return Err(SchemaExpressionIndexStagedValidationError::EntryCountMismatch);
        }

        if !self
            .entries
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
        {
            return Err(SchemaExpressionIndexStagedValidationError::UnsortedOrDuplicateEntries);
        }
        if self.target.unique() && has_duplicate_unique_components(self.entries.as_slice())? {
            return Err(SchemaExpressionIndexStagedValidationError::DuplicateUniqueKey);
        }

        Ok(SchemaExpressionIndexStagedValidation {
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

        Ok(SchemaMutationRunnerReport::expression_index_staged(
            step_count,
            execution_plan.runner_capabilities(),
            validation,
        ))
    }
}

fn has_duplicate_unique_components(
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<bool, SchemaExpressionIndexStagedValidationError> {
    for pair in entries.windows(2) {
        let left = staged_index_key(pair[0].key())?;
        let right = staged_index_key(pair[1].key())?;
        if same_unique_components(&left, &right) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn staged_index_key(
    raw_key: &RawIndexKey,
) -> Result<IndexKey, SchemaExpressionIndexStagedValidationError> {
    IndexKey::try_from_raw(raw_key)
        .map_err(|_| SchemaExpressionIndexStagedValidationError::IndexKeyDecode)
}

fn same_unique_components(left: &IndexKey, right: &IndexKey) -> bool {
    left.has_same_components(right)
}

///
/// SchemaExpressionIndexStagedValidationError
///
/// Fail-closed validation reasons for staged expression-index rebuild output.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild validation before physical runners consume it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaExpressionIndexStagedValidationError {
    PublishedVisibility,
    SkippedRowsExceedSourceRows,
    EntryCountMismatch,
    UnsortedOrDuplicateEntries,
    DuplicateUniqueKey,
    IndexKeyDecode,
}

///
/// SchemaExpressionIndexStagedValidation
///
/// Positive validation report for an in-memory staged expression-index rebuild.
///

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild validation before physical runners consume it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedValidation {
    entry_count: usize,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.157 stages expression rebuild validation before physical runners consume it"
)]
impl SchemaExpressionIndexStagedValidation {
    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
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
}
