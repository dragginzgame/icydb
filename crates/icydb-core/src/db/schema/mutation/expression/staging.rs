use super::*;

///
/// SchemaExpressionIndexRebuildRow
///
/// One authoritative row exposed to the expression-index rebuild staging
/// primitive. The row is already decoded behind a canonical slot-reader
/// contract; staging derives accepted expression keys from these row slots.
///

#[derive(Clone, Copy)]
pub(in crate::db::schema) struct SchemaExpressionIndexRebuildRow<'a> {
    primary_key_value: PrimaryKeyValue,
    slots: &'a dyn CanonicalSlotReader,
}

impl<'a> SchemaExpressionIndexRebuildRow<'a> {
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) fn new(
        primary_key_value: impl Into<PrimaryKeyValue>,
        slots: &'a dyn CanonicalSlotReader,
    ) -> Self {
        Self {
            primary_key_value: primary_key_value.into(),
            slots,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn primary_key_value(self) -> PrimaryKeyValue {
        self.primary_key_value
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
/// work. It remains staged until the runner validates and publishes it.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedEntry {
    key: RawIndexStoreKey,
    entry: IndexEntryValue,
}

impl SchemaExpressionIndexStagedEntry {
    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }

    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) const fn entry(&self) -> &IndexEntryValue {
        &self.entry
    }
}

///
/// SchemaExpressionIndexStagedRebuild
///
/// In-memory staged expression-index state. This is not a published store and
/// must not be made planner-visible until validation and publication complete.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedRebuild {
    target: SchemaExpressionIndexRebuildTarget,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

impl SchemaExpressionIndexStagedRebuild {
    pub(in crate::db::schema) fn from_rows<'a>(
        _entity_path: &str,
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
                row.primary_key_value(),
                &target,
                row.slots(),
            )?
            else {
                skipped_rows = skipped_rows.saturating_add(1);
                continue;
            };
            let raw_entry = IndexEntryValue::presence();

            entries.push(SchemaExpressionIndexStagedEntry {
                key: key.to_raw()?,
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
    #[cfg(test)]
    pub(in crate::db::schema) const fn target(&self) -> &SchemaExpressionIndexRebuildTarget {
        &self.target
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaExpressionIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn source_rows(&self) -> usize {
        self.source_rows
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn skipped_rows(&self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    #[cfg(test)]
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
}

fn has_duplicate_unique_components(
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<bool, SchemaExpressionIndexStagedValidationError> {
    staged_index_keys_have_duplicate_unique_components(
        entries.iter().map(SchemaExpressionIndexStagedEntry::key),
    )
    .map_err(|SchemaStagedIndexValidationError::IndexKeyDecode| {
        SchemaExpressionIndexStagedValidationError::IndexKeyDecode
    })
}

///
/// SchemaExpressionIndexStagedValidationError
///
/// Fail-closed validation reasons for staged expression-index rebuild output.
///

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexStagedValidation {
    entry_count: usize,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

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
    #[cfg(test)]
    pub(in crate::db::schema) const fn skipped_rows(&self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }
}
