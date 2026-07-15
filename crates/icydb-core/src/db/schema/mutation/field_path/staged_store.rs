use super::*;

///
/// SchemaFieldPathIndexStagedStore
///
/// In-memory staged store image for one field-path index rebuild. This is the
/// first writer-facing shape after staged rebuild validation: it binds raw
/// index entries to the accepted store identity without mutating `IndexStore`
/// or making rebuilt state runtime-visible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStore {
    store: String,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    validation: SchemaFieldPathIndexStagedValidation,
}

impl SchemaFieldPathIndexStagedStore {
    pub(in crate::db::schema) fn from_rebuild(
        rebuild: &SchemaFieldPathIndexStagedRebuild,
    ) -> Result<Self, SchemaFieldPathIndexStagedValidationError> {
        let validation = rebuild.validate()?;

        Ok(Self {
            store: rebuild.target().store().to_string(),
            entries: rebuild.entries().to_vec(),
            validation,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn validation(&self) -> SchemaFieldPathIndexStagedValidation {
        self.validation
    }

    #[cfg(test)]
    pub(in crate::db::schema) fn write_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreWriter,
    ) {
        for entry in &self.entries {
            writer.write_staged_entry(&self.store, entry.key(), entry.entry());
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
            validation: self.validation,
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

pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexStoreKey) -> Option<IndexEntryValue>;
}

///
/// SchemaFieldPathIndexStagedStoreWriter
///
/// Write-intent sink for one validated staged field-path index-store buffer.
/// The contract exposes accepted store identity and raw index entries while
/// keeping concrete physical mutation behind the isolated-store boundary.
///

pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey, entry: &IndexEntryValue);
}

///
/// SchemaFieldPathIndexStagedStoreWriteBatch
///
/// Isolated staged write batch for one field-path index rebuild. It carries the
/// accepted raw write intents plus rollback snapshots captured from the
/// physical read view. The isolated-store writer consumes this batch.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreWriteBatch {
    store: String,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    rollback_snapshots: Vec<SchemaFieldPathIndexStagedStoreRollbackSnapshot>,
    validation: SchemaFieldPathIndexStagedValidation,
}

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
    #[cfg(test)]
    pub(in crate::db::schema) const fn rollback_snapshots(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStoreRollbackSnapshot] {
        self.rollback_snapshots.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(&self) -> SchemaFieldPathIndexStagedValidation {
        self.validation
    }

    pub(in crate::db::schema) fn write_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreWriter,
    ) {
        for entry in &self.entries {
            writer.write_staged_entry(&self.store, entry.key(), entry.entry());
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
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreRollbackSnapshot
///
/// Previous physical entry captured before one staged raw index write. The
/// rollback phase replays these snapshots in reverse write order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreRollbackSnapshot {
    store: String,
    key: RawIndexStoreKey,
    previous_entry: Option<IndexEntryValue>,
}

impl SchemaFieldPathIndexStagedStoreRollbackSnapshot {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn previous_entry(&self) -> Option<&IndexEntryValue> {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreRollbackPlan {
    store: String,
    actions: Vec<SchemaFieldPathIndexStagedStoreRollbackAction>,
}

impl SchemaFieldPathIndexStagedStoreRollbackPlan {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn actions(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStoreRollbackAction] {
        self.actions.as_slice()
    }

    pub(in crate::db::schema) fn rollback_to(
        &self,
        writer: &mut impl SchemaFieldPathIndexStagedStoreRollbackWriter,
    ) {
        for action in &self.actions {
            match action {
                SchemaFieldPathIndexStagedStoreRollbackAction::Restore { store, key, entry } => {
                    writer.restore_staged_entry(store, key, entry);
                }
                SchemaFieldPathIndexStagedStoreRollbackAction::Remove { store, key } => {
                    writer.remove_staged_entry(store, key);
                }
            }
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

pub(in crate::db::schema) trait SchemaFieldPathIndexStagedStoreRollbackWriter {
    fn restore_staged_entry(
        &mut self,
        store: &str,
        key: &RawIndexStoreKey,
        entry: &IndexEntryValue,
    );

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey);
}

/// SchemaFieldPathIndexStagedStoreRollbackAction
///
/// One physical rollback action derived from a prior-entry snapshot. `Restore`
/// replaces an overwritten entry; `Remove` deletes a key that did not exist
/// before staged physical writes.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStoreRollbackAction {
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
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        match self {
            Self::Restore { store, .. } | Self::Remove { store, .. } => store.as_str(),
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        match self {
            Self::Restore { key, .. } | Self::Remove { key, .. } => key,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn restore_entry(&self) -> Option<&IndexEntryValue> {
        match self {
            Self::Restore { entry, .. } => Some(entry),
            Self::Remove { .. } => None,
        }
    }
}
