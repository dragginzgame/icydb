use crate::{
    db::{
        CommitIndexOp,
        index::{IndexEntry, IndexEntryEncodeError, IndexKey, RawIndexEntry, RawIndexKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, Storable},
};
use std::collections::BTreeMap;

/// Compute commit-time index operations for a single index.
///
/// Produces a minimal set of index updates:
/// - `Some(bytes)` -> insert/update index entry
/// - `None`        -> delete index entry
///
/// Correctly handles old/new key overlap and guarantees that
/// apply-time mutations cannot fail except by invariant violation.
#[expect(clippy::too_many_arguments)]
pub(super) fn build_commit_ops_for_index<E: EntityKind>(
    commit_ops: &mut Vec<CommitIndexOp>,
    index: &'static crate::model::index::IndexModel,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_entry: Option<IndexEntry<E>>,
    new_entry: Option<IndexEntry<E>>,
    old_entity_key: Option<E::Key>,
    new_entity_key: Option<E::Key>,
) -> Result<(), InternalError> {
    let mut touched: BTreeMap<RawIndexKey, Option<IndexEntry<E>>> = BTreeMap::new();
    let fields = index.fields.join(", ");

    // Removal phase.
    if let Some(old_key) = old_key {
        let Some(old_entity_key) = old_entity_key else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Index,
                "missing old entity key for index removal".to_string(),
            ));
        };

        if let Some(mut entry) = old_entry {
            entry.remove(old_entity_key);
            let after = if entry.is_empty() { None } else { Some(entry) };
            touched.insert(old_key.to_raw(), after);
        } else {
            // No existing index entry -> nothing to remove.
            touched.insert(old_key.to_raw(), None);
        }
    }

    // Insertion phase.
    if let Some(new_key) = new_key {
        let Some(new_entity_key) = new_entity_key else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Index,
                "missing new entity key for index insertion".to_string(),
            ));
        };

        let raw_key = new_key.to_raw();

        // Start from:
        // 1. result of removal (if same key)
        // 2. existing entry loaded from store
        // 3. brand new entry
        let mut entry = if let Some(existing) = touched.remove(&raw_key) {
            existing.unwrap_or_else(|| IndexEntry::new(new_entity_key))
        } else if let Some(existing) = new_entry {
            existing
        } else {
            IndexEntry::new(new_entity_key)
        };

        entry.insert(new_entity_key);
        touched.insert(raw_key, Some(entry));
    }

    // Emit commit ops.
    for (raw_key, entry) in touched {
        let value = if let Some(entry) = entry {
            let raw = RawIndexEntry::try_from(&entry).map_err(|err| match err {
                IndexEntryEncodeError::TooManyKeys { keys } => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry exceeds max keys: {} ({}) -> {} keys",
                        E::PATH,
                        fields,
                        keys
                    ),
                ),
                IndexEntryEncodeError::KeyEncoding(err) => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry key encoding failed: {} ({}) -> {err}",
                        E::PATH,
                        fields
                    ),
                ),
            })?;
            Some(raw.into_bytes())
        } else {
            None
        };

        commit_ops.push(CommitIndexOp {
            store: index.store.to_string(),
            key: raw_key.as_bytes().to_vec(),
            value,
        });
    }

    Ok(())
}
