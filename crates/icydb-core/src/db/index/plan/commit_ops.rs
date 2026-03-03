//! Module: index::plan::commit_ops
//! Responsibility: synthesize deterministic index commit operations.
//! Does not own: index-entry loading or uniqueness validation.
//! Boundary: called from index planning after prevalidation succeeds.

use crate::{
    db::{
        commit::CommitIndexOp,
        index::{IndexEntry, IndexEntryEncodeError, IndexKey, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    model::index::IndexModel,
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
    index: &'static IndexModel,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_entry: Option<IndexEntry<E>>,
    new_entry: Option<IndexEntry<E>>,
    old_entity_key: Option<E::Key>,
    new_entity_key: Option<E::Key>,
) -> Result<(), InternalError> {
    // Phase 1: model old/new membership transitions in memory.
    let mut touched: BTreeMap<RawIndexKey, Option<IndexEntry<E>>> = BTreeMap::new();
    let fields = index.fields.join(", ");

    // Removal phase.
    if let Some(old_key) = old_key {
        let Some(old_entity_key) = old_entity_key else {
            return Err(InternalError::index_internal(
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
            return Err(InternalError::index_internal(
                "missing new entity key for index insertion".to_string(),
            ));
        };

        let raw_key = new_key.to_raw();

        // Start insertion from removal outcome for same-key transitions when
        // available; otherwise use loaded entry fallback or initialize fresh.
        let mut entry =
            derive_initial_entry_for_insert(&mut touched, &raw_key, new_entry, new_entity_key);

        entry.insert(new_entity_key);
        touched.insert(raw_key, Some(entry));
    }

    // Phase 2: encode touched entries into commit operations.
    for (raw_key, entry) in touched {
        let value = if let Some(entry) = entry {
            let raw = RawIndexEntry::try_from(&entry).map_err(|err| match err {
                IndexEntryEncodeError::TooManyKeys { keys } => {
                    InternalError::index_unsupported(format!(
                        "index entry exceeds max keys: {} ({}) -> {} keys",
                        E::PATH,
                        fields,
                        keys
                    ))
                }
                IndexEntryEncodeError::DuplicateKey => InternalError::index_invariant(format!(
                    "index entry unexpectedly contains duplicate keys: {} ({})",
                    E::PATH,
                    fields
                )),
                IndexEntryEncodeError::KeyEncoding(err) => {
                    InternalError::index_unsupported(format!(
                        "index entry key encoding failed: {} ({}) -> {err}",
                        E::PATH,
                        fields
                    ))
                }
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

// Derive one insertion baseline entry under old/new key overlap semantics.
fn derive_initial_entry_for_insert<E: EntityKind>(
    touched: &mut BTreeMap<RawIndexKey, Option<IndexEntry<E>>>,
    raw_key: &RawIndexKey,
    new_entry: Option<IndexEntry<E>>,
    new_entity_key: E::Key,
) -> IndexEntry<E> {
    if let Some(existing) = touched.remove(raw_key) {
        return existing.unwrap_or_else(|| IndexEntry::new(new_entity_key));
    }

    if let Some(existing) = new_entry {
        return existing;
    }

    IndexEntry::new(new_entity_key)
}
