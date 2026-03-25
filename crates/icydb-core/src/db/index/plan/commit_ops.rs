//! Module: index::plan::commit_ops
//! Responsibility: synthesize deterministic index commit operations.
//! Does not own: index-entry loading or uniqueness validation.
//! Boundary: called from index planning after prevalidation succeeds.

use crate::{
    db::{
        commit::{CommitIndexOp, PreparedIndexDeltaKind},
        data::StorageKey,
        index::{IndexEntry, IndexKey, IndexStore, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    model::index::IndexModel,
};
use std::{cell::RefCell, thread::LocalKey};

/// Compute commit-time index operations for a single index.
///
/// Produces a minimal set of index updates:
/// - `Some(raw)` -> insert/update index entry
/// - `None`      -> delete index entry
///
/// Correctly handles old/new key overlap and guarantees that
/// apply-time mutations cannot fail except by invariant violation.
#[expect(clippy::too_many_arguments)]
pub(super) fn build_commit_ops_for_index(
    commit_ops: &mut Vec<CommitIndexOp>,
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity_path: &str,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_entry: Option<IndexEntry>,
    new_entry: Option<IndexEntry>,
    old_entity_key: Option<StorageKey>,
    new_entity_key: Option<StorageKey>,
) -> Result<(), InternalError> {
    let fields = index.fields().join(", ");

    // Phase 1: same-key transitions collapse into one entry mutation.
    if old_key == new_key {
        if let Some(key) = old_key {
            let Some(new_entity_key) = new_entity_key else {
                return Err(InternalError::index_commit_op_new_entity_key_required());
            };

            let mut entry = old_entry.unwrap_or_else(|| IndexEntry::new(new_entity_key));
            if let Some(old_entity_key) = old_entity_key {
                entry.remove(old_entity_key);
            }
            entry.insert(new_entity_key);

            push_commit_op_for_index_entry(
                commit_ops,
                store,
                entity_path,
                &fields,
                key.to_raw(),
                Some(entry),
                PreparedIndexDeltaKind::None,
            )?;
        }

        return Ok(());
    }

    // Phase 2: different-key transitions can touch at most two keys. Preserve
    // deterministic key order without the general BTreeMap machinery.
    let mut first: Option<(RawIndexKey, Option<IndexEntry>, PreparedIndexDeltaKind)> = None;
    let mut second: Option<(RawIndexKey, Option<IndexEntry>, PreparedIndexDeltaKind)> = None;

    if let Some(old_key) = old_key {
        let Some(old_entity_key) = old_entity_key else {
            return Err(InternalError::index_commit_op_old_entity_key_required());
        };

        let after = old_entry.map(|mut entry| {
            entry.remove(old_entity_key);
            entry
        });
        let after = after.filter(|entry| !entry.is_empty());
        insert_commit_candidate(
            &mut first,
            &mut second,
            old_key.to_raw(),
            after,
            PreparedIndexDeltaKind::IndexRemove,
        );
    }

    if let Some(new_key) = new_key {
        let Some(new_entity_key) = new_entity_key else {
            return Err(InternalError::index_commit_op_new_entity_key_required());
        };

        let mut entry = new_entry.unwrap_or_else(|| IndexEntry::new(new_entity_key));
        entry.insert(new_entity_key);
        insert_commit_candidate(
            &mut first,
            &mut second,
            new_key.to_raw(),
            Some(entry),
            PreparedIndexDeltaKind::IndexInsert,
        );
    }

    if let Some((raw_key, entry, delta_kind)) = first {
        push_commit_op_for_index_entry(
            commit_ops,
            store,
            entity_path,
            &fields,
            raw_key,
            entry,
            delta_kind,
        )?;
    }
    if let Some((raw_key, entry, delta_kind)) = second {
        push_commit_op_for_index_entry(
            commit_ops,
            store,
            entity_path,
            &fields,
            raw_key,
            entry,
            delta_kind,
        )?;
    }

    Ok(())
}

/// Insert one touched key into the small fixed-size ordered candidate set.
fn insert_commit_candidate(
    first: &mut Option<(RawIndexKey, Option<IndexEntry>, PreparedIndexDeltaKind)>,
    second: &mut Option<(RawIndexKey, Option<IndexEntry>, PreparedIndexDeltaKind)>,
    raw_key: RawIndexKey,
    entry: Option<IndexEntry>,
    delta_kind: PreparedIndexDeltaKind,
) {
    match first {
        None => *first = Some((raw_key, entry, delta_kind)),
        Some((first_key, _, _)) if raw_key < *first_key => {
            *second = first.take();
            *first = Some((raw_key, entry, delta_kind));
        }
        _ => *second = Some((raw_key, entry, delta_kind)),
    }
}

/// Encode one touched index entry into one deterministic commit operation.
fn push_commit_op_for_index_entry(
    commit_ops: &mut Vec<CommitIndexOp>,
    store: &'static LocalKey<RefCell<IndexStore>>,
    entity_path: &str,
    fields: &str,
    raw_key: RawIndexKey,
    entry: Option<IndexEntry>,
    delta_kind: PreparedIndexDeltaKind,
) -> Result<(), InternalError> {
    let value = if let Some(entry) = entry {
        let raw = RawIndexEntry::try_from(&entry)
            .map_err(|err| err.into_commit_internal_error(entity_path, fields))?;
        Some(raw)
    } else {
        None
    };

    commit_ops.push(CommitIndexOp {
        store,
        key: raw_key,
        value,
        delta_kind,
    });

    Ok(())
}
