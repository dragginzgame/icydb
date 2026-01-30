use crate::{
    db::{
        CommitIndexOp,
        index::{IndexKey, IndexStore, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::Storable,
};
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, thread::LocalKey};

/// Apply commit marker index ops with pre-resolved stores.
pub fn apply_marker_index_ops(
    ops: &[CommitIndexOp],
    stores: Vec<&'static LocalKey<RefCell<IndexStore>>>,
) {
    // SAFETY / INVARIANT:
    // All structural and semantic invariants for these marker ops are fully
    // validated during planning *before* the commit marker is persisted.
    // After marker creation, apply is required to be infallible or trap.
    // Therefore, debug_assert!s here are correctness sentinels, not user errors.
    debug_assert_eq!(
        ops.len(),
        stores.len(),
        "commit marker index ops length mismatch"
    );

    for (op, store) in ops.iter().zip(stores.into_iter()) {
        debug_assert_eq!(op.key.len(), IndexKey::STORED_SIZE as usize);
        let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));

        store.with_borrow_mut(|s| {
            if let Some(value) = &op.value {
                debug_assert!(value.len() <= MAX_INDEX_ENTRY_BYTES as usize);
                let raw_entry = RawIndexEntry::from_bytes(Cow::Borrowed(value.as_slice()));
                s.insert(raw_key, raw_entry);
            } else {
                s.remove(&raw_key);
            }
        });
    }
}

// Resolve and validate a commit marker index op, returning the store and key.
pub(super) fn resolve_index_key(
    stores: &'_ BTreeMap<&'static str, &'static LocalKey<RefCell<IndexStore>>>,
    op: &CommitIndexOp,
    entity_path: &str,
    on_missing: impl FnOnce() -> Option<InternalError>,
) -> Result<(&'static LocalKey<RefCell<IndexStore>>, RawIndexKey), InternalError> {
    // Phase 1: resolve the target store.
    let store = stores.get(op.store.as_str()).ok_or_else(|| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            format!(
                "commit marker references unknown index store '{}' ({})",
                op.store, entity_path
            ),
        )
    })?;

    // Phase 2: validate key and entry sizes.
    if op.key.len() != IndexKey::STORED_SIZE as usize {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            format!(
                "commit marker index key length {} does not match {} ({})",
                op.key.len(),
                IndexKey::STORED_SIZE,
                entity_path
            ),
        ));
    }
    if let Some(value) = &op.value
        && value.len() > MAX_INDEX_ENTRY_BYTES as usize
    {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            format!(
                "commit marker index entry exceeds max size: {} bytes ({})",
                value.len(),
                entity_path
            ),
        ));
    }

    // Phase 3: decode key and fetch existing entry.
    let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
    let existing = store.with_borrow(|s| s.get(&raw_key));
    if existing.is_none()
        && let Some(err) = on_missing()
    {
        return Err(err);
    }

    Ok((*store, raw_key))
}
