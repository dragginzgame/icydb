use crate::{
    db::{
        CommitIndexOp,
        executor::ExecutorError,
        index::{IndexEntry, IndexKey, IndexStore, RawIndexEntry, RawIndexKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    key::Key,
    model::index::IndexModel,
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, Storable},
};
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// IndexApplyPlan
///

pub struct IndexApplyPlan {
    pub index: &'static IndexModel,
    pub store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
///

pub struct IndexMutationPlan {
    pub apply: Vec<IndexApplyPlan>,
    pub commit_ops: Vec<CommitIndexOp>,
}

/// Plan all index mutations for a single entity transition.
///
/// This function:
/// - Loads existing index entries
/// - Validates unique constraints
/// - Computes the exact index writes/deletes required
///
/// All fallible work happens here. The returned plan is safe to apply
/// infallibly after a commit marker is written.
pub fn plan_index_mutation_for_entity<E: EntityKind>(
    db: &crate::db::Db<E::Canister>,
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    let old_entity_key = old.map(EntityKind::key);
    let new_entity_key = new.map(EntityKind::key);

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    for index in E::INDEXES {
        let store = db.with_index(|reg| reg.try_get_store(index.store))?;

        let old_key = old.and_then(|e| IndexKey::new(e, index));
        let new_key = new.and_then(|e| IndexKey::new(e, index));

        let old_entry = load_existing_entry(store, index, old)?;
        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load_existing_entry(store, index, new)?
        };

        validate_unique_constraint::<E>(index, new_entry.as_ref(), new_entity_key.as_ref())?;

        build_commit_ops_for_index::<E>(
            &mut commit_ops,
            index,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_entity_key,
            new_entity_key,
        )?;

        apply.push(IndexApplyPlan { index, store });
    }

    Ok(IndexMutationPlan { apply, commit_ops })
}

fn load_existing_entry<E: EntityKind>(
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry>, InternalError> {
    let Some(entity) = entity else {
        return Ok(None);
    };
    let Some(key) = IndexKey::new(entity, index) else {
        return Ok(None);
    };

    store
        .with_borrow(|s| s.get(&key.to_raw()))
        .map(|raw| {
            raw.try_decode().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        err
                    ),
                )
                .into()
            })
        })
        .transpose()
}

/// Validate unique index constraints against the existing index entry.
///
/// This detects:
/// - Index corruption (multiple keys in a unique entry)
/// - Uniqueness violations (conflicting key ownership)
fn validate_unique_constraint<E: EntityKind>(
    index: &IndexModel,
    entry: Option<&IndexEntry>,
    new_key: Option<&Key>,
) -> Result<(), InternalError> {
    if !index.unique {
        return Ok(());
    }

    let Some(entry) = entry else {
        return Ok(());
    };

    if entry.len() > 1 {
        return Err(ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        )
        .into());
    }

    if let Some(new_key) = new_key
        && !entry.is_empty()
        && !entry.contains(new_key)
    {
        sink::record(MetricsEvent::UniqueViolation {
            entity_path: E::PATH,
        });

        return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
    }

    Ok(())
}

/// Compute commit-time index operations for a single index.
///
/// Produces a minimal set of index updates:
/// - `Some(bytes)` → insert/update index entry
/// - `None`        → delete index entry
///
/// Correctly handles old/new key overlap and guarantees that
/// apply-time mutations cannot fail except by invariant violation.
#[allow(clippy::too_many_arguments)]
fn build_commit_ops_for_index<E: EntityKind>(
    commit_ops: &mut Vec<CommitIndexOp>,
    index: &'static IndexModel,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_entry: Option<IndexEntry>,
    new_entry: Option<IndexEntry>,
    old_entity_key: Option<Key>,
    new_entity_key: Option<Key>,
) -> Result<(), InternalError> {
    let mut touched: BTreeMap<RawIndexKey, Option<IndexEntry>> = BTreeMap::new();
    let fields = index.fields.join(", ");

    // ── Removal ────────────────────────────────

    if let Some(old_key) = old_key {
        let Some(old_entity_key) = old_entity_key else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Index,
                "missing old entity key for index removal".to_string(),
            ));
        };

        if let Some(mut entry) = old_entry {
            entry.remove_key(&old_entity_key);
            let after = if entry.is_empty() { None } else { Some(entry) };
            touched.insert(old_key.to_raw(), after);
        } else {
            // No existing index entry -> nothing to remove.
            touched.insert(old_key.to_raw(), None);
        }
    }

    // ── Insertion ──────────────────────────────

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
        //   1. result of removal (if same key)
        //   2. existing entry loaded from store
        //   3. brand new entry
        let mut entry = if let Some(existing) = touched.remove(&raw_key) {
            existing.unwrap_or_else(|| IndexEntry::new(new_entity_key))
        } else if let Some(existing) = new_entry {
            existing
        } else {
            IndexEntry::new(new_entity_key)
        };

        entry.insert_key(new_entity_key);
        touched.insert(raw_key, Some(entry));
    }

    // ── Emit commit ops ────────────────────────

    for (raw_key, entry) in touched {
        let value = if let Some(entry) = entry {
            let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry exceeds max keys: {} ({}) -> {} keys",
                        E::PATH,
                        fields,
                        err.keys()
                    ),
                )
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

/// Load and decode the current index entry for an entity, if one exists.
///
/// This is a *planning-phase* helper used during index mutation prevalidation.
/// It performs a read-only lookup of the index store and attempts to decode
/// the raw entry into an `IndexEntry`.
///
/// Semantics:
/// - Returns `Ok(None)` if:
///   - the entity does not participate in this index, or
///   - no index entry exists for the computed index key.
/// - Returns `Ok(Some(IndexEntry))` if a valid entry is present.
/// - Returns `Err` if the raw index data exists but cannot be decoded.
///
/// Error handling:
/// - Decode failures are treated as *index corruption*, not user error.
///   Such corruption must be detected *before* any commit marker is written,
///   ensuring no partial mutations occur.
///
/// Atomicity rationale:
/// This function is intentionally fallible and must only be called during the
/// prevalidation phase. Once the commit marker is persisted, all index/data
/// mutations are assumed infallible (or trap), so any corruption must surface
/// here to preserve Stage-2 atomicity guarantees.
pub fn load_existing_index_entry<E: EntityKind>(
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry>, InternalError> {
    let Some(entity) = entity else {
        return Ok(None);
    };
    let Some(key) = IndexKey::new(entity, index) else {
        return Ok(None);
    };

    store
        .with_borrow(|s| s.get(&key.to_raw()))
        .map(|raw| {
            raw.try_decode().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        err
                    ),
                )
                .into()
            })
        })
        .transpose()
}
