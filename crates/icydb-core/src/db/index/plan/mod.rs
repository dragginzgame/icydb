#[cfg(test)]
mod tests;

use crate::{
    db::{
        CommitIndexOp,
        executor::ExecutorError,
        index::{
            IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, IndexKey, IndexStore,
            RawIndexEntry, RawIndexKey,
        },
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, EntityValue, FieldValue, Storable},
};
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

///
/// IndexApplyPlan
///

#[derive(Debug)]
pub struct IndexApplyPlan {
    pub index: &'static IndexModel,
    pub store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
///

#[derive(Debug)]
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
pub fn plan_index_mutation_for_entity<E: EntityKind + EntityValue>(
    db: &crate::db::Db<E::Canister>,
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    let old_entity_key = old.map(|entity| entity.id().into_key());
    let new_entity_key = new.map(|entity| entity.id().into_key());

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    for index in E::INDEXES {
        let store = db.with_index(|reg| reg.try_get_store(index.store))?;

        let old_key = match old {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };
        let new_key = match new {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };

        let old_entry = load_existing_entry(store, index, old)?;
        // Prevalidate membership so commit-phase mutations cannot surface corruption.
        if let Some(old_key) = &old_key {
            let Some(old_entity_key) = old_entity_key else {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    "missing old entity key for index removal".to_string(),
                ));
            };
            let entry = old_entry.as_ref().ok_or_else(|| {
                ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                    ),
                )
            })?;
            if index.unique && entry.len() > 1 {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }
                    ),
                )
                .into());
            }
            if !entry.contains(old_entity_key) {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Index,
                    format!(
                        "index corrupted: {} ({}) -> {}",
                        E::PATH,
                        index.fields.join(", "),
                        IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                    ),
                )
                .into());
            }
        }
        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load_existing_entry(store, index, new)?
        };

        validate_unique_constraint::<E>(
            db,
            index,
            new_entry.as_ref(),
            new_entity_key.as_ref(),
            new,
        )?;

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

fn load_existing_entry<E: EntityKind + EntityValue>(
    store: &'static LocalKey<RefCell<IndexStore>>,
    index: &'static IndexModel,
    entity: Option<&E>,
) -> Result<Option<IndexEntry<E>>, InternalError> {
    let Some(entity) = entity else {
        return Ok(None);
    };
    let Some(key) = IndexKey::new(entity, index)? else {
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
#[expect(clippy::too_many_lines)]
fn validate_unique_constraint<E: EntityKind + EntityValue>(
    db: &crate::db::Db<E::Canister>,
    index: &IndexModel,
    entry: Option<&IndexEntry<E>>,
    new_key: Option<&E::Key>,
    new_entity: Option<&E>,
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

    let Some(new_key) = new_key else {
        return Ok(());
    };
    if entry.contains(*new_key) {
        return Ok(());
    }

    let Some(new_entity) = new_entity else {
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Index,
            "missing entity payload during unique validation".to_string(),
        ));
    };
    let existing_key = entry.single_id().ok_or_else(|| {
        ExecutorError::corruption(
            ErrorOrigin::Index,
            format!(
                "index corrupted: {} ({}) -> {} keys",
                E::PATH,
                index.fields.join(", "),
                entry.len()
            ),
        )
    })?;

    let stored = {
        let data_key = DataKey::try_new::<E>(existing_key)?;
        let row = db.context::<E>().read_strict(&data_key)?;
        row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key} ({err})"),
            )
        })?
    };
    let stored_key = stored.id().into_key();
    if stored_key != existing_key {
        // Stored row decoded successfully but key mismatch indicates index/data divergence; treat as corruption.
        return Err(ExecutorError::corruption(
            ErrorOrigin::Store,
            format!(
                "index corrupted: {} ({}) -> {}",
                E::PATH,
                index.fields.join(", "),
                IndexEntryCorruption::RowKeyMismatch {
                    indexed_key: Box::new(existing_key.to_value()),
                    row_key: Box::new(stored_key.to_value()),
                }
            ),
        )
        .into());
    }

    for field in index.fields {
        let expected = new_entity.get_value(field).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index field missing on lookup entity: {} ({})",
                    E::PATH,
                    field
                ),
            )
        })?;
        let actual = stored.get_value(field).ok_or_else(|| {
            ExecutorError::corruption(
                ErrorOrigin::Index,
                format!(
                    "index corrupted: {} ({}) -> stored entity missing field",
                    E::PATH,
                    field
                ),
            )
        })?;

        if expected != actual {
            return Err(ExecutorError::corruption(
                ErrorOrigin::Index,
                format!("index hash collision: {} ({})", E::PATH, field),
            )
            .into());
        }
    }

    sink::record(MetricsEvent::UniqueViolation {
        entity_path: E::PATH,
    });

    Err(ExecutorError::index_violation(E::PATH, index.fields).into())
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
    old_entry: Option<IndexEntry<E>>,
    new_entry: Option<IndexEntry<E>>,
    old_entity_key: Option<E::Key>,
    new_entity_key: Option<E::Key>,
) -> Result<(), InternalError> {
    let mut touched: BTreeMap<RawIndexKey, Option<IndexEntry<E>>> = BTreeMap::new();
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
            entry.remove(old_entity_key);
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

        entry.insert(new_entity_key);
        touched.insert(raw_key, Some(entry));
    }

    // ── Emit commit ops ────────────────────────

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
