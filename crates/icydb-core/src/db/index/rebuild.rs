use crate::{
    db::{
        Db, ensure_recovered,
        executor::Context,
        index::{
            IndexEntry, IndexEntryEncodeError, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
        },
        query::{ReadConsistency, plan::AccessPath},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
    types::Ref,
};
use std::collections::{BTreeMap, BTreeSet};

/// Rebuild all indexes for a single entity type, regenerating fingerprints.
#[allow(dead_code)]
pub fn rebuild_indexes_for_entity<E>(db: &Db<E::Canister>) -> Result<(), InternalError>
where
    E: EntityKind<PrimaryKey = Ref<E>>,
{
    // Phase 1: recovery guard to avoid rebuilding from partial commit state.
    ensure_recovered(db)?;

    // Phase 2: load authoritative entity rows from the data store.
    let ctx = Context::<E>::new(db);
    let rows = ctx.rows_from_access(&AccessPath::FullScan, ReadConsistency::MissingOk)?;
    let entities = ctx.deserialize_rows(rows)?;

    // Phase 3: clear index stores (entry + fingerprint) before rebuild.
    // When fingerprint derivation changes, rebuild is mandatory; old fingerprints
    // must not be reused or partially updated.
    let mut cleared = BTreeSet::new();
    for index in E::INDEXES {
        if cleared.insert(index.store) {
            let store = db.with_index(|reg| reg.try_get_store(index.store))?;
            store.with_borrow_mut(IndexStore::clear);
        }
    }

    // Phase 4: rebuild entries and fingerprints for each index.
    for index in E::INDEXES {
        let mut entries: BTreeMap<RawIndexKey, IndexEntry> = BTreeMap::new();
        for (key, entity) in &entities {
            let Some(index_key) = IndexKey::new(entity, index)? else {
                continue;
            };
            let raw_key = index_key.to_raw();
            entries
                .entry(raw_key)
                .and_modify(|entry| entry.insert_key(*key))
                .or_insert_with(|| IndexEntry::new(*key));
        }

        let mut writes = Vec::with_capacity(entries.len());
        for (raw_key, entry) in entries {
            if index.unique && entry.len() > 1 {
                return Err(InternalError::new(
                    ErrorClass::Conflict,
                    ErrorOrigin::Index,
                    format!(
                        "unique index '{}' rebuild encountered {} keys",
                        index.name,
                        entry.len()
                    ),
                ));
            }
            let raw_entry = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
                IndexEntryEncodeError::TooManyKeys { keys } => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry exceeds max keys during rebuild: {} ({}) -> {} keys",
                        E::PATH,
                        index.fields.join(", "),
                        keys
                    ),
                ),
                IndexEntryEncodeError::KeyEncoding(err) => InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index entry key encoding failed during rebuild: {} ({}) -> {err}",
                        E::PATH,
                        index.fields.join(", ")
                    ),
                ),
            })?;
            writes.push((raw_key, raw_entry));
        }

        let store = db.with_index(|reg| reg.try_get_store(index.store))?;
        store.with_borrow_mut(|s| {
            // Use the normal insert path so fingerprints are regenerated using
            // the same helper as live mutations.
            for (raw_key, raw_entry) in writes {
                let _ = s.insert(raw_key, raw_entry);
            }
        });
    }

    Ok(())
}
