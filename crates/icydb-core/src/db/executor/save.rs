use crate::{
    db::{
        Db,
        executor::{ExecutorError, WriteUnit},
        index::{
            IndexEntry, IndexEntryCorruption, IndexInsertOutcome, IndexKey, IndexRemoveOutcome,
            IndexStore, RawIndexEntry,
        },
        query::{SaveMode, SaveQuery},
        store::{DataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::{deserialize, serialize},
    traits::EntityKind,
    validate::validate,
};
use std::{cell::RefCell, marker::PhantomData, thread::LocalKey};

///
/// IndexApplyPlan
///

struct IndexApplyPlan {
    index: &'static IndexModel,
    store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub struct SaveExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub fn insert(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Insert a new view, returning the stored view.
    pub fn insert_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);
        Ok(self.insert(entity)?.to_view())
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.update(entity)?.to_view())
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.replace(entity)?.to_view())
    }

    // ======================================================================
    // Batch save operations (fail-fast, non-atomic)
    // ======================================================================

    pub fn insert_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only with caller idempotency and conflict handling.
        for entity in iter {
            out.push(self.insert(entity)?);
        }

        Ok(out)
    }

    pub fn update_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only if the caller tolerates already-updated rows.
        for entity in iter {
            out.push(self.update(entity)?);
        }

        Ok(out)
    }

    pub fn replace_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only with caller idempotency and conflict handling.
        for entity in iter {
            out.push(self.replace(entity)?);
        }

        Ok(out)
    }

    // ======================================================================
    // Low-level execution
    // ======================================================================

    /// Execute a serialized save query.
    ///
    /// NOTE: Deserialization here is over user-supplied bytes. Failures are
    /// considered invalid input rather than storage corruption.
    pub fn execute(&self, query: SaveQuery) -> Result<E, InternalError> {
        let entity: E = deserialize(&query.bytes)?;
        self.save_entity(query.mode, entity)
    }

    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = self.db.context::<E>();
        let _unit = WriteUnit::new("save_entity_non_atomic");

        // Sanitize & validate before key extraction in case PK fields are normalized
        sanitize(&mut entity)?;
        validate(&entity)?;

        let key = entity.key();
        let data_key = DataKey::new::<E>(key);
        let raw_key = data_key.to_raw();
        let old_result = ctx.with_store(|store| store.get(&raw_key))?;

        let old = match (mode, old_result) {
            (SaveMode::Insert | SaveMode::Replace, None) => None,
            (SaveMode::Update | SaveMode::Replace, Some(old_row)) => {
                Some(old_row.try_decode::<E>().map_err(|err| {
                    ExecutorError::corruption(
                        ErrorOrigin::Serialize,
                        format!("failed to deserialize row: {data_key} ({err})"),
                    )
                })?)
            }
            (SaveMode::Insert, Some(_)) => return Err(ExecutorError::KeyExists(data_key).into()),
            (SaveMode::Update, None) => return Err(ExecutorError::KeyNotFound(data_key).into()),
        };

        let bytes = serialize(&entity)?;
        let row = RawRow::try_new(bytes)?;

        // Preflight data store availability before index mutations.
        ctx.with_store(|_| ())?;

        // Partial-write window:
        // - Phase 1 uniqueness checks are safe (no mutation, retry-safe).
        // - Phase 2 mutates indexes; failures here can leave index divergence.
        // - Data write happens after index updates; failures can orphan indexes.
        // Corruption risk exists if failures occur after index mutation.
        self.replace_indexes(old.as_ref(), &entity)?;

        ctx.with_store_mut(|store| store.insert(raw_key, row))
            .expect("data store missing after preflight");
        span.set_rows(1);

        Ok(entity)
    }

    // ======================================================================
    // Index maintenance
    // ======================================================================

    /// Replace index entries using a two-phase (validate, then mutate) approach
    /// to avoid partial updates on uniqueness violations.
    #[allow(clippy::too_many_lines)]
    fn replace_indexes(&self, old: Option<&E>, new: &E) -> Result<(), InternalError> {
        let plans = self.prevalidate_indexes(old, new)?;

        // FIRST STABLE WRITE: index mutations begin here; apply is infallible or traps.
        for plan in plans {
            let mut removed = false;
            let mut inserted = false;

            plan.store.with_borrow_mut(|s| {
                if let Some(old) = old {
                    let outcome = s
                        .remove_index_entry(old, plan.index)
                        .expect("index remove failed after prevalidation");
                    if outcome == IndexRemoveOutcome::Removed {
                        removed = true;
                    }
                }

                let outcome = s
                    .insert_index_entry(new, plan.index)
                    .expect("index insert failed after prevalidation");
                if outcome == IndexInsertOutcome::Inserted {
                    inserted = true;
                }
            });

            if removed {
                sink::record(MetricsEvent::IndexRemove {
                    entity_path: E::PATH,
                });
            }

            if inserted {
                sink::record(MetricsEvent::IndexInsert {
                    entity_path: E::PATH,
                });
            }
        }

        Ok(())
    }

    fn prevalidate_indexes(
        &self,
        old: Option<&E>,
        new: &E,
    ) -> Result<Vec<IndexApplyPlan>, InternalError> {
        let old_entity_key = old.map(|entity| entity.key());
        let new_entity_key = new.key();
        let mut plans = Vec::with_capacity(E::INDEXES.len());

        for index in E::INDEXES {
            let fields = index.fields.join(", ");
            let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;

            let old_key = old.and_then(|entity| IndexKey::new(entity, index));
            let new_key = IndexKey::new(new, index);
            let old_raw = old_key.as_ref().map(IndexKey::to_raw);
            let new_raw = new_key.as_ref().map(IndexKey::to_raw);

            let mut old_entry: Option<IndexEntry> = None;
            let mut new_entry: Option<IndexEntry> = None;

            if let Some(raw_key) = &old_raw {
                if let Some(raw_entry) = store.with_borrow(|s| s.get(raw_key)) {
                    let entry = raw_entry.try_decode().map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Index,
                            format!("index corrupted: {} ({}) -> {}", E::PATH, &fields, err),
                        )
                    })?;
                    old_entry = Some(entry);
                }
            }

            if let Some(raw_key) = &new_raw {
                if old_raw.as_ref() == Some(raw_key) {
                    new_entry = old_entry.clone();
                } else if let Some(raw_entry) = store.with_borrow(|s| s.get(raw_key)) {
                    let entry = raw_entry.try_decode().map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Index,
                            format!("index corrupted: {} ({}) -> {}", E::PATH, &fields, err),
                        )
                    })?;
                    new_entry = Some(entry);
                }
            }

            if index.unique
                && let Some(entry) = &new_entry
            {
                if entry.len() > 1 {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {} keys",
                            E::PATH,
                            &fields,
                            entry.len()
                        ),
                    )
                    .into());
                }
                if !entry.contains(&new_entity_key) && !entry.is_empty() {
                    sink::record(MetricsEvent::UniqueViolation {
                        entity_path: E::PATH,
                    });

                    return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
                }
            }

            let mut entry_after_remove: Option<IndexEntry> = None;
            if let (Some(_old_key), Some(mut entry), Some(old_entity_key)) =
                (old_key.as_ref(), old_entry.clone(), old_entity_key)
            {
                entry.remove_key(&old_entity_key);
                if entry.is_empty() {
                    entry_after_remove = None;
                } else {
                    let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Index,
                            format!(
                                "index corrupted: {} ({}) -> {}",
                                E::PATH,
                                &fields,
                                IndexEntryCorruption::TooManyKeys { count: err.keys() }
                            ),
                        )
                    })?;
                    drop(raw);
                    entry_after_remove = Some(entry);
                }
            }

            if let Some(_new_key) = new_key.as_ref() {
                let base_entry = if old_raw.is_some() && old_raw == new_raw {
                    entry_after_remove.clone()
                } else {
                    new_entry.clone()
                };

                match base_entry {
                    Some(mut entry) => {
                        if index.unique {
                            if entry.len() > 1 {
                                return Err(ExecutorError::corruption(
                                    ErrorOrigin::Index,
                                    format!(
                                        "index corrupted: {} ({}) -> {} keys",
                                        E::PATH,
                                        &fields,
                                        entry.len()
                                    ),
                                )
                                .into());
                            }
                            if entry.contains(&new_entity_key) {
                                // Skip (no mutation).
                            } else if !entry.is_empty() {
                                sink::record(MetricsEvent::UniqueViolation {
                                    entity_path: E::PATH,
                                });

                                return Err(
                                    ExecutorError::index_violation(E::PATH, index.fields).into()
                                );
                            } else {
                                let entry = IndexEntry::new(new_entity_key);
                                let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                                    InternalError::new(
                                        ErrorClass::Unsupported,
                                        ErrorOrigin::Index,
                                        format!(
                                            "index entry exceeds max keys: {} ({}) -> {} keys",
                                            E::PATH,
                                            &fields,
                                            err.keys()
                                        ),
                                    )
                                })?;
                                drop(raw);
                            }
                        } else {
                            entry.insert_key(new_entity_key);
                            let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                                InternalError::new(
                                    ErrorClass::Unsupported,
                                    ErrorOrigin::Index,
                                    format!(
                                        "index entry exceeds max keys: {} ({}) -> {} keys",
                                        E::PATH,
                                        &fields,
                                        err.keys()
                                    ),
                                )
                            })?;
                            drop(raw);
                        }
                    }
                    None => {
                        let entry = IndexEntry::new(new_entity_key);
                        let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                            InternalError::new(
                                ErrorClass::Unsupported,
                                ErrorOrigin::Index,
                                format!(
                                    "index entry exceeds max keys: {} ({}) -> {} keys",
                                    E::PATH,
                                    &fields,
                                    err.keys()
                                ),
                            )
                        })?;
                        drop(raw);
                    }
                }
            }

            plans.push(IndexApplyPlan { index, store });
        }

        Ok(())
    }
}
