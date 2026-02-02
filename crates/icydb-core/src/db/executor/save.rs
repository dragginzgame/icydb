use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit,
        ensure_recovered,
        executor::{
            ExecutorError,
            commit_ops::{apply_marker_index_ops, resolve_index_key},
            trace::{QueryTraceSink, TraceExecutorKind, start_exec_trace},
        },
        finish_commit,
        index::{
            IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
            plan::{IndexApplyPlan, plan_index_mutation_for_entity},
        },
        query::{
            SaveMode,
            plan::refs::EntityReferences,
            predicate::validate::{SchemaInfo, literal_matches_type},
        },
        store::{DataKey, DataStore, EntityRef, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::EntityFieldKind,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::serialize,
    traits::{EntityKind, FieldValue, Path, Storable},
    validate::validate,
    value::Value,
};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    sync::OnceLock,
    thread::LocalKey,
};

// Debug assertions below are diagnostic sentinels; correctness is enforced by
// runtime validation earlier in the pipeline.

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub struct SaveExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    // Debug is session-scoped via DbSession and propagated into executors;
    // executors do not expose independent debug control.
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            trace: None,
            _marker: PhantomData,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_trace_sink(
        mut self,
        sink: Option<&'static dyn QueryTraceSink>,
    ) -> Self {
        self.trace = sink;
        self
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("[debug] {}", s.into());
        }
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

    #[expect(clippy::too_many_lines)]
    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, InternalError> {
        let mut commit_started = false;
        let trace = start_exec_trace(
            self.trace,
            TraceExecutorKind::Save,
            E::PATH,
            None,
            Some(save_mode_tag(mode)),
        );
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = self.db.context::<E>();

            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered(&self.db)?;

            // Sanitize & validate before key extraction in case PK fields are normalized
            sanitize(&mut entity)?;
            validate(&entity)?;
            Self::ensure_entity_invariants(&entity)?;
            self.ensure_reference_targets(&entity)?;

            let key = entity.id();
            let data_key = DataKey::try_new::<E>(key)?;
            let raw_key = data_key.to_raw()?;

            self.debug_log(format!("save {:?} on {} (key={})", mode, E::PATH, data_key));
            let (old, old_raw) = match mode {
                SaveMode::Insert => {
                    // Inserts must not load or decode existing rows; absence is expected.
                    if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                        let stored = existing.try_decode::<E>().map_err(|err| {
                            ExecutorError::corruption(
                                ErrorOrigin::Serialize,
                                format!("failed to deserialize row: {data_key} ({err})"),
                            )
                        })?;

                        let expected = data_key.try_id::<E>()?;
                        let actual = stored.id();
                        if expected != actual {
                            return Err(ExecutorError::corruption(
                                ErrorOrigin::Store,
                                format!(
                                    "row key mismatch: expected {expected:?}, found {actual:?}",
                                ),
                            )
                            .into());
                        }

                        return Err(ExecutorError::KeyExists(data_key).into());
                    }

                    (None, None)
                }
                SaveMode::Update => {
                    let Some(old_row) = ctx.with_store(|store| store.get(&raw_key))? else {
                        return Err(InternalError::store_not_found(data_key.to_string()));
                    };
                    let old = old_row.try_decode::<E>().map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Serialize,
                            format!("failed to deserialize row: {data_key} ({err})"),
                        )
                    })?;
                    let expected = data_key.try_id::<E>()?;
                    let actual = old.id();
                    if expected != actual {
                        return Err(ExecutorError::corruption(
                            ErrorOrigin::Store,
                            format!("row key mismatch: expected {expected:?}, found {actual:?}",),
                        )
                        .into());
                    }
                    (Some(old), Some(old_row))
                }
                SaveMode::Replace => {
                    let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                    let old = old_row
                        .as_ref()
                        .map(|row| {
                            row.try_decode::<E>().map_err(|err| {
                                ExecutorError::corruption(
                                    ErrorOrigin::Serialize,
                                    format!("failed to deserialize row: {data_key} ({err})"),
                                )
                            })
                        })
                        .transpose()?;
                    if let Some(old) = old.as_ref() {
                        let expected = data_key.try_id::<E>()?;
                        let actual = old.id();
                        if expected != actual {
                            return Err(ExecutorError::corruption(
                                ErrorOrigin::Store,
                                format!(
                                    "row key mismatch: expected {expected:?}, found {actual:?}",
                                ),
                            )
                            .into());
                        }
                    }
                    (old, old_row)
                }
            };

            let bytes = serialize(&entity)?;
            let row = RawRow::try_new(bytes)?;

            // Preflight data store availability before index mutations.
            ctx.with_store(|_| ())?;

            // Stage-2 atomicity:
            // Prevalidate index/data mutations before the commit marker is written.
            // After the marker is persisted, mutations run inside a WriteUnit so
            // failures roll back before the marker is cleared.
            let index_plan =
                plan_index_mutation_for_entity::<E>(&self.db, old.as_ref(), Some(&entity))?;
            let data_op = CommitDataOp {
                store: E::DataStore::PATH.to_string(),
                key: raw_key.as_bytes().to_vec(),
                value: Some(row.as_bytes().to_vec()),
            };
            let marker = CommitMarker::new(CommitKind::Save, index_plan.commit_ops, vec![data_op])?;
            let (index_apply_stores, index_rollback_ops) =
                Self::prepare_index_save_ops(&index_plan.apply, &marker.index_ops)?;
            let (index_removes, index_inserts) = Self::plan_index_metrics(old.as_ref(), &entity)?;
            let data_rollback_ops = Self::prepare_data_save_ops(&marker.data_ops, old_raw)?;
            if index_apply_stores.len() != marker.index_ops.len() {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index ops length mismatch: {} ops vs {} stores ({})",
                        marker.index_ops.len(),
                        index_apply_stores.len(),
                        E::PATH
                    ),
                ));
            }
            let data_store = self
                .db
                .with_data(|reg| reg.try_get_store(E::DataStore::PATH))?;
            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Save commit window opened");

            // FIRST STABLE WRITE: commit marker is persisted before any mutations.
            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("save_entity_atomic");
                let index_rollback_ops = index_rollback_ops;
                unit.record_rollback(move || Self::apply_index_rollbacks(index_rollback_ops));
                apply_marker_index_ops(&guard.marker.index_ops, index_apply_stores);
                for _ in 0..index_removes {
                    sink::record(MetricsEvent::IndexRemove {
                        entity_path: E::PATH,
                    });
                }
                for _ in 0..index_inserts {
                    sink::record(MetricsEvent::IndexInsert {
                        entity_path: E::PATH,
                    });
                }

                #[cfg(test)]
                unit.checkpoint("save_entity_after_indexes")?;

                let data_rollback_ops = data_rollback_ops;
                unit.record_rollback(move || {
                    Self::apply_data_rollbacks(data_store, data_rollback_ops);
                });
                Self::apply_marker_data_ops(&guard.marker.data_ops, data_store);

                span.set_rows(1);
                unit.commit()?;

                Ok(())
            })?;

            self.debug_log("Save committed");

            Ok(entity)
        })();

        if commit_started && result.is_err() {
            self.debug_log("Save failed; rollback applied");
        }

        if let Some(trace) = trace {
            match &result {
                Ok(_) => trace.finish(1),
                Err(err) => trace.error(err),
            }
        }

        result
    }

    // Cache schema validation per entity type to keep invariant checks fast.
    // Note: these trait boundaries may be sealed in a future major version.
    fn ensure_entity_invariants(entity: &E) -> Result<(), InternalError> {
        let schema = Self::schema_info()?;

        Self::validate_entity_invariants(entity, schema)
    }

    fn ensure_reference_targets(&self, entity: &E) -> Result<(), InternalError> {
        // Strong references only: direct `Ref<T>` and `Option<Ref<T>>` fields.
        // Nested and collection references are weak in 0.6 and are not validated.
        let refs = entity.entity_refs()?;
        for reference in refs {
            self.ensure_target_exists(reference)?;
        }

        Ok(())
    }

    fn ensure_target_exists(&self, reference: EntityRef) -> Result<(), InternalError> {
        // Phase 1: resolve the referenced entity metadata.
        let entry = self
            .db
            .entity_registry()
            .iter()
            .find(|entry| entry.entity_path == reference.target_path)
            .ok_or_else(|| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("reference target not registered: {}", reference.target_path),
                )
            })?;

        // Phase 2: ensure the referenced row exists in the target store.
        let store = self
            .db
            .with_data(|reg| reg.try_get_store(entry.store_path))?;

        // Scan for the key without resolving entity names or schema metadata.
        let exists = store.with_borrow(|s| -> Result<bool, InternalError> {
            for store_entry in s.iter() {
                let data_key = DataKey::try_from_raw(store_entry.key()).map_err(|err| {
                    InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Store,
                        format!(
                            "corrupted data key while checking reference: {} ({err})",
                            reference.target_path
                        ),
                    )
                })?;

                if data_key.storage_key() == reference.storage_key() {
                    return Ok(true);
                }
            }

            Ok(false)
        })?;

        if !exists {
            return Err(InternalError::new(
                ErrorClass::Conflict,
                ErrorOrigin::Executor,
                format!(
                    "missing referenced entity: {} key={}",
                    reference.target_path,
                    reference.storage_key()
                ),
            ));
        }

        Ok(())
    }

    // Cache schema validation results per entity type.
    fn schema_info() -> Result<&'static SchemaInfo, InternalError> {
        static CACHE: OnceLock<Result<SchemaInfo, CachedInvariant>> = OnceLock::new();
        let cached = CACHE.get_or_init(|| {
            SchemaInfo::from_entity_model(E::MODEL).map_err(|err| {
                CachedInvariant::from_error(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("entity schema invalid for {}: {err}", E::PATH),
                ))
            })
        });

        match cached {
            Ok(schema) => Ok(schema),
            Err(err) => Err(err.to_error()),
        }
    }

    // Enforce trait boundary invariants for user-provided entities.
    fn validate_entity_invariants(entity: &E, schema: &SchemaInfo) -> Result<(), InternalError> {
        let key = entity.id();

        // Phase 1: validate primary key field presence and value.
        let expected = key.to_value();
        let actual = entity.get_value(E::PRIMARY_KEY).ok_or_else(|| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "entity primary key field missing: {} field={}",
                    E::PATH,
                    E::PRIMARY_KEY
                ),
            )
        })?;
        if actual != expected {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "entity primary key field mismatch: {} expected={expected:?} actual={actual:?}",
                    E::PATH
                ),
            ));
        }

        // Phase 2: validate field presence and runtime value shapes.
        let indexed_fields = indexed_field_set::<E>();
        for field in E::MODEL.fields {
            let value = entity.get_value(field.name).ok_or_else(|| {
                let note = if indexed_fields.contains(field.name) {
                    " (indexed)"
                } else {
                    ""
                };
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "entity field missing: {} field={}{}",
                        E::PATH,
                        field.name,
                        note
                    ),
                )
            })?;

            if matches!(value, Value::None) {
                continue;
            }

            if matches!(value, Value::Unit) {
                // Unit is an executor-only sentinel for singleton presence; skip type checks.
                continue;
            }

            if matches!(field.kind, EntityFieldKind::Unsupported) {
                // Unsupported fields accept any runtime value; comparisons treat mismatches as
                // incomparable, so we intentionally skip type validation here.
                continue;
            }

            if matches!(value, Value::Unsupported) {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "entity field value is unsupported: {} field={}",
                        E::PATH,
                        field.name
                    ),
                ));
            }

            let Some(field_type) = schema.field(field.name) else {
                // Field is not part of schema (runtime-only); treat as unsupported.
                continue;
            };
            if !literal_matches_type(&value, field_type) {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "entity field type mismatch: {} field={} value={value:?}",
                        E::PATH,
                        field.name
                    ),
                ));
            }
        }

        Ok(())
    }

    // ======================================================================
    // Commit-marker apply (mechanical)
    // ======================================================================

    /// Precompute index mutation metrics before the commit marker is persisted.
    fn plan_index_metrics(old: Option<&E>, new: &E) -> Result<(usize, usize), InternalError> {
        let mut removes = 0usize;
        let mut inserts = 0usize;

        for index in E::INDEXES {
            if let Some(old) = old
                && IndexKey::new(old, index)?.is_some()
            {
                removes = removes.saturating_add(1);
            }
            if IndexKey::new(new, index)?.is_some() {
                inserts = inserts.saturating_add(1);
            }
        }

        Ok((removes, inserts))
    }

    /// Resolve commit index ops into stores and capture rollback entries.
    #[allow(clippy::type_complexity)]
    fn prepare_index_save_ops(
        plans: &[IndexApplyPlan],
        ops: &[CommitIndexOp],
    ) -> Result<
        (
            Vec<&'static LocalKey<RefCell<IndexStore>>>,
            Vec<PreparedIndexRollback>,
        ),
        InternalError,
    > {
        // Phase 1: map index store paths to store handles.
        let mut stores = BTreeMap::new();
        for plan in plans {
            stores.insert(plan.index.store, plan.store);
        }

        let mut apply_stores = Vec::with_capacity(ops.len());
        let mut rollbacks = Vec::with_capacity(ops.len());

        // Phase 2: validate marker ops and snapshot current entries for rollback.
        for op in ops {
            let (store, raw_key) = resolve_index_key(&stores, op, E::PATH, || {
                if op.value.is_none() {
                    Some(InternalError::new(
                        ErrorClass::Internal,
                        ErrorOrigin::Index,
                        format!(
                            "commit marker index op missing entry before save: {} ({})",
                            op.store,
                            E::PATH
                        ),
                    ))
                } else {
                    None
                }
            })?;
            let existing = store.with_borrow(|s| s.get(&raw_key));

            apply_stores.push(store);
            rollbacks.push(PreparedIndexRollback {
                store,
                key: raw_key,
                value: existing,
            });
        }

        Ok((apply_stores, rollbacks))
    }

    /// Validate commit data ops and prepare rollback rows for the save.
    fn prepare_data_save_ops(
        ops: &[CommitDataOp],
        old_row: Option<RawRow>,
    ) -> Result<Vec<PreparedDataRollback>, InternalError> {
        if ops.len() != 1 {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker save expects 1 data op, found {} ({})",
                    ops.len(),
                    E::PATH
                ),
            ));
        }

        let op = &ops[0];
        if op.store != E::DataStore::PATH {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker references unexpected data store '{}' ({})",
                    op.store,
                    E::PATH
                ),
            ));
        }
        if op.key.len() != DataKey::STORED_SIZE_USIZE {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data key length {} does not match {} ({})",
                    op.key.len(),
                    DataKey::STORED_SIZE_USIZE,
                    E::PATH
                ),
            ));
        }
        let Some(value) = &op.value else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!("commit marker save missing data payload ({})", E::PATH),
            ));
        };
        if value.len() > crate::db::store::MAX_ROW_BYTES as usize {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data payload exceeds max size: {} bytes ({})",
                    value.len(),
                    E::PATH
                ),
            ));
        }

        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
        Ok(vec![PreparedDataRollback {
            key: raw_key,
            value: old_row,
        }])
    }

    /// Apply rollback mutations for index entries using raw bytes.
    fn apply_index_rollbacks(ops: Vec<PreparedIndexRollback>) {
        for op in ops {
            op.store.with_borrow_mut(|s| {
                if let Some(value) = op.value {
                    s.insert(op.key, value);
                } else {
                    s.remove(&op.key);
                }
            });
        }
    }

    /// Apply commit marker data ops to the data store.
    fn apply_marker_data_ops(ops: &[CommitDataOp], store: &'static LocalKey<RefCell<DataStore>>) {
        // SAFETY / INVARIANT:
        // All structural and semantic invariants for these marker ops are fully
        // validated during planning *before* the commit marker is persisted.
        // After marker creation, apply is required to be infallible or trap.
        for op in ops {
            assert!(
                op.value.is_some(),
                "commit marker save missing data payload ({})",
                E::PATH
            );
            let value = op.value.as_ref().unwrap();
            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let raw_value = RawRow::from_bytes(Cow::Borrowed(value.as_slice()));
            store.with_borrow_mut(|s| s.insert(raw_key, raw_value));
        }
    }

    /// Apply rollback mutations for saved rows.
    fn apply_data_rollbacks(
        store: &'static LocalKey<RefCell<DataStore>>,
        ops: Vec<PreparedDataRollback>,
    ) {
        for op in ops {
            store.with_borrow_mut(|s| {
                if let Some(value) = op.value {
                    s.insert(op.key, value);
                } else {
                    s.remove(&op.key);
                }
            });
        }
    }
}

// Persisted error metadata for schema validation results.
struct CachedInvariant {
    class: ErrorClass,
    origin: ErrorOrigin,
    message: String,
}

impl CachedInvariant {
    fn from_error(err: InternalError) -> Self {
        Self {
            class: err.class,
            origin: err.origin,
            message: err.message,
        }
    }

    fn to_error(&self) -> InternalError {
        InternalError::new(self.class, self.origin, self.message.clone())
    }
}

// Build the set of fields referenced by indexes for an entity.
fn indexed_field_set<E: EntityKind>() -> BTreeSet<&'static str> {
    let mut fields = BTreeSet::new();
    for index in E::INDEXES {
        fields.extend(index.fields.iter().copied());
    }

    fields
}

const fn save_mode_tag(mode: SaveMode) -> &'static str {
    match mode {
        SaveMode::Insert => "insert",
        SaveMode::Update => "update",
        SaveMode::Replace => "replace",
    }
}

/// Rollback descriptor for index mutations recorded in a commit marker.
struct PreparedIndexRollback {
    store: &'static LocalKey<RefCell<IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

/// Rollback descriptor for data mutations recorded in a commit marker.
struct PreparedDataRollback {
    key: RawDataKey,
    value: Option<RawRow>,
}

/*
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::SaveExecutor;
    use crate::{
        error::{ErrorClass, ErrorOrigin},
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
            index::IndexModel,
        },
        traits::{
            CanisterKind, DataStoreKind, EntityKind, FieldValue, FieldValues, Path, SanitizeAuto,
            SanitizeCustom, ValidateAuto, ValidateCustom, View, Visitable,
        },
        types::{Ref, Ulid},
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    const CANISTER_PATH: &str = "save_invariant_test::TestCanister";
    const STORE_PATH: &str = "save_invariant_test::TestStore";

    const KEY_ENTITY_PATH: &str = "save_invariant_test::BadKeyEntity";
    const FIELD_ENTITY_PATH: &str = "save_invariant_test::BadFieldEntity";
    const TYPE_ENTITY_PATH: &str = "save_invariant_test::BadTypeEntity";

    const TEST_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    }];
    const INDEXES: [&IndexModel; 0] = [];

    const KEY_MODEL: EntityModel = EntityModel {
        path: KEY_ENTITY_PATH,
        entity_name: "BadKeyEntity",
        primary_key: &TEST_FIELDS[0],
        fields: &TEST_FIELDS,
        indexes: &INDEXES,
    };
    const FIELD_MODEL: EntityModel = EntityModel {
        path: FIELD_ENTITY_PATH,
        entity_name: "BadFieldEntity",
        primary_key: &TEST_FIELDS[0],
        fields: &TEST_FIELDS,
        indexes: &INDEXES,
    };
    const TYPE_MODEL: EntityModel = EntityModel {
        path: TYPE_ENTITY_PATH,
        entity_name: "BadTypeEntity",
        primary_key: &TEST_FIELDS[0],
        fields: &TEST_FIELDS,
        indexes: &INDEXES,
    };

    /// Deliberately violates `key() == primary_key()` for invariant testing.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadKeyEntity {
        id: Ref<Self>,
        other: Ref<Self>,
    }

    /// Deliberately returns an inconsistent primary key field value.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadFieldEntity {
        id: Ref<Self>,
        other: Ref<Self>,
    }

    /// Deliberately returns an invalid value type for the primary key field.
    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct BadTypeEntity {
        id: Ref<Self>,
    }

    impl Path for BadKeyEntity {
        const PATH: &'static str = KEY_ENTITY_PATH;
    }

    impl Path for BadFieldEntity {
        const PATH: &'static str = FIELD_ENTITY_PATH;
    }

    impl Path for BadTypeEntity {
        const PATH: &'static str = TYPE_ENTITY_PATH;
    }

    impl View for BadKeyEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl View for BadFieldEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl View for BadTypeEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for BadKeyEntity {}
    impl SanitizeCustom for BadKeyEntity {}
    impl ValidateAuto for BadKeyEntity {}
    impl ValidateCustom for BadKeyEntity {}
    impl Visitable for BadKeyEntity {}

    impl SanitizeAuto for BadFieldEntity {}
    impl SanitizeCustom for BadFieldEntity {}
    impl ValidateAuto for BadFieldEntity {}
    impl ValidateCustom for BadFieldEntity {}
    impl Visitable for BadFieldEntity {}

    impl SanitizeAuto for BadTypeEntity {}
    impl SanitizeCustom for BadTypeEntity {}
    impl ValidateAuto for BadTypeEntity {}
    impl ValidateCustom for BadTypeEntity {}
    impl Visitable for BadTypeEntity {}

    impl FieldValues for BadKeyEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(self.id.to_value()),
                _ => None,
            }
        }
    }

    impl FieldValues for BadFieldEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(self.other.to_value()),
                _ => None,
            }
        }
    }

    impl FieldValues for BadTypeEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Text(self.id.to_string())),
                _ => None,
            }
        }
    }

    #[derive(Clone, Copy)]
    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = CANISTER_PATH;
    }

    impl CanisterKind for TestCanister {}

    struct TestStore;

    impl Path for TestStore {
        const PATH: &'static str = STORE_PATH;
    }

    impl DataStoreKind for TestStore {
        type Canister = TestCanister;
    }

    impl EntityKind for BadKeyEntity {
        type PrimaryKey = Ref<Self>;
        type DataStore = TestStore;
        type Canister = TestCanister;

        const ENTITY_NAME: &'static str = "BadKeyEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id"];
        const INDEXES: &'static [&'static IndexModel] = &INDEXES;
        const MODEL: &'static EntityModel = &KEY_MODEL;

        fn key(&self) -> Self::PrimaryKey {
            self.other
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    impl EntityKind for BadFieldEntity {
        type PrimaryKey = Ref<Self>;
        type DataStore = TestStore;
        type Canister = TestCanister;

        const ENTITY_NAME: &'static str = "BadFieldEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id"];
        const INDEXES: &'static [&'static IndexModel] = &INDEXES;
        const MODEL: &'static EntityModel = &FIELD_MODEL;

        fn key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    impl EntityKind for BadTypeEntity {
        type PrimaryKey = Ref<Self>;
        type DataStore = TestStore;
        type Canister = TestCanister;

        const ENTITY_NAME: &'static str = "BadTypeEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id"];
        const INDEXES: &'static [&'static IndexModel] = &INDEXES;
        const MODEL: &'static EntityModel = &TYPE_MODEL;

        fn key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    #[test]
    fn validate_entity_invariants_rejects_key_mismatch() {
        let entity = BadKeyEntity {
            id: Ref::new(Ulid::from_u128(1)),
            other: Ref::new(Ulid::from_u128(2)),
        };
        let schema = SaveExecutor::<BadKeyEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadKeyEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected key mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }

    #[test]
    fn validate_entity_invariants_rejects_field_mismatch() {
        let entity = BadFieldEntity {
            id: Ref::new(Ulid::from_u128(1)),
            other: Ref::new(Ulid::from_u128(2)),
        };
        let schema = SaveExecutor::<BadFieldEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadFieldEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected field mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }

    #[test]
    fn validate_entity_invariants_rejects_type_mismatch() {
        let entity = BadTypeEntity {
            id: Ref::new(Ulid::from_u128(1)),
        };
        let schema = SaveExecutor::<BadTypeEntity>::schema_info().expect("schema");
        let err = SaveExecutor::<BadTypeEntity>::validate_entity_invariants(&entity, schema)
            .expect_err("expected type mismatch to fail");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Executor);
    }
}
*/
