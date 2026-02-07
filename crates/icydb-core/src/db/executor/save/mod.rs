#[cfg(test)]
mod tests;

use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit,
        ensure_recovered_for_write,
        executor::{
            ExecutorError,
            commit_ops::{apply_marker_index_ops, resolve_index_key},
            trace::{QueryTraceSink, TraceExecutorKind, start_exec_trace},
        },
        finish_commit,
        identity::EntityName,
        index::{
            IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
            plan::{IndexApplyPlan, plan_index_mutation_for_entity},
        },
        query::{
            SaveMode,
            predicate::{
                coercion::canonical_cmp,
                validate::{SchemaInfo, literal_matches_type},
            },
        },
        store::{DataKey, DataStore, RawDataKey, RawRow, StorageKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::{EntityFieldKind, RelationStrength},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::serialize,
    traits::{EntityKind, EntityValue, FieldValue, Path, Storable},
    validate::validate,
    value::Value,
};
use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::Ordering,
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
pub struct SaveExecutor<E: EntityKind + EntityValue> {
    db: Db<E::Canister>,
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

///
/// StrongRelationInfo
///
/// Lightweight descriptor for strong relation validation.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct StrongRelationInfo {
    target_path: &'static str,
    target_entity_name: &'static str,
    target_store_path: &'static str,
}

// Resolve a field-kind into strong relation metadata (if applicable).
const fn strong_relation_from_kind(kind: &EntityFieldKind) -> Option<StrongRelationInfo> {
    match kind {
        EntityFieldKind::Ref {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }
        | EntityFieldKind::List(EntityFieldKind::Ref {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        })
        | EntityFieldKind::Set(EntityFieldKind::Ref {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationInfo {
            target_path,
            target_entity_name,
            target_store_path,
        }),
        _ => {
            // NOTE: Only strong Ref and collection (List/Set) Ref fields participate in save-time RI.
            None
        }
    }
}

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
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
    #[expect(dead_code)]
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
        self.save_view(SaveMode::Insert, view)
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Update, view)
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Replace, view)
    }

    // Shared wrapper for view-based save operations.
    fn save_view(&self, mode: SaveMode, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.save_entity(mode, entity)?.as_view())
    }

    // ======================================================================
    // Batch save operations (fail-fast, non-atomic)
    // ======================================================================

    /// Save a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: this helper is fail-fast and non-atomic. If one element fails,
    /// earlier elements in the batch remain committed.
    pub fn save_batch_non_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        let mut batch_index = 0usize;

        for entity in iter {
            batch_index = batch_index.saturating_add(1);
            match self.save_entity(mode, entity) {
                Ok(saved) => out.push(saved),
                Err(err) => {
                    if !out.is_empty() {
                        // Batch writes are intentionally non-atomic; surface partial commits loudly.
                        println!(
                            "[warn] icydb non-atomic batch partial commit: mode={mode:?} entity={} committed={} failed_at_item={} error={err}",
                            E::PATH,
                            out.len(),
                            batch_index,
                        );
                    }

                    return Err(err);
                }
            }
        }

        Ok(out)
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Insert, entities)
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Update, entities)
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Replace, entities)
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
            ensure_recovered_for_write(&self.db)?;

            // Sanitize & validate before key extraction in case PK fields are normalized
            sanitize(&mut entity)?;
            validate(&entity)?;
            Self::ensure_entity_invariants(&entity)?;

            // Enforce explicit strong relations before commit planning.
            self.validate_strong_relations(&entity)?;

            let key = entity.id().into_storage_key();
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

                        let expected = data_key.try_key::<E>()?;
                        let actual = stored.id().into_storage_key();
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
                    let expected = data_key.try_key::<E>()?;
                    let actual = old.id().into_storage_key();
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
                        let expected = data_key.try_key::<E>()?;
                        let actual = old.id().into_storage_key();
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

            if matches!(value, Value::Null) {
                continue;
            }

            if matches!(value, Value::Unit) {
                // Unit is an executor-only sentinel for singleton presence; skip type checks.
                continue;
            }

            if !field.kind.value_kind().is_queryable() {
                // Non-queryable structured fields are not planner-addressable.
                // Skip predicate/index shape checks for this affordance class.
                continue;
            }

            let Some(field_type) = schema.field(field.name) else {
                // Field is not part of schema (runtime-only); treat as non-queryable.
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

            // Phase 3: enforce deterministic collection/map encodings at runtime.
            Self::validate_deterministic_field_value(field.name, &field.kind, &value)?;
        }

        Ok(())
    }

    /// Enforce deterministic value encodings for collection-like field kinds.
    fn validate_deterministic_field_value(
        field_name: &str,
        kind: &EntityFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        match kind {
            EntityFieldKind::Set(_) => Self::validate_set_encoding(field_name, value),
            EntityFieldKind::Map { .. } => Self::validate_map_encoding(field_name, value),
            _ => Ok(()),
        }
    }

    /// Validate canonical ordering + uniqueness for set-encoded list values.
    fn validate_set_encoding(field_name: &str, value: &Value) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        let Value::List(items) = value else {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "set field must encode as Value::List: {} field={field_name}",
                    E::PATH
                ),
            ));
        };

        for pair in items.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            let ordering = canonical_cmp(left, right);
            if ordering != Ordering::Less {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "set field must be strictly ordered and deduplicated: {} field={field_name}",
                        E::PATH
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Validate canonical map entry invariants for persisted map values.
    fn validate_map_encoding(field_name: &str, value: &Value) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        let Value::Map(entries) = value else {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "map field must encode as Value::Map: {} field={field_name}",
                    E::PATH
                ),
            ));
        };

        Value::validate_map_entries(entries.as_slice()).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "map field entries violate map invariants: {} field={field_name} ({err})",
                    E::PATH
                ),
            )
        })?;

        let normalized = Value::normalize_map_entries(entries.clone()).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "map field entries cannot be normalized: {} field={field_name} ({err})",
                    E::PATH
                ),
            )
        })?;
        if normalized.as_slice() != entries.as_slice() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "map field entries are not in canonical deterministic order: {} field={field_name}",
                    E::PATH
                ),
            ));
        }

        Ok(())
    }

    /// Validate strong relation references against the target data stores.
    fn validate_strong_relations(&self, entity: &E) -> Result<(), InternalError> {
        // Phase 1: identify strong relation fields and read their values.
        for field in E::MODEL.fields {
            let Some(relation) = strong_relation_from_kind(&field.kind) else {
                continue;
            };

            let value = entity.get_value(field.name).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("entity field missing: {} field={}", E::PATH, field.name),
                )
            })?;

            // Phase 2: validate each referenced key.
            match &value {
                Value::List(items) => {
                    // Collection enforcement is aggregate: every referenced key must exist.
                    // NOTE: relation List/Set shapes are represented as Value::List at runtime.
                    for item in items {
                        // NOTE: Optional list entries are allowed; skip explicit None values.
                        if matches!(item, Value::Null) {
                            continue;
                        }
                        self.validate_strong_relation_value(field.name, relation, item)?;
                    }
                }
                Value::Null => {
                    // NOTE: Optional strong relations may be unset; None does not trigger RI.
                }
                _ => {
                    self.validate_strong_relation_value(field.name, relation, &value)?;
                }
            }
        }

        Ok(())
    }

    /// Validate a single strong relation key against the target store.
    fn validate_strong_relation_value(
        &self,
        field_name: &str,
        relation: StrongRelationInfo,
        value: &Value,
    ) -> Result<(), InternalError> {
        // Phase 1: normalize the key into a storage-compatible form.
        let storage_key = StorageKey::try_from_value(value).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "strong relation key not storage-compatible: source={} field={} target={} value={value:?} ({err})",
                    E::PATH,
                    field_name,
                    relation.target_path
                ),
            )
        })?;
        let entity_name = EntityName::try_from_str(relation.target_entity_name).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "strong relation target name invalid: source={} field={} target={} name={} ({err})",
                    E::PATH,
                    field_name,
                    relation.target_path,
                    relation.target_entity_name
                ),
            )
        })?;

        let entity_bytes = entity_name.to_bytes();
        let key_bytes = storage_key.to_bytes()?;
        let mut raw_bytes = [0u8; DataKey::STORED_SIZE_USIZE];
        raw_bytes[..EntityName::STORED_SIZE_USIZE].copy_from_slice(&entity_bytes);
        raw_bytes[EntityName::STORED_SIZE_USIZE..].copy_from_slice(&key_bytes);
        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(raw_bytes.as_slice()));

        // Phase 2: resolve the target store and confirm existence.
        let store = self
            .db
            .with_data(|reg| reg.try_get_store(relation.target_store_path))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "strong relation target store missing: source={} field={} target={} store={} key={value:?} ({err})",
                        E::PATH,
                        field_name,
                        relation.target_path,
                        relation.target_store_path
                    ),
                )
            })?;
        let exists = store.with_borrow(|s| s.contains_key(&raw_key));
        if !exists {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "strong relation missing: source={} field={} target={} key={value:?}",
                    E::PATH,
                    field_name,
                    relation.target_path
                ),
            ));
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

///
/// CachedInvariant
/// Persisted error metadata for schema validation results
///

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
