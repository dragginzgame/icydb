use crate::{
    db::{
        executor::save::SaveExecutor,
        query::predicate::{
            coercion::canonical_cmp,
            validate::{SchemaInfo, literal_matches_type},
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::FieldKind,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, OnceLock},
};

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    // Cache schema validation per entity type to keep invariant checks fast.
    // Note: these trait boundaries may be sealed in a future major version.
    pub(super) fn ensure_entity_invariants(entity: &E) -> Result<(), InternalError> {
        let schema = Self::schema_info()?;

        Self::validate_entity_invariants(entity, schema)
    }

    // Cache schema validation results per entity type.
    fn schema_info() -> Result<&'static SchemaInfo, InternalError> {
        type SchemaCache = BTreeMap<&'static str, Result<&'static SchemaInfo, CachedInvariant>>;
        static CACHE: OnceLock<Mutex<SchemaCache>> = OnceLock::new();

        let cache = CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut cache_guard = cache
            .lock()
            .expect("schema cache lock should not be poisoned");

        let entry = cache_guard.entry(E::PATH).or_insert_with(|| {
            SchemaInfo::from_entity_model(E::MODEL)
                .map(|schema| Box::leak(Box::new(schema)) as &'static SchemaInfo)
                .map_err(|err| {
                    CachedInvariant::from_error(InternalError::executor_invariant(format!(
                        "entity schema invalid for {}: {err}",
                        E::PATH
                    )))
                })
        });

        match entry {
            Ok(schema) => Ok(*schema),
            Err(err) => Err(err.to_error()),
        }
    }

    // Enforce trait boundary invariants for user-provided entities.
    fn validate_entity_invariants(entity: &E, schema: &SchemaInfo) -> Result<(), InternalError> {
        // Phase 1: validate primary key field presence and *shape*.
        let pk_value = entity.get_value(E::PRIMARY_KEY).ok_or_else(|| {
            InternalError::executor_invariant(format!(
                "entity primary key field missing: {} field={}",
                E::PATH,
                E::PRIMARY_KEY
            ))
        })?;

        // Primary key must not be Null.
        // Unit is valid for singleton entities and is enforced by schema shape checks below.
        if matches!(pk_value, Value::Null) {
            return Err(InternalError::executor_invariant(format!(
                "entity primary key field has invalid value: {} field={} value={pk_value:?}",
                E::PATH,
                E::PRIMARY_KEY
            )));
        }

        // If schema knows the PK type, enforce literal shape compatibility.
        if let Some(pk_type) = schema.field(E::PRIMARY_KEY)
            && !literal_matches_type(&pk_value, pk_type)
        {
            return Err(InternalError::executor_invariant(format!(
                "entity primary key field type mismatch: {} field={} value={pk_value:?}",
                E::PATH,
                E::PRIMARY_KEY
            )));
        }

        // The declared PK field value must exactly match the runtime identity key.
        let identity_pk = crate::traits::FieldValue::to_value(&entity.id().key());
        if pk_value != identity_pk {
            return Err(InternalError::executor_invariant(format!(
                "entity primary key mismatch: {} field={} field_value={pk_value:?} id_key={identity_pk:?}",
                E::PATH,
                E::PRIMARY_KEY
            )));
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
                InternalError::executor_invariant(format!(
                    "entity field missing: {} field={}{}",
                    E::PATH,
                    field.name,
                    note
                ))
            })?;

            if matches!(value, Value::Null | Value::Unit) {
                // Null = absent, Unit = singleton sentinel; both skip type checks.
                continue;
            }

            if !field.kind.value_kind().is_queryable() {
                // Non-queryable structured fields are not planner-addressable.
                continue;
            }

            let Some(field_type) = schema.field(field.name) else {
                // Runtime-only field; treat as non-queryable.
                continue;
            };

            if !literal_matches_type(&value, field_type) {
                return Err(InternalError::executor_invariant(format!(
                    "entity field type mismatch: {} field={} value={value:?}",
                    E::PATH,
                    field.name
                )));
            }

            // Phase 3: enforce deterministic collection/map encodings at runtime.
            Self::validate_deterministic_field_value(field.name, &field.kind, &value)?;
        }

        Ok(())
    }

    /// Enforce deterministic value encodings for collection-like field kinds.
    pub(super) fn validate_deterministic_field_value(
        field_name: &str,
        kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        match kind {
            FieldKind::Set(_) => Self::validate_set_encoding(field_name, value),
            FieldKind::Map { .. } => Self::validate_map_encoding(field_name, value),
            _ => Ok(()),
        }
    }

    /// Validate canonical ordering + uniqueness for set-encoded list values.
    fn validate_set_encoding(field_name: &str, value: &Value) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        let Value::List(items) = value else {
            return Err(InternalError::executor_invariant(format!(
                "set field must encode as Value::List: {} field={field_name}",
                E::PATH
            )));
        };

        for pair in items.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            let ordering = canonical_cmp(left, right);
            if ordering != Ordering::Less {
                return Err(InternalError::executor_invariant(format!(
                    "set field must be strictly ordered and deduplicated: {} field={field_name}",
                    E::PATH
                )));
            }
        }

        Ok(())
    }

    /// Validate canonical map entry invariants for persisted map values.
    ///
    /// Map fields are persisted as atomic row-level value replacements; this
    /// check guarantees each stored map payload is already canonical.
    fn validate_map_encoding(field_name: &str, value: &Value) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        let Value::Map(entries) = value else {
            return Err(InternalError::executor_invariant(format!(
                "map field must encode as Value::Map: {} field={field_name}",
                E::PATH
            )));
        };

        Value::validate_map_entries(entries.as_slice()).map_err(|err| {
            InternalError::executor_invariant(format!(
                "map field entries violate map invariants: {} field={field_name} ({err})",
                E::PATH
            ))
        })?;

        let normalized = Value::normalize_map_entries(entries.clone()).map_err(|err| {
            InternalError::executor_invariant(format!(
                "map field entries cannot be normalized: {} field={field_name} ({err})",
                E::PATH
            ))
        })?;
        if normalized.as_slice() != entries.as_slice() {
            return Err(InternalError::executor_invariant(format!(
                "map field entries are not in canonical deterministic order: {} field={field_name}",
                E::PATH
            )));
        }

        Ok(())
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
