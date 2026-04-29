//! Module: executor::mutation::save_validation
//! Responsibility: save preflight invariant enforcement for entity values.
//! Does not own: commit-window apply mechanics or relation metadata ownership.
//! Boundary: validation-only helpers invoked before save commit planning.

use crate::{
    db::{
        PersistedRow,
        data::{
            CanonicalSlotReader, DataKey, RawRow, SlotReader, StructuralPatch, StructuralSlotReader,
        },
        executor::{EntityAuthority, mutation::save::SaveExecutor},
        predicate::canonical_cmp,
        relation::validate_save_strong_relations,
        schema::{SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    model::field::FieldKind,
    sanitize::{SanitizeWriteContext, sanitize_with_context},
    traits::{EntityKind, EntityValue},
    validate::validate,
    value::Value,
};
use std::cmp::Ordering;

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Enforce write-boundary scalar bounds before structural patch values are
    // serialized into persisted-row payloads. This keeps user-authored
    // structural writes on the same executor error taxonomy as typed writes
    // instead of letting storage encoders report user mistakes as internal
    // row-encode failures.
    pub(in crate::db::executor::mutation) fn validate_structural_patch_write_bounds(
        patch: &StructuralPatch,
    ) -> Result<(), InternalError> {
        let fields = EntityAuthority::for_type::<E>().fields();

        for entry in patch.entries() {
            let Some(field) = fields.get(entry.slot().index()) else {
                return Err(InternalError::persisted_row_slot_lookup_out_of_bounds(
                    E::MODEL.path(),
                    entry.slot().index(),
                ));
            };

            Self::validate_decimal_scale_is_normalizable(field.name, &field.kind, entry.value())?;
            Self::validate_text_max_len(field.name, &field.kind, entry.value())?;
        }

        Ok(())
    }

    // Validate one persisted row against the current write-boundary invariants
    // without rebuilding a typed entity first.
    pub(in crate::db::executor::mutation) fn ensure_persisted_row_invariants(
        data_key: &DataKey,
        row: &RawRow,
    ) -> Result<(), InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let schema = authority.schema_info();
        let row_fields = authority.row_layout().open_raw_row(row)?;
        row_fields.validate_storage_key(data_key)?;

        Self::validate_structural_row_invariants(&row_fields, schema)
    }

    // Load the trusted generated schema view for one entity type.
    pub(in crate::db::executor::mutation) fn schema_info() -> &'static SchemaInfo {
        EntityAuthority::for_type::<E>().schema_info()
    }

    // Execute save preflight using already-resolved schema and relation metadata.
    //
    // Batch save lanes call this helper so they do not repay the schema-cache
    // mutex lookup and strong-relation capability probe for every row.
    pub(in crate::db::executor::mutation) fn preflight_entity_with_cached_schema(
        &self,
        entity: &mut E,
        schema: &SchemaInfo,
        validate_relations: bool,
        write_context: SanitizeWriteContext,
        authored_create_slots: Option<&[usize]>,
    ) -> Result<(), InternalError> {
        Self::validate_create_authorship(authored_create_slots)?;
        sanitize_with_context(entity, Some(write_context))?;
        validate(entity)?;
        Self::validate_entity_invariants(entity, schema)?;
        if validate_relations {
            validate_save_strong_relations::<E>(&self.db, entity)?;
        }

        Ok(())
    }

    // Enforce the typed create authorship contract for generated create-input
    // payloads. Every user-authorable create field must be explicitly present;
    // only generated or managed fields may be omitted by the create type.
    fn validate_create_authorship(
        authored_create_slots: Option<&[usize]>,
    ) -> Result<(), InternalError> {
        let Some(authored_create_slots) = authored_create_slots else {
            return Ok(());
        };

        let missing_fields = EntityAuthority::for_type::<E>()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.insert_generation().is_none())
            .filter(|(_, field)| field.write_management().is_none())
            .filter(|(index, _)| !authored_create_slots.contains(index))
            .map(|(_, field)| field.name().to_string())
            .collect::<Vec<_>>();

        if missing_fields.is_empty() {
            return Ok(());
        }

        Err(InternalError::mutation_create_missing_authored_fields(
            E::PATH,
            &missing_fields.join(", "),
        ))
    }

    // Enforce trait boundary invariants for user-provided entities.
    fn validate_entity_invariants(entity: &E, schema: &SchemaInfo) -> Result<(), InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let primary_key_name = authority.primary_key_name();

        // Phase 1: validate primary key field presence and *shape*.
        let pk_field_index = authority.row_layout().primary_key_slot();
        let pk_value = entity.get_value_by_index(pk_field_index).ok_or_else(|| {
            InternalError::mutation_entity_primary_key_missing(E::PATH, primary_key_name)
        })?;

        // Primary key must not be Null.
        // Unit is valid for singleton entities and is enforced by schema shape checks below.
        if matches!(pk_value, Value::Null) {
            return Err(InternalError::mutation_entity_primary_key_invalid_value(
                E::PATH,
                primary_key_name,
                &pk_value,
            ));
        }

        // If schema knows the PK type, enforce literal shape compatibility.
        if let Some(pk_type) = schema.field(primary_key_name)
            && !literal_matches_type(&pk_value, pk_type)
        {
            return Err(InternalError::mutation_entity_primary_key_type_mismatch(
                E::PATH,
                primary_key_name,
                &pk_value,
            ));
        }

        // The declared PK field value must exactly match the runtime identity key.
        let identity_pk = crate::traits::KeyValueCodec::to_key_value(&entity.id().key());
        if pk_value != identity_pk {
            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                primary_key_name,
                &pk_value,
                &identity_pk,
            ));
        }

        // Phase 2: validate field presence and runtime value shapes.
        for (field_index, field) in authority.fields().iter().enumerate() {
            let value = entity.get_value_by_index(field_index).ok_or_else(|| {
                InternalError::mutation_entity_field_missing(
                    E::PATH,
                    field.name,
                    field_is_indexed::<E>(field.name),
                )
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

            if !literal_matches_type(&value, field_type) && !field.kind.accepts_value(&value) {
                return Err(InternalError::mutation_entity_field_type_mismatch(
                    E::PATH,
                    field.name,
                    &value,
                ));
            }

            // Phase 3: enforce schema-declared scalar bounds at write boundaries.
            Self::validate_decimal_scale_is_normalizable(field.name, &field.kind, &value)?;
            Self::validate_text_max_len(field.name, &field.kind, &value)?;

            // Phase 4: enforce deterministic collection/map encodings at runtime.
            Self::validate_deterministic_field_value(field.name, &field.kind, &value)?;
        }

        Ok(())
    }

    // Enforce the persisted-row write invariants directly on the structural row
    // reader after row-shape and primary-key validation have already succeeded.
    fn validate_structural_row_invariants(
        row_fields: &StructuralSlotReader<'_>,
        schema: &SchemaInfo,
    ) -> Result<(), InternalError> {
        let authority = EntityAuthority::for_type::<E>();

        for (field_index, field) in authority.fields().iter().enumerate() {
            if !row_fields.has(field_index) {
                return Err(InternalError::mutation_entity_field_missing(
                    E::PATH,
                    field.name,
                    field_is_indexed::<E>(field.name),
                ));
            }

            let value = row_fields.required_value_by_contract_cow(field_index)?;

            if matches!(value.as_ref(), Value::Null | Value::Unit) {
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

            if !literal_matches_type(value.as_ref(), field_type)
                && !field.kind.accepts_value(value.as_ref())
            {
                return Err(InternalError::mutation_entity_field_type_mismatch(
                    E::PATH,
                    field.name,
                    value.as_ref(),
                ));
            }

            // Phase 1: enforce schema-declared scalar bounds at write boundaries.
            Self::validate_decimal_scale_exact(field.name, &field.kind, value.as_ref())?;
            Self::validate_text_max_len(field.name, &field.kind, value.as_ref())?;

            // Phase 2: enforce deterministic collection/map encodings at runtime.
            Self::validate_deterministic_field_value(field.name, &field.kind, value.as_ref())?;
        }

        Ok(())
    }

    /// Accept authored decimal values that can be normalized to fixed field scale.
    fn validate_decimal_scale_is_normalizable(
        field_name: &'static str,
        kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
                let probe = crate::model::field::FieldModel::generated(field_name, *kind);
                probe
                    .normalize_runtime_value_for_storage(value)
                    .map(|_| ())
                    .map_err(|_| {
                        InternalError::mutation_decimal_scale_mismatch(
                            E::PATH,
                            field_name,
                            scale,
                            decimal.scale(),
                        )
                    })
            }
            (FieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_decimal_scale_is_normalizable(field_name, key_kind, value)
            }
            (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
                for item in items {
                    Self::validate_decimal_scale_is_normalizable(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                FieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_decimal_scale_is_normalizable(field_name, key, entry_key)?;
                    Self::validate_decimal_scale_is_normalizable(
                        field_name,
                        map_value,
                        entry_value,
                    )?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Enforce fixed decimal scales across already persisted structural values.
    fn validate_decimal_scale_exact(
        field_name: &'static str,
        kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
                if decimal.scale() != *scale {
                    return Err(InternalError::mutation_decimal_scale_mismatch(
                        E::PATH,
                        field_name,
                        scale,
                        decimal.scale(),
                    ));
                }

                Ok(())
            }
            (FieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_decimal_scale_exact(field_name, key_kind, value)
            }
            (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
                for item in items {
                    Self::validate_decimal_scale_exact(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                FieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_decimal_scale_exact(field_name, key, entry_key)?;
                    Self::validate_decimal_scale_exact(field_name, map_value, entry_value)?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Enforce bounded text lengths across scalar and nested collection values.
    fn validate_text_max_len(
        field_name: &str,
        kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (FieldKind::Text { max_len: Some(max) }, Value::Text(text)) => {
                let actual_len = text.chars().count();
                if actual_len > *max as usize {
                    return Err(InternalError::mutation_text_max_len_exceeded(
                        E::PATH,
                        field_name,
                        max,
                        actual_len,
                    ));
                }

                Ok(())
            }
            (FieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_text_max_len(field_name, key_kind, value)
            }
            (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
                for item in items {
                    Self::validate_text_max_len(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                FieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_text_max_len(field_name, key, entry_key)?;
                    Self::validate_text_max_len(field_name, map_value, entry_value)?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Enforce deterministic value encodings for collection-like field kinds.
    pub(in crate::db::executor) fn validate_deterministic_field_value(
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
            return Err(InternalError::mutation_set_field_list_required(
                E::PATH,
                field_name,
            ));
        };

        for pair in items.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            let ordering = canonical_cmp(left, right);
            if ordering != Ordering::Less {
                return Err(InternalError::mutation_set_field_not_canonical(
                    E::PATH,
                    field_name,
                ));
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
            return Err(InternalError::mutation_map_field_map_required(
                E::PATH,
                field_name,
            ));
        };

        Value::validate_map_entries(entries.as_slice()).map_err(|err| {
            InternalError::mutation_map_field_entries_invalid(E::PATH, field_name, err)
        })?;

        // Save preflight only needs to prove the incoming map is already
        // canonical. Re-normalizing through an owned clone would drag the full
        // sort path into every save-capable entity even though write-boundary
        // validation never consumes the reordered output.
        if !Value::map_entries_are_strictly_canonical(entries.as_slice()) {
            return Err(InternalError::mutation_map_field_entries_not_canonical(
                E::PATH,
                field_name,
            ));
        }

        Ok(())
    }
}

// Check whether the missing field participates in any declared index.
fn field_is_indexed<E: EntityKind>(field_name: &str) -> bool {
    E::MODEL
        .indexes()
        .iter()
        .any(|index| index.fields().contains(&field_name))
}
