//! Module: executor::mutation::save_validation
//! Responsibility: save preflight invariant enforcement for entity values.
//! Does not own: commit-window apply mechanics or relation metadata ownership.
//! Boundary: validation-only helpers invoked before save commit planning.

use crate::{
    db::{
        PersistedRow,
        data::{
            CanonicalSlotReader, DataKey, RawRow, StructuralPatch, StructuralRowContract,
            StructuralSlotReader,
        },
        executor::{EntityAuthority, mutation::save::SaveExecutor},
        predicate::canonical_cmp,
        relation::validate_save_strong_relations,
        schema::{AcceptedRowDecodeContract, SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    model::field::{FieldKind, normalize_decimal_scale_for_storage},
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
        let row_layout = EntityAuthority::for_type::<E>().row_layout();

        for entry in patch.entries() {
            let field = row_layout
                .contract()
                .field_decode_contract(entry.slot().index())?;
            let field_name = field.name();
            let field_kind = field.kind();

            Self::validate_decimal_scale_is_normalizable(field_name, &field_kind, entry.value())?;
            Self::validate_text_max_len(field_name, &field_kind, entry.value())?;
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
        let row_layout = authority.row_layout();
        let row_fields = row_layout.open_raw_row_with_contract(row)?;
        row_fields.validate_storage_key(data_key)?;

        Self::validate_structural_row_invariants(&row_fields, schema)
    }

    // Validate one persisted row through an accepted row-decode contract before
    // structural updates use it as a baseline. This keeps old short rows inside
    // the accepted-schema path while preserving the same storage-key and field
    // invariant checks as generated-only rows.
    pub(in crate::db::executor::mutation) fn ensure_persisted_row_invariants_with_accepted_contract(
        data_key: &DataKey,
        row: &RawRow,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Result<(), InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let schema = authority.schema_info();
        let contract = StructuralRowContract::from_model_with_accepted_decode_contract(
            authority.model(),
            accepted_row_decode_contract,
        );
        let row_fields = StructuralSlotReader::from_raw_row_with_validated_contract(row, contract)?;
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
        let row_layout = authority.row_layout();
        for field_index in 0..row_layout.field_count() {
            let field = row_layout.contract().field_decode_contract(field_index)?;
            let field_name = field.name();
            let field_kind = field.kind();

            let value = entity.get_value_by_index(field_index).ok_or_else(|| {
                InternalError::mutation_entity_field_missing(
                    E::PATH,
                    field_name,
                    field_is_indexed::<E>(field_name),
                )
            })?;

            if matches!(value, Value::Null | Value::Unit) {
                // Null = absent, Unit = singleton sentinel; both skip type checks.
                continue;
            }

            // Phase 3: enforce runtime shape, scalar bounds, and deterministic
            // collection/map encodings for user-authored values.
            Self::validate_authored_field_value_invariants(
                schema,
                field_name,
                &field_kind,
                &value,
            )?;
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
        let row_layout = authority.row_layout();

        for field_index in 0..row_layout.field_count() {
            let field = row_fields.field_decode_contract(field_index)?;
            let field_name = field.name();
            let field_kind = field.kind();

            let value = row_fields.required_cached_value(field_index)?;

            if matches!(value, Value::Null | Value::Unit) {
                // Null = absent, Unit = singleton sentinel; both skip type checks.
                continue;
            }

            // Phase 1: enforce runtime shape, exact scalar bounds, and
            // deterministic collection/map encodings for already persisted rows.
            Self::validate_persisted_field_value_invariants(
                schema,
                field_name,
                &field_kind,
                value,
            )?;
        }

        Ok(())
    }

    // Validate a user-authored runtime field value before write encoding can
    // normalize fixed-scale decimal values into the persisted field shape.
    fn validate_authored_field_value_invariants(
        schema: &SchemaInfo,
        field_name: &'static str,
        field_kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if !Self::validate_queryable_field_value_shape(schema, field_name, field_kind, value)? {
            return Ok(());
        }

        Self::validate_decimal_scale_is_normalizable(field_name, field_kind, value)?;
        Self::validate_text_max_len(field_name, field_kind, value)?;
        Self::validate_deterministic_field_value(field_name, field_kind, value)
    }

    // Validate an already materialized persisted row value. These values have
    // already passed storage decode, so decimal scale must be exact rather than
    // merely normalizable.
    fn validate_persisted_field_value_invariants(
        schema: &SchemaInfo,
        field_name: &'static str,
        field_kind: &FieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if !Self::validate_queryable_field_value_shape(schema, field_name, field_kind, value)? {
            return Ok(());
        }

        Self::validate_decimal_scale_exact(field_name, field_kind, value)?;
        Self::validate_text_max_len(field_name, field_kind, value)?;
        Self::validate_deterministic_field_value(field_name, field_kind, value)
    }

    // Enforce the shared query-visible field shape checks used by both
    // user-authored entity values and structural persisted-row validation.
    fn validate_queryable_field_value_shape(
        schema: &SchemaInfo,
        field_name: &'static str,
        field_kind: &FieldKind,
        value: &Value,
    ) -> Result<bool, InternalError> {
        if !field_kind.value_kind().is_queryable() {
            // Non-queryable structured fields are not planner-addressable.
            return Ok(false);
        }

        let Some(field_type) = schema.field(field_name) else {
            // Runtime-only field; treat as non-queryable.
            return Ok(false);
        };

        if !literal_matches_type(value, field_type) && !field_kind.accepts_value(value) {
            return Err(InternalError::mutation_entity_field_type_mismatch(
                E::PATH,
                field_name,
                value,
            ));
        }

        Ok(true)
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
                normalize_decimal_scale_for_storage(*kind, value)
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
