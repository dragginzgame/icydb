//! Module: executor::mutation::save_validation
//! Responsibility: save preflight invariant enforcement for entity values.
//! Does not own: commit-window apply mechanics or relation metadata ownership.
//! Boundary: validation-only helpers invoked before save commit planning.

#[cfg(test)]
use crate::model::field::FieldKind;
use crate::{
    db::{
        PersistedRow,
        data::{DataKey, RawRow, StructuralPatch, StructuralRowContract, StructuralSlotReader},
        executor::mutation::save::SaveExecutor,
        predicate::canonical_cmp,
        relation::validate_save_strong_relations_with_accepted_contract,
        schema::{AcceptedRowDecodeContract, PersistedFieldKind, SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    sanitize::{SanitizeWriteContext, sanitize_with_context},
    traits::EntityValue,
    validate::validate,
    value::Value,
};
use std::cmp::Ordering;

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Enforce accepted-contract scalar bounds before structural patch values are
    // serialized into persisted-row payloads. The accepted lane uses only the
    // selected schema snapshot's field contracts, so out-of-range slots fail
    // before write encoding.
    pub(in crate::db::executor::mutation) fn validate_structural_patch_write_bounds_with_accepted_contract(
        patch: &StructuralPatch,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<(), InternalError> {
        let contract = StructuralRowContract::from_accepted_decode_contract(
            E::PATH,
            accepted_row_decode_contract.clone(),
        );

        Self::validate_structural_patch_write_bounds_for_accepted_row_contract(patch, &contract)
    }

    // Enforce write-boundary scalar bounds for accepted structural patch input.
    // The accepted lane uses only the selected schema snapshot's field
    // contracts, so out-of-range slots fail before write encoding.
    fn validate_structural_patch_write_bounds_for_accepted_row_contract(
        patch: &StructuralPatch,
        contract: &StructuralRowContract,
    ) -> Result<(), InternalError> {
        for entry in patch.entries() {
            let slot = entry.slot().index();
            let accepted_field = contract.required_accepted_field_decode_contract(slot)?;

            Self::validate_persisted_decimal_scale_is_normalizable(
                accepted_field.field_name(),
                accepted_field.kind(),
                entry.value(),
            )?;
            Self::validate_persisted_text_max_len(
                accepted_field.field_name(),
                accepted_field.kind(),
                entry.value(),
            )?;
        }

        Ok(())
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
        let schema = Self::schema_info();
        let contract = StructuralRowContract::from_accepted_decode_contract(
            E::PATH,
            accepted_row_decode_contract,
        );
        let row_fields = StructuralSlotReader::from_raw_row_with_validated_contract(row, contract)?;
        row_fields.validate_storage_key(data_key)?;

        Self::validate_structural_row_invariants_with_accepted_contract(&row_fields, schema)
    }

    // Load the trusted generated schema view for one entity type.
    pub(in crate::db::executor::mutation) fn schema_info() -> &'static SchemaInfo {
        SchemaInfo::cached_for_entity_model(E::MODEL)
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
        self.validate_create_authorship(authored_create_slots)?;
        sanitize_with_context(entity, Some(write_context))?;
        validate(entity)?;
        self.validate_entity_invariants(entity, schema)?;
        if validate_relations {
            validate_save_strong_relations_with_accepted_contract::<E>(
                &self.db,
                entity,
                self.accepted_row_decode_contract(),
            )?;
        }

        Ok(())
    }

    // Enforce the typed create authorship contract for generated create-input
    // payloads. Every user-authorable create field must be explicitly present;
    // only generated or managed fields may be omitted by the create type.
    fn validate_create_authorship(
        &self,
        authored_create_slots: Option<&[usize]>,
    ) -> Result<(), InternalError> {
        let Some(authored_create_slots) = authored_create_slots else {
            return Ok(());
        };

        let missing_fields = Self::missing_authored_fields_with_accepted_contract(
            self.accepted_row_decode_contract(),
            authored_create_slots,
        );

        if missing_fields.is_empty() {
            return Ok(());
        }

        Err(InternalError::mutation_create_missing_authored_fields(
            E::PATH,
            &missing_fields.join(", "),
        ))
    }

    // Resolve missing authored fields from accepted schema write policy when
    // the save lane has an accepted row contract. This keeps typed create
    // authorship on persisted schema facts instead of generated `FieldModel`
    // write policy metadata.
    fn missing_authored_fields_with_accepted_contract(
        accepted_contract: &AcceptedRowDecodeContract,
        authored_create_slots: &[usize],
    ) -> Vec<String> {
        (0..accepted_contract.required_slot_count())
            .filter_map(|slot| {
                let field = accepted_contract.field_for_slot(slot)?;
                let write_policy = field.write_policy();
                let requires_authorship = write_policy.insert_generation().is_none()
                    && write_policy.write_management().is_none();

                (requires_authorship && !authored_create_slots.contains(&slot))
                    .then(|| field.field_name().to_string())
            })
            .collect()
    }

    // Enforce trait boundary invariants for user-provided entities.
    fn validate_entity_invariants(
        &self,
        entity: &E,
        schema: &SchemaInfo,
    ) -> Result<(), InternalError> {
        let accepted_contract = self.accepted_row_decode_contract();
        let (primary_key_name, pk_field_index) =
            Self::accepted_primary_key_field(accepted_contract)?;

        // Phase 1: validate primary key field presence and *shape*.
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
        Self::validate_entity_field_invariants_with_accepted_contract(
            entity,
            schema,
            accepted_contract,
        )?;

        Ok(())
    }

    // Resolve the primary-key name and physical slot from the accepted row
    // contract. Accepted save lanes use this instead of reopening generated
    // row layout metadata after schema compatibility has already been proven.
    fn accepted_primary_key_field(
        accepted_contract: &AcceptedRowDecodeContract,
    ) -> Result<(&str, usize), InternalError> {
        let primary_key_slot = accepted_contract.primary_key_slot_index();
        let primary_key_field = accepted_contract
            .field_for_slot(primary_key_slot)
            .ok_or_else(|| {
                InternalError::persisted_row_slot_lookup_out_of_bounds(E::PATH, primary_key_slot)
            })?;

        Ok((primary_key_field.field_name(), primary_key_slot))
    }

    // Validate typed entity field values against accepted schema field facts
    // when the save lane has an accepted row contract. Decimal checks remain
    // normalizing because these are authored typed values before row encoding.
    fn validate_entity_field_invariants_with_accepted_contract(
        entity: &E,
        schema: &SchemaInfo,
        accepted_contract: &AcceptedRowDecodeContract,
    ) -> Result<(), InternalError> {
        for field_index in 0..accepted_contract.required_slot_count() {
            let field = accepted_contract
                .field_for_slot(field_index)
                .ok_or_else(|| {
                    InternalError::persisted_row_slot_lookup_out_of_bounds(E::PATH, field_index)
                })?;
            let field_name = field.field_name();
            let field_kind = field.kind();

            let value = entity.get_value_by_index(field_index).ok_or_else(|| {
                InternalError::mutation_entity_field_missing(
                    E::PATH,
                    field_name,
                    schema.field_is_indexed(field_name),
                )
            })?;

            if matches!(value, Value::Null | Value::Unit) {
                // Null = absent, Unit = singleton sentinel; both skip type checks.
                continue;
            }

            // Phase 3: enforce runtime shape, scalar bounds, and deterministic
            // collection/map encodings using accepted persisted field metadata.
            Self::validate_accepted_authored_field_value_invariants(
                schema, field_name, field_kind, &value,
            )?;
        }

        Ok(())
    }

    // Validate an already materialized accepted-schema structural row against
    // the saved field contracts selected for this read/write boundary.
    fn validate_structural_row_invariants_with_accepted_contract(
        row_fields: &StructuralSlotReader<'_>,
        schema: &SchemaInfo,
    ) -> Result<(), InternalError> {
        for field_index in 0..row_fields.field_count() {
            let value = row_fields.required_cached_value(field_index)?;

            if matches!(value, Value::Null | Value::Unit) {
                // Null = absent, Unit = singleton sentinel; both skip type checks.
                continue;
            }

            // Phase 1: enforce runtime shape, exact scalar bounds, and
            // deterministic collection/map encodings for persisted rows.
            let accepted_field = row_fields
                .contract()
                .required_accepted_field_decode_contract(field_index)?;

            Self::validate_accepted_persisted_field_value_invariants(
                schema,
                accepted_field.field_name(),
                accepted_field.kind(),
                value,
            )?;
        }

        Ok(())
    }

    // Validate one typed entity field value against accepted schema field facts.
    // Authored typed values still pass through write encoding after this check,
    // so decimal scale validation uses the normalizable rule rather than the
    // exact persisted-row rule.
    fn validate_accepted_authored_field_value_invariants(
        schema: &SchemaInfo,
        field_name: &str,
        field_kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if !Self::validate_accepted_queryable_field_value_shape(
            schema, field_name, field_kind, value,
        )? {
            return Ok(());
        }

        Self::validate_persisted_decimal_scale_is_normalizable(field_name, field_kind, value)?;
        Self::validate_persisted_text_max_len(field_name, field_kind, value)?;
        Self::validate_persisted_deterministic_field_value(field_name, field_kind, value)
    }

    // Validate one accepted-schema persisted row value against the saved field
    // kind. Generated `FieldKind` remains the typed write fallback only when no
    // accepted row contract is attached.
    fn validate_accepted_persisted_field_value_invariants(
        schema: &SchemaInfo,
        field_name: &str,
        field_kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if !Self::validate_accepted_queryable_field_value_shape(
            schema, field_name, field_kind, value,
        )? {
            return Ok(());
        }

        Self::validate_persisted_decimal_scale_exact(field_name, field_kind, value)?;
        Self::validate_persisted_text_max_len(field_name, field_kind, value)?;
        Self::validate_persisted_deterministic_field_value(field_name, field_kind, value)
    }

    // Enforce the query-visible field shape checks using accepted persisted
    // field kind metadata instead of generated `FieldKind` metadata.
    fn validate_accepted_queryable_field_value_shape(
        schema: &SchemaInfo,
        field_name: &str,
        field_kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<bool, InternalError> {
        if !persisted_field_kind_is_queryable(field_kind) {
            // Non-queryable structured fields are not planner-addressable.
            return Ok(false);
        }

        let Some(field_type) = schema.field(field_name) else {
            // Runtime-only field; treat as non-queryable.
            return Ok(false);
        };

        if !literal_matches_type(value, field_type)
            && !persisted_field_kind_accepts_value(field_kind, value)
        {
            return Err(InternalError::mutation_entity_field_type_mismatch(
                E::PATH,
                field_name,
                value,
            ));
        }

        Ok(true)
    }

    // Accept authored decimal values against accepted persisted schema
    // metadata. This mirrors generated `FieldKind` validation without
    // converting accepted field kinds back into generated metadata.
    fn validate_persisted_decimal_scale_is_normalizable(
        field_name: &str,
        kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (PersistedFieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
                let normalizable = match decimal.scale().cmp(scale) {
                    Ordering::Equal | Ordering::Greater => true,
                    Ordering::Less => decimal.scale_to_integer(*scale).is_some(),
                };
                if normalizable {
                    return Ok(());
                }

                Err(InternalError::mutation_decimal_scale_mismatch(
                    E::PATH,
                    field_name,
                    scale,
                    decimal.scale(),
                ))
            }
            (PersistedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_decimal_scale_is_normalizable(field_name, key_kind, value)
            }
            (
                PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner),
                Value::List(items),
            ) => {
                for item in items {
                    Self::validate_persisted_decimal_scale_is_normalizable(
                        field_name, inner, item,
                    )?;
                }

                Ok(())
            }
            (
                PersistedFieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_persisted_decimal_scale_is_normalizable(
                        field_name, key, entry_key,
                    )?;
                    Self::validate_persisted_decimal_scale_is_normalizable(
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

    // Enforce exact decimal scale for values already decoded from accepted
    // persisted schema metadata.
    fn validate_persisted_decimal_scale_exact(
        field_name: &str,
        kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (PersistedFieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
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
            (PersistedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_decimal_scale_exact(field_name, key_kind, value)
            }
            (
                PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner),
                Value::List(items),
            ) => {
                for item in items {
                    Self::validate_persisted_decimal_scale_exact(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                PersistedFieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_persisted_decimal_scale_exact(field_name, key, entry_key)?;
                    Self::validate_persisted_decimal_scale_exact(
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

    /// Enforce accepted persisted-schema text bounds across nested values.
    fn validate_persisted_text_max_len(
        field_name: &str,
        kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (PersistedFieldKind::Text { max_len: Some(max) }, Value::Text(text)) => {
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
            (PersistedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_text_max_len(field_name, key_kind, value)
            }
            (
                PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner),
                Value::List(items),
            ) => {
                for item in items {
                    Self::validate_persisted_text_max_len(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                PersistedFieldKind::Map {
                    key,
                    value: map_value,
                },
                Value::Map(entries),
            ) => {
                for (entry_key, entry_value) in entries {
                    Self::validate_persisted_text_max_len(field_name, key, entry_key)?;
                    Self::validate_persisted_text_max_len(field_name, map_value, entry_value)?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Enforce deterministic value encodings for collection-like field kinds.
    #[cfg(test)]
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

    // Enforce deterministic collection encodings for accepted persisted schema
    // field kinds.
    fn validate_persisted_deterministic_field_value(
        field_name: &str,
        kind: &PersistedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        match kind {
            PersistedFieldKind::Set(_) => Self::validate_set_encoding(field_name, value),
            PersistedFieldKind::Map { .. } => Self::validate_map_encoding(field_name, value),
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

// Return whether one accepted persisted field kind is planner/query-visible.
// This mirrors generated `FieldKind::value_kind().is_queryable()` for saved
// schema metadata without converting accepted field kinds back into generated
// model metadata.
const fn persisted_field_kind_is_queryable(kind: &PersistedFieldKind) -> bool {
    match kind {
        PersistedFieldKind::Map { .. } => false,
        PersistedFieldKind::Structured { queryable } => *queryable,
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Enum { .. }
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::List(_)
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Relation { .. }
        | PersistedFieldKind::Set(_)
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Uint
        | PersistedFieldKind::Uint128
        | PersistedFieldKind::UintBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => true,
    }
}

// Match one runtime value against accepted persisted schema kind metadata.
// Relation, list/set, and map shapes recurse exactly like generated field
// validation, but the accepted snapshot remains the kind authority.
fn persisted_field_kind_accepts_value(kind: &PersistedFieldKind, value: &Value) -> bool {
    match (kind, value) {
        (PersistedFieldKind::Account, Value::Account(_))
        | (PersistedFieldKind::Blob { .. }, Value::Blob(_))
        | (PersistedFieldKind::Bool, Value::Bool(_))
        | (PersistedFieldKind::Date, Value::Date(_))
        | (PersistedFieldKind::Decimal { .. }, Value::Decimal(_))
        | (PersistedFieldKind::Duration, Value::Duration(_))
        | (PersistedFieldKind::Enum { .. }, Value::Enum(_))
        | (PersistedFieldKind::Float32, Value::Float32(_))
        | (PersistedFieldKind::Float64, Value::Float64(_))
        | (PersistedFieldKind::Int, Value::Int(_))
        | (PersistedFieldKind::Int128, Value::Int128(_))
        | (PersistedFieldKind::IntBig, Value::IntBig(_))
        | (PersistedFieldKind::Principal, Value::Principal(_))
        | (PersistedFieldKind::Subaccount, Value::Subaccount(_))
        | (PersistedFieldKind::Text { .. }, Value::Text(_))
        | (PersistedFieldKind::Timestamp, Value::Timestamp(_))
        | (PersistedFieldKind::Uint, Value::Uint(_))
        | (PersistedFieldKind::Uint128, Value::Uint128(_))
        | (PersistedFieldKind::UintBig, Value::UintBig(_))
        | (PersistedFieldKind::Ulid, Value::Ulid(_))
        | (PersistedFieldKind::Unit, Value::Unit)
        | (PersistedFieldKind::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
        (PersistedFieldKind::Relation { key_kind, .. }, value) => {
            persisted_field_kind_accepts_value(key_kind, value)
        }
        (PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner), Value::List(items)) => {
            items
                .iter()
                .all(|item| persisted_field_kind_accepts_value(inner, item))
        }
        (PersistedFieldKind::Map { key, value }, Value::Map(entries)) => {
            if Value::validate_map_entries(entries.as_slice()).is_err() {
                return false;
            }

            entries.iter().all(|(entry_key, entry_value)| {
                persisted_field_kind_accepts_value(key, entry_key)
                    && persisted_field_kind_accepts_value(value, entry_value)
            })
        }
        _ => false,
    }
}
