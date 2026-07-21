//! Module: executor::mutation::save_validation
//! Responsibility: save preflight invariant enforcement for entity values.
//! Does not own: commit-window apply mechanics or relation metadata ownership.
//! Boundary: validation-only helpers invoked before save commit planning.

#[cfg(test)]
use crate::model::field::FieldKind;
use crate::{
    db::{
        PersistedRow,
        data::{
            AuthoredStructuralPatch, DecodedDataStoreKey, RawRow, StructuralRowContract,
            StructuralSlotReader,
        },
        executor::mutation::save::SaveExecutor,
        predicate::canonical_cmp,
        relation::validate_save_relations_with_accepted_contract,
        schema::{
            AcceptedFieldAbsencePolicy, AcceptedFieldKind, AcceptedFieldKindCategory,
            AcceptedRowDecodeContract, AcceptedScalarClass, SchemaInfo,
            accepted_insert_field_is_omittable, classify_accepted_field_kind, literal_matches_type,
        },
    },
    error::InternalError,
    sanitize::{SanitizeWriteContext, sanitize_with_context},
    validate::validate,
    value::Value,
};
use std::cmp::Ordering;

// Resolve typed-create omissions exclusively from the accepted insert
// contract. Generated create DTOs retain authored-slot provenance, but they do
// not decide whether an omitted field is legal at runtime.
fn missing_create_authored_fields(
    accepted_contract: &AcceptedRowDecodeContract,
    authored_create_slots: &[usize],
) -> Vec<String> {
    let mut missing = Vec::new();
    for slot in 0..accepted_contract.required_slot_count() {
        let Some(field) = accepted_contract.field_for_slot(slot) else {
            continue;
        };
        if accepted_insert_field_is_omittable(field.absence_policy(), field.write_policy()) {
            continue;
        }
        if !authored_create_slots.contains(&slot) {
            missing.push(field.field_name().to_string());
        }
    }

    missing
}

impl<E: PersistedRow> SaveExecutor<E> {
    // Enforce accepted-contract scalar bounds before structural patch values are
    // serialized into persisted-row payloads. The accepted lane uses only the
    // selected schema snapshot's field contracts, so out-of-range slots fail
    // before write encoding.
    pub(in crate::db::executor::mutation) fn validate_structural_patch_write_bounds_with_accepted_contract(
        patch: &AuthoredStructuralPatch,
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
        patch: &AuthoredStructuralPatch,
        contract: &StructuralRowContract,
    ) -> Result<(), InternalError> {
        for entry in patch.entries() {
            let slot = entry.slot().index();
            let accepted_field = contract.required_accepted_field_decode_contract(slot)?;
            let Some(value) = entry.value().clone().try_into_runtime_non_enum() else {
                // Canonical enum admission owns enum and recursive payload
                // validation during accepted patch serialization.
                continue;
            };

            Self::validate_persisted_decimal_scale_is_normalizable(
                accepted_field.field_name(),
                accepted_field.kind(),
                &value,
            )?;
            Self::validate_persisted_text_max_len(
                accepted_field.field_name(),
                accepted_field.kind(),
                &value,
            )?;
        }

        Ok(())
    }

    // Validate one persisted row through an accepted row-decode contract before
    // structural updates use it as a baseline. This keeps old short rows inside
    // the accepted-schema path while preserving the same storage-key and field
    // invariant checks as generated-only rows.
    pub(in crate::db::executor::mutation) fn ensure_persisted_row_invariants_with_accepted_contract(
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        schema: &SchemaInfo,
    ) -> Result<(), InternalError> {
        let contract = StructuralRowContract::from_accepted_decode_contract(
            E::PATH,
            accepted_row_decode_contract,
        );
        let row_fields = StructuralSlotReader::from_raw_row_with_validated_contract(row, contract)?;
        row_fields.validate_primary_key(data_key)?;

        Self::validate_structural_row_invariants_with_accepted_contract(&row_fields, schema)
    }

    // Execute save preflight using already-resolved schema and relation metadata.
    //
    // Batch save lanes call this helper so they do not repay the schema-cache
    // mutex lookup and relation capability probe for every row.
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
            validate_save_relations_with_accepted_contract::<E>(
                &self.db,
                entity,
                self.accepted_row_decode_contract(),
            )?;
        }

        Ok(())
    }

    // Enforce the typed create authorship contract for generated create-input
    // payloads. Accepted null/default policy and database-owned generation may
    // satisfy an omission; every other user-authorable field must be present.
    fn validate_create_authorship(
        &self,
        authored_create_slots: Option<&[usize]>,
    ) -> Result<(), InternalError> {
        let Some(authored_create_slots) = authored_create_slots else {
            return Ok(());
        };

        let missing_fields = missing_create_authored_fields(
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

    // Enforce trait boundary invariants for user-provided entities.
    fn validate_entity_invariants(
        &self,
        entity: &E,
        schema: &SchemaInfo,
    ) -> Result<(), InternalError> {
        let accepted_contract = self.accepted_row_decode_contract();
        let primary_key_slots = accepted_contract.primary_key_slot_indices();
        if primary_key_slots.len() > 1 {
            Self::validate_composite_entity_primary_key_invariants(
                entity,
                schema,
                accepted_contract,
                primary_key_slots,
            )?;
        } else {
            let primary_key_slot = primary_key_slots
                .first()
                .copied()
                .unwrap_or_else(|| accepted_contract.first_primary_key_slot_index());
            Self::validate_scalar_entity_primary_key_invariants(
                entity,
                schema,
                accepted_contract,
                primary_key_slot,
            )?;
        }

        // Phase 2: validate field presence and runtime value shapes.
        Self::validate_entity_field_invariants_with_accepted_contract(
            entity,
            schema,
            accepted_contract,
        )?;

        Ok(())
    }

    fn validate_scalar_entity_primary_key_invariants(
        entity: &E,
        schema: &SchemaInfo,
        accepted_contract: &AcceptedRowDecodeContract,
        primary_key_slot: usize,
    ) -> Result<(), InternalError> {
        let (primary_key_name, pk_field_index) =
            Self::accepted_primary_key_field_at_slot(accepted_contract, primary_key_slot)?;

        let pk_value = entity.get_value_by_index(pk_field_index).ok_or_else(|| {
            InternalError::mutation_entity_primary_key_missing(E::PATH, primary_key_name)
        })?;

        if matches!(pk_value, Value::Null) {
            return Err(InternalError::mutation_entity_primary_key_invalid_value(
                E::PATH,
                primary_key_name,
                &pk_value,
            ));
        }

        if let Some(pk_type) = schema.field(primary_key_name)
            && !literal_matches_type(&pk_value, pk_type)
        {
            return Err(InternalError::mutation_entity_primary_key_type_mismatch(
                E::PATH,
                primary_key_name,
                &pk_value,
            ));
        }

        let identity_pk = crate::db::KeyValueCodec::to_key_value(&entity.id().key());
        if pk_value != identity_pk {
            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                primary_key_name,
                &pk_value,
                &identity_pk,
            ));
        }

        Ok(())
    }

    fn validate_composite_entity_primary_key_invariants(
        entity: &E,
        schema: &SchemaInfo,
        accepted_contract: &AcceptedRowDecodeContract,
        primary_key_slots: &[usize],
    ) -> Result<(), InternalError> {
        let identity_pk = crate::db::KeyValueCodec::to_key_value(&entity.id().key());
        let Value::List(identity_components) = &identity_pk else {
            return Err(InternalError::executor_invariant());
        };

        if identity_components.len() != primary_key_slots.len() {
            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                &Self::accepted_primary_key_field_label(accepted_contract, primary_key_slots)?,
                &Value::List(Vec::new()),
                &identity_pk,
            ));
        }

        for (slot, identity_component) in primary_key_slots
            .iter()
            .copied()
            .zip(identity_components.iter())
        {
            let (primary_key_name, pk_field_index) =
                Self::accepted_primary_key_field_at_slot(accepted_contract, slot)?;
            let pk_value = entity.get_value_by_index(pk_field_index).ok_or_else(|| {
                InternalError::mutation_entity_primary_key_missing(E::PATH, primary_key_name)
            })?;

            if matches!(pk_value, Value::Null) {
                return Err(InternalError::mutation_entity_primary_key_invalid_value(
                    E::PATH,
                    primary_key_name,
                    &pk_value,
                ));
            }

            if let Some(pk_type) = schema.field(primary_key_name)
                && !literal_matches_type(&pk_value, pk_type)
            {
                return Err(InternalError::mutation_entity_primary_key_type_mismatch(
                    E::PATH,
                    primary_key_name,
                    &pk_value,
                ));
            }

            if pk_value != *identity_component {
                return Err(InternalError::mutation_entity_primary_key_mismatch(
                    E::PATH,
                    primary_key_name,
                    &pk_value,
                    identity_component,
                ));
            }
        }

        Ok(())
    }

    // Resolve one primary-key name and physical slot from the accepted row
    // contract. Accepted save lanes use this instead of reopening generated
    // row layout metadata after schema compatibility has already been proven.
    fn accepted_primary_key_field_at_slot(
        accepted_contract: &AcceptedRowDecodeContract,
        primary_key_slot: usize,
    ) -> Result<(&str, usize), InternalError> {
        let primary_key_field =
            accepted_contract.required_field_for_slot(E::PATH, primary_key_slot)?;

        Ok((primary_key_field.field_name(), primary_key_slot))
    }

    fn accepted_primary_key_field_label(
        accepted_contract: &AcceptedRowDecodeContract,
        primary_key_slots: &[usize],
    ) -> Result<String, InternalError> {
        let mut names = Vec::with_capacity(primary_key_slots.len());
        for slot in primary_key_slots {
            let field = accepted_contract.required_field_for_slot(E::PATH, *slot)?;
            names.push(field.field_name());
        }

        Ok(names.join(", "))
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
            let Some(field) = accepted_contract.field_for_slot(field_index) else {
                continue;
            };
            let field_name = field.field_name();
            let field_kind = field.kind();
            if !field.generated()
                && !matches!(field.absence_policy(), AcceptedFieldAbsencePolicy::Required)
            {
                continue;
            }

            let Some(value) = entity.get_value_by_index(field_index) else {
                if entity.get_input_value_by_index(field_index).is_some() {
                    // Enum and generated composite fields remain authored
                    // input until the accepted row encoder admits them. Their
                    // absence from the runtime projection is intentional.
                    continue;
                }
                return Err(InternalError::mutation_entity_field_missing(
                    E::PATH,
                    field_name,
                    schema.field_is_indexed(field_name),
                ));
            };

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
        field_kind: &AcceptedFieldKind,
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
        field_kind: &AcceptedFieldKind,
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
        field_kind: &AcceptedFieldKind,
        value: &Value,
    ) -> Result<bool, InternalError> {
        if !persisted_field_kind_is_queryable(field_kind) {
            // Whole composite values are not planner-addressable.
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
        kind: &AcceptedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (AcceptedFieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
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
            (AcceptedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_decimal_scale_is_normalizable(field_name, key_kind, value)
            }
            (
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner),
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
                AcceptedFieldKind::Map {
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
        kind: &AcceptedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (AcceptedFieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
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
            (AcceptedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_decimal_scale_exact(field_name, key_kind, value)
            }
            (
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner),
                Value::List(items),
            ) => {
                for item in items {
                    Self::validate_persisted_decimal_scale_exact(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                AcceptedFieldKind::Map {
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
        kind: &AcceptedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null | Value::Unit) {
            return Ok(());
        }

        match (kind, value) {
            (AcceptedFieldKind::Text { max_len: Some(max) }, Value::Text(text)) => {
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
            (AcceptedFieldKind::Relation { key_kind, .. }, value) => {
                Self::validate_persisted_text_max_len(field_name, key_kind, value)
            }
            (
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner),
                Value::List(items),
            ) => {
                for item in items {
                    Self::validate_persisted_text_max_len(field_name, inner, item)?;
                }

                Ok(())
            }
            (
                AcceptedFieldKind::Map {
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
        kind: &AcceptedFieldKind,
        value: &Value,
    ) -> Result<(), InternalError> {
        match kind {
            AcceptedFieldKind::Set(_) => Self::validate_set_encoding(field_name, value),
            AcceptedFieldKind::Map { .. } => Self::validate_map_encoding(field_name, value),
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
const fn persisted_field_kind_is_queryable(kind: &AcceptedFieldKind) -> bool {
    match classify_accepted_field_kind(kind).category() {
        AcceptedFieldKindCategory::Scalar(_) | AcceptedFieldKindCategory::Relation(_) => true,
        AcceptedFieldKindCategory::Collection => !matches!(kind, AcceptedFieldKind::Map { .. }),
        AcceptedFieldKindCategory::Composite => false,
    }
}

// Match one runtime value against accepted persisted schema kind metadata.
// Relation, list/set, and map shapes recurse exactly like generated field
// validation, but the accepted snapshot remains the kind authority.
fn persisted_field_kind_accepts_value(kind: &AcceptedFieldKind, value: &Value) -> bool {
    match classify_accepted_field_kind(kind).category() {
        AcceptedFieldKindCategory::Scalar(class) => {
            persisted_scalar_class_accepts_value(kind, class, value)
        }
        AcceptedFieldKindCategory::Relation(_) => {
            let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
                return false;
            };

            persisted_field_kind_accepts_value(key_kind, value)
        }
        AcceptedFieldKindCategory::Collection => {
            persisted_collection_kind_accepts_value(kind, value)
        }
        // Exact composite shape admission requires the accepted composite
        // catalog, which is intentionally owned by the later authored-field
        // encoding boundary. Save preflight still validates every enclosing
        // list/set/map shape here, then defers the nominal leaf contract.
        AcceptedFieldKindCategory::Composite => true,
    }
}

fn persisted_scalar_class_accepts_value(
    kind: &AcceptedFieldKind,
    class: AcceptedScalarClass,
    value: &Value,
) -> bool {
    match (class, value) {
        (AcceptedScalarClass::Account, Value::Account(_))
        | (AcceptedScalarClass::Blob, Value::Blob(_))
        | (AcceptedScalarClass::Bool, Value::Bool(_))
        | (AcceptedScalarClass::Date, Value::Date(_))
        | (AcceptedScalarClass::Decimal, Value::Decimal(_))
        | (AcceptedScalarClass::Duration, Value::Duration(_))
        | (AcceptedScalarClass::Enum, Value::Enum(_))
        | (AcceptedScalarClass::Float32, Value::Float32(_))
        | (AcceptedScalarClass::Float64, Value::Float64(_))
        | (AcceptedScalarClass::Signed128, Value::Int128(_))
        | (AcceptedScalarClass::Principal, Value::Principal(_))
        | (AcceptedScalarClass::Subaccount, Value::Subaccount(_))
        | (AcceptedScalarClass::Text, Value::Text(_))
        | (AcceptedScalarClass::Timestamp, Value::Timestamp(_))
        | (AcceptedScalarClass::Unsigned128, Value::Nat128(_))
        | (AcceptedScalarClass::Ulid, Value::Ulid(_))
        | (AcceptedScalarClass::Unit, Value::Unit) => true,
        (AcceptedScalarClass::Signed64, Value::Int64(value)) => {
            persisted_signed64_kind_accepts_value(kind, *value)
        }
        (AcceptedScalarClass::SignedBig, Value::IntBig(value)) => {
            let AcceptedFieldKind::IntBig { max_bytes } = kind else {
                return false;
            };

            value.to_leb128().len() <= *max_bytes as usize
        }
        (AcceptedScalarClass::Unsigned64, Value::Nat64(value)) => {
            persisted_unsigned64_kind_accepts_value(kind, *value)
        }
        (AcceptedScalarClass::UnsignedBig, Value::NatBig(value)) => {
            let AcceptedFieldKind::NatBig { max_bytes } = kind else {
                return false;
            };

            value.to_leb128().len() <= *max_bytes as usize
        }
        _ => false,
    }
}

const fn persisted_signed64_kind_accepts_value(kind: &AcceptedFieldKind, value: i64) -> bool {
    match kind {
        AcceptedFieldKind::Int8 => value >= i8::MIN as i64 && value <= i8::MAX as i64,
        AcceptedFieldKind::Int16 => value >= i16::MIN as i64 && value <= i16::MAX as i64,
        AcceptedFieldKind::Int32 => value >= i32::MIN as i64 && value <= i32::MAX as i64,
        AcceptedFieldKind::Int64 => true,
        _ => false,
    }
}

const fn persisted_unsigned64_kind_accepts_value(kind: &AcceptedFieldKind, value: u64) -> bool {
    match kind {
        AcceptedFieldKind::Nat8 => value <= u8::MAX as u64,
        AcceptedFieldKind::Nat16 => value <= u16::MAX as u64,
        AcceptedFieldKind::Nat32 => value <= u32::MAX as u64,
        AcceptedFieldKind::Nat64 => true,
        _ => false,
    }
}

fn persisted_collection_kind_accepts_value(kind: &AcceptedFieldKind, value: &Value) -> bool {
    match (kind, value) {
        (AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner), Value::List(items)) => {
            items
                .iter()
                .all(|item| persisted_field_kind_accepts_value(inner, item))
        }
        (AcceptedFieldKind::Map { key, value }, Value::Map(entries)) => {
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

#[cfg(test)]
mod tests {
    use super::{
        missing_create_authored_fields, persisted_field_kind_accepts_value,
        persisted_field_kind_is_queryable,
    };
    use crate::{
        db::schema::{AcceptedFieldKind, AcceptedRowDecodeContract},
        model::{
            EntityModel, IndexModel,
            field::{
                FieldDatabaseDefault, FieldInsertGeneration, FieldKind, FieldModel,
                FieldStorageDecode,
            },
        },
        types::EntityTag,
        value::Value,
    };

    static CREATE_DEFAULT_PAYLOAD: &[u8] = &[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0];
    static CREATE_FIELDS: [FieldModel; 4] = [
        FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
            "id",
            FieldKind::Ulid,
            FieldStorageDecode::ByKind,
            false,
            Some(FieldInsertGeneration::Ulid),
            None,
            FieldDatabaseDefault::None,
            &[],
        ),
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated_with_storage_decode_and_nullability(
            "note",
            FieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            true,
        ),
        FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
            "score",
            FieldKind::Nat64,
            FieldStorageDecode::ByKind,
            false,
            None,
            None,
            FieldDatabaseDefault::EncodedSlotPayload(CREATE_DEFAULT_PAYLOAD),
            &[],
        ),
    ];
    static CREATE_INDEXES: [&IndexModel; 0] = [];
    static CREATE_MODEL: EntityModel = EntityModel::generated(
        "tests::CreateDefaultEntity",
        "create_default_entity",
        1,
        &CREATE_FIELDS[0],
        0,
        &CREATE_FIELDS,
        &CREATE_INDEXES,
    );

    fn relation_to_key(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
        AcceptedFieldKind::Relation {
            target_path: "target::Entity".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "target::Store".into(),
            key_kind: Box::new(key_kind),
        }
    }

    #[test]
    fn typed_create_omissions_follow_accepted_insert_policy() {
        let contract = AcceptedRowDecodeContract::from_model_proposal_for_test(&CREATE_MODEL);

        assert!(missing_create_authored_fields(&contract, &[1]).is_empty());
        assert_eq!(
            missing_create_authored_fields(&contract, &[]),
            vec!["name".to_string()],
        );
    }

    #[test]
    fn persisted_field_kind_queryability_uses_schema_semantics_for_kind_shape() {
        let scalar_kind = AcceptedFieldKind::Nat64;
        let relation_kind = relation_to_key(AcceptedFieldKind::Ulid);
        let list_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
        let set_kind = AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64));
        let map_kind = AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat64),
        };
        let composite = AcceptedFieldKind::test_composite();

        assert!(persisted_field_kind_is_queryable(&scalar_kind));
        assert!(persisted_field_kind_is_queryable(&relation_kind));
        assert!(persisted_field_kind_is_queryable(&list_kind));
        assert!(persisted_field_kind_is_queryable(&set_kind));
        assert!(!persisted_field_kind_is_queryable(&map_kind));
        assert!(!persisted_field_kind_is_queryable(&composite));
    }

    #[test]
    fn persisted_field_kind_value_acceptance_uses_schema_semantics_for_shape_dispatch() {
        let nat8_kind = AcceptedFieldKind::Nat8;
        let int16_kind = AcceptedFieldKind::Int16;
        let relation_kind = relation_to_key(AcceptedFieldKind::Nat8);
        let list_kind =
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: None }));
        let map_kind = AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat8),
        };
        let composite_list_kind =
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::test_composite()));

        assert!(persisted_field_kind_accepts_value(
            &nat8_kind,
            &Value::Nat64(u64::from(u8::MAX)),
        ));
        assert!(!persisted_field_kind_accepts_value(
            &nat8_kind,
            &Value::Nat64(u64::from(u8::MAX) + 1),
        ));
        assert!(persisted_field_kind_accepts_value(
            &int16_kind,
            &Value::Int64(i64::from(i16::MIN)),
        ));
        assert!(!persisted_field_kind_accepts_value(
            &int16_kind,
            &Value::Int64(i64::from(i16::MIN) - 1),
        ));
        assert!(persisted_field_kind_accepts_value(
            &relation_kind,
            &Value::Nat64(7),
        ));
        assert!(persisted_field_kind_accepts_value(
            &list_kind,
            &Value::List(vec![Value::Text("alpha".into())]),
        ));
        assert!(!persisted_field_kind_accepts_value(
            &list_kind,
            &Value::List(vec![Value::Nat64(7)]),
        ));
        assert!(persisted_field_kind_accepts_value(
            &map_kind,
            &Value::Map(vec![(Value::Text("alpha".into()), Value::Nat64(7))]),
        ));
        assert!(!persisted_field_kind_accepts_value(
            &map_kind,
            &Value::Map(vec![(Value::Text("alpha".into()), Value::Nat64(300))]),
        ));
        assert!(persisted_field_kind_accepts_value(
            &composite_list_kind,
            &Value::List(vec![Value::Map(Vec::new())]),
        ));
        assert!(!persisted_field_kind_accepts_value(
            &composite_list_kind,
            &Value::Map(Vec::new()),
        ));
    }
}
