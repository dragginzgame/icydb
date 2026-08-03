//! Closed catalog-bound source-migration transform compiler and evaluator.
//!
//! Authored field names are consumed only while compiling against the exact
//! predecessor and candidate catalogs. Runtime evaluation retains accepted
//! entity, field, and slot identities and cannot call application code.

use std::borrow::Cow;

use icydb_schema::{
    EntityMigration, FieldType, ScalarLiteral, SchemaMigrationTransform, SchemaProposal,
    TargetStoreIdentity,
};

use crate::{
    db::{
        data::{
            CanonicalRow, CanonicalSlotReader, DecodedDataStoreKey, StructuralRowContract,
            StructuralSlotReader, canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedFieldKind, AcceptedSchemaSnapshot,
            CandidateSchemaRevision, ExistingProposalStore, FieldId,
            PersistedSchemaMigrationTransformReason, SchemaFieldSlot, ValueAdmissionBudget,
            lower_field_type, source_literal_input,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};

/// One row-local transform failure bound to accepted IDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct MigrationTransformFinding {
    source_field: Option<FieldId>,
    target_field: FieldId,
    reason: PersistedSchemaMigrationTransformReason,
}

impl MigrationTransformFinding {
    #[must_use]
    pub(in crate::db::schema) const fn source_field(self) -> Option<FieldId> {
        self.source_field
    }

    #[must_use]
    pub(in crate::db::schema) const fn target_field(self) -> FieldId {
        self.target_field
    }

    #[must_use]
    pub(in crate::db::schema) const fn reason(self) -> PersistedSchemaMigrationTransformReason {
        self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompiledMigrationTransform {
    Fill {
        target: FieldId,
        target_slot: SchemaFieldSlot,
        literal: Value,
    },
    Copy {
        source: FieldId,
        source_slot: SchemaFieldSlot,
        target: FieldId,
        target_slot: SchemaFieldSlot,
    },
    CheckedCast {
        source: FieldId,
        source_slot: SchemaFieldSlot,
        target: FieldId,
        target_slot: SchemaFieldSlot,
        target_kind: AcceptedFieldKind,
    },
    Coalesce {
        source: FieldId,
        source_slot: SchemaFieldSlot,
        target: FieldId,
        target_slot: SchemaFieldSlot,
        literal: Value,
    },
}

impl CompiledMigrationTransform {
    const fn target_slot(&self) -> SchemaFieldSlot {
        match self {
            Self::Fill { target_slot, .. }
            | Self::Copy { target_slot, .. }
            | Self::CheckedCast { target_slot, .. }
            | Self::Coalesce { target_slot, .. } => *target_slot,
        }
    }
}

/// One accepted-ID program for one migrated entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct CompiledMigrationEntityProgram {
    store: TargetStoreIdentity,
    store_path: &'static str,
    entity: EntityTag,
    before_path: String,
    candidate_path: String,
    preserved_slots: Vec<(SchemaFieldSlot, SchemaFieldSlot)>,
    transforms: Vec<CompiledMigrationTransform>,
}

impl CompiledMigrationEntityProgram {
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::schema) const fn transform_count(&self) -> usize {
        self.transforms.len()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> TargetStoreIdentity {
        self.store
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_path(&self) -> &'static str {
        self.store_path
    }

    #[must_use]
    pub(in crate::db::schema) const fn entity(&self) -> EntityTag {
        self.entity
    }

    #[must_use]
    pub(in crate::db::schema) const fn before_path(&self) -> &str {
        self.before_path.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn candidate_path(&self) -> &str {
        self.candidate_path.as_str()
    }

    /// Materialize one transient candidate row without writing accepted data.
    pub(in crate::db::schema) fn evaluate(
        &self,
        before: &StructuralSlotReader<'_>,
        candidate_contract: &StructuralRowContract,
        key: &DecodedDataStoreKey,
    ) -> Result<CanonicalRow, MigrationTransformFinding> {
        let mut values = vec![None; candidate_contract.field_count()];
        for (target, source) in &self.preserved_slots {
            let value = before
                .required_value_by_contract_cow(usize::from(source.get()))
                .map_err(|_| {
                    self.finding(
                        None,
                        *target,
                        PersistedSchemaMigrationTransformReason::ValueContract,
                    )
                })?
                .into_owned();
            set_candidate_value(&mut values, *target, value)
                .map_err(|reason| self.finding(None, *target, reason))?;
        }
        for transform in &self.transforms {
            Self::evaluate_transform(before, &mut values, transform)?;
        }
        let row = canonical_row_from_runtime_value_source_with_accepted_contract(
            candidate_contract,
            |slot| {
                values
                    .get(slot)
                    .and_then(Option::as_ref)
                    .map(Cow::Borrowed)
                    .ok_or_else(InternalError::store_invariant)
            },
        )
        .map_err(|_| {
            self.finding(
                None,
                SchemaFieldSlot::new(0),
                PersistedSchemaMigrationTransformReason::ValueContract,
            )
        })?;
        let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            row.as_raw_row(),
            candidate_contract,
        )
        .map_err(|_| {
            self.finding(
                None,
                SchemaFieldSlot::new(0),
                PersistedSchemaMigrationTransformReason::ValueContract,
            )
        })?;
        reader.validate_primary_key(key).map_err(|_| {
            self.finding(
                None,
                SchemaFieldSlot::new(0),
                PersistedSchemaMigrationTransformReason::ValueContract,
            )
        })?;
        Ok(row)
    }

    fn evaluate_transform(
        before: &StructuralSlotReader<'_>,
        values: &mut [Option<Value>],
        transform: &CompiledMigrationTransform,
    ) -> Result<(), MigrationTransformFinding> {
        match transform {
            CompiledMigrationTransform::Fill {
                target,
                target_slot,
                literal,
            } => set_candidate_value(values, *target_slot, literal.clone()).map_err(|reason| {
                MigrationTransformFinding {
                    source_field: None,
                    target_field: *target,
                    reason,
                }
            }),
            CompiledMigrationTransform::Copy {
                source,
                source_slot,
                target,
                target_slot,
            } => {
                let value = before
                    .required_value_by_contract_cow(usize::from(source_slot.get()))
                    .map_err(|_| MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason: PersistedSchemaMigrationTransformReason::ValueContract,
                    })?
                    .into_owned();
                set_candidate_value(values, *target_slot, value).map_err(|reason| {
                    MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason,
                    }
                })
            }
            CompiledMigrationTransform::CheckedCast {
                source,
                source_slot,
                target,
                target_slot,
                target_kind,
            } => {
                let value = before
                    .required_value_by_contract_cow(usize::from(source_slot.get()))
                    .map_err(|_| MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason: PersistedSchemaMigrationTransformReason::ValueContract,
                    })?;
                let cast = checked_cast_value(value.as_ref(), target_kind).map_err(|reason| {
                    MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason,
                    }
                })?;
                set_candidate_value(values, *target_slot, cast).map_err(|reason| {
                    MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason,
                    }
                })
            }
            CompiledMigrationTransform::Coalesce {
                source,
                source_slot,
                target,
                target_slot,
                literal,
            } => {
                let source_value = before
                    .required_value_by_contract_cow(usize::from(source_slot.get()))
                    .map_err(|_| MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason: PersistedSchemaMigrationTransformReason::ValueContract,
                    })?;
                let value = if matches!(source_value.as_ref(), Value::Null) {
                    literal.clone()
                } else {
                    source_value.into_owned()
                };
                set_candidate_value(values, *target_slot, value).map_err(|reason| {
                    MigrationTransformFinding {
                        source_field: Some(*source),
                        target_field: *target,
                        reason,
                    }
                })
            }
        }
    }

    fn finding(
        &self,
        source_field: Option<FieldId>,
        target_slot: SchemaFieldSlot,
        reason: PersistedSchemaMigrationTransformReason,
    ) -> MigrationTransformFinding {
        let mut index = 0;
        while index < self.transforms.len() {
            let transform = &self.transforms[index];
            if transform.target_slot().get() == target_slot.get() {
                let target_field = match transform {
                    CompiledMigrationTransform::Fill { target, .. }
                    | CompiledMigrationTransform::Copy { target, .. }
                    | CompiledMigrationTransform::CheckedCast { target, .. }
                    | CompiledMigrationTransform::Coalesce { target, .. } => *target,
                };
                return MigrationTransformFinding {
                    source_field,
                    target_field,
                    reason,
                };
            }
            index += 1;
        }
        MigrationTransformFinding {
            source_field,
            target_field: FieldId::new(0),
            reason,
        }
    }
}

/// Compile every physical transition against predecessor and exact candidate
/// catalogs. Metadata-only transitions deliberately produce no program.
pub(in crate::db::schema) fn compile_migration_programs(
    proposal: &SchemaProposal,
    stores: &[ExistingProposalStore<'_>],
    candidates: &[CandidateSchemaRevision],
) -> Result<Vec<CompiledMigrationEntityProgram>, InternalError> {
    let plan = proposal
        .migration()
        .ok_or_else(InternalError::store_invariant)?;
    let mut programs = Vec::new();
    for transition in plan.transitions() {
        if transition.transforms().is_empty() {
            continue;
        }
        let predecessor = transition
            .from_name()
            .unwrap_or_else(|| transition.entity());
        let (store, entity) = resolve_predecessor(stores, predecessor)?;
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.store_path() == store.path)
            .ok_or_else(InternalError::store_invariant)?;
        programs.push(compile_entity_program(
            store, candidate, entity, transition,
        )?);
    }
    programs.sort_unstable_by_key(|program| (program.store, program.entity));
    Ok(programs)
}

fn resolve_predecessor<'a>(
    stores: &'a [ExistingProposalStore<'_>],
    source: &icydb_schema::EntitySourceKey,
) -> Result<(&'a ExistingProposalStore<'a>, EntityTag), InternalError> {
    let mut matching = stores.iter().filter_map(|store| {
        store
            .bundle
            .source_bindings()
            .entity(source)
            .map(|entity| (store, entity))
    });
    let first = matching.next().ok_or_else(InternalError::store_invariant)?;
    if matching.next().is_some() {
        return Err(InternalError::store_invariant());
    }
    Ok(first)
}

fn compile_entity_program(
    store: &ExistingProposalStore<'_>,
    candidate: &CandidateSchemaRevision,
    entity: EntityTag,
    transition: &EntityMigration,
) -> Result<CompiledMigrationEntityProgram, InternalError> {
    let before_snapshot = store
        .bundle
        .entity_snapshots()
        .get(&entity)
        .ok_or_else(InternalError::store_invariant)?;
    let candidate_snapshot = candidate
        .bundle()
        .entity_snapshots()
        .get(&entity)
        .ok_or_else(InternalError::store_invariant)?;
    let selection = AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        entity,
        candidate_snapshot.entity_path(),
        store.path,
    )?
    .ok_or_else(InternalError::store_invariant)?;
    let accepted_candidate = selection.decode_verified()?;
    let candidate_contract =
        crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
            candidate_snapshot.entity_path(),
            &selection,
        )?
        .into_row_contract();
    let mut transforms = Vec::with_capacity(transition.transforms().len());
    for transform in transition.transforms() {
        transforms.push(compile_transform(
            store,
            candidate,
            entity,
            before_snapshot,
            &accepted_candidate,
            &candidate_contract,
            transform,
        )?);
    }
    let target_slots = transforms
        .iter()
        .map(CompiledMigrationTransform::target_slot)
        .collect::<Vec<_>>();
    let mut preserved_slots = Vec::new();
    for target in candidate_snapshot.fields() {
        if target_slots.contains(&target.slot()) {
            continue;
        }
        let source = before_snapshot
            .fields()
            .iter()
            .find(|source| source.id() == target.id())
            .ok_or_else(InternalError::store_invariant)?;
        preserved_slots.push((target.slot(), source.slot()));
    }
    preserved_slots.sort_unstable();
    Ok(CompiledMigrationEntityProgram {
        store: store.identity,
        store_path: store.path,
        entity,
        before_path: before_snapshot.entity_path().to_string(),
        candidate_path: candidate_snapshot.entity_path().to_string(),
        preserved_slots,
        transforms,
    })
}

fn compile_transform(
    store: &ExistingProposalStore<'_>,
    candidate: &CandidateSchemaRevision,
    entity: EntityTag,
    before_snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    candidate_snapshot: &AcceptedSchemaSnapshot,
    candidate_contract: &StructuralRowContract,
    transform: &SchemaMigrationTransform,
) -> Result<CompiledMigrationTransform, InternalError> {
    let target_id = candidate
        .bundle()
        .source_bindings()
        .field(entity, transform.target())
        .ok_or_else(InternalError::store_invariant)?;
    let target = candidate_snapshot
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.id() == target_id)
        .ok_or_else(InternalError::store_invariant)?;
    if !target.generated()
        || candidate_snapshot
            .persisted_snapshot()
            .primary_key_field_ids()
            .contains(&target_id)
    {
        return Err(InternalError::store_unsupported());
    }
    let literal =
        |literal: &ScalarLiteral| compile_literal(candidate, candidate_contract, target, literal);
    match transform {
        SchemaMigrationTransform::Fill { literal: value, .. } => {
            Ok(CompiledMigrationTransform::Fill {
                target: target_id,
                target_slot: target.slot(),
                literal: literal(value)?,
            })
        }
        SchemaMigrationTransform::Copy { from, .. } => {
            let (source_id, source) = source_field(store, entity, before_snapshot, from)?;
            if !same_value_contract(source, target) {
                return Err(InternalError::store_unsupported());
            }
            Ok(CompiledMigrationTransform::Copy {
                source: source_id,
                source_slot: source.slot(),
                target: target_id,
                target_slot: target.slot(),
            })
        }
        SchemaMigrationTransform::CheckedCast {
            from,
            target: scalar,
            ..
        } => {
            let (source_id, source) = source_field(store, entity, before_snapshot, from)?;
            let declared_target = lower_field_type(&FieldType::Scalar(*scalar), |_| None)?;
            if declared_target != *target.kind()
                || !supported_cast_source(source.kind())
                || !supported_cast_target(target.kind())
            {
                return Err(InternalError::store_unsupported());
            }
            Ok(CompiledMigrationTransform::CheckedCast {
                source: source_id,
                source_slot: source.slot(),
                target: target_id,
                target_slot: target.slot(),
                target_kind: target.kind().clone(),
            })
        }
        SchemaMigrationTransform::Coalesce {
            from,
            literal: value,
            ..
        } => {
            let (source_id, source) = source_field(store, entity, before_snapshot, from)?;
            if !source.nullable()
                || target.nullable()
                || !compatible_non_null_contract(source, target)
            {
                return Err(InternalError::store_unsupported());
            }
            Ok(CompiledMigrationTransform::Coalesce {
                source: source_id,
                source_slot: source.slot(),
                target: target_id,
                target_slot: target.slot(),
                literal: literal(value)?,
            })
        }
    }
}

fn source_field<'a>(
    store: &ExistingProposalStore<'_>,
    entity: EntityTag,
    snapshot: &'a crate::db::schema::PersistedSchemaSnapshot,
    source: &icydb_schema::FieldSourceKey,
) -> Result<(FieldId, &'a crate::db::schema::PersistedFieldSnapshot), InternalError> {
    let id = store
        .bundle
        .source_bindings()
        .field(entity, source)
        .ok_or_else(InternalError::store_invariant)?;
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.id() == id)
        .ok_or_else(InternalError::store_invariant)?;
    if !field.generated() || snapshot.primary_key_field_ids().contains(&id) {
        return Err(InternalError::store_unsupported());
    }
    Ok((id, field))
}

fn compile_literal(
    candidate: &CandidateSchemaRevision,
    contract: &StructuralRowContract,
    target: &crate::db::schema::PersistedFieldSnapshot,
    literal: &ScalarLiteral,
) -> Result<Value, InternalError> {
    let input = source_literal_input(
        literal,
        target.kind(),
        candidate.bundle().source_bindings(),
        candidate.bundle().enum_catalog(),
    )
    .map_err(|_| InternalError::store_unsupported())?;
    let field =
        contract.required_accepted_field_persistence_contract(usize::from(target.slot().get()))?;
    field
        .admission_contract()
        .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
        .map_err(|_| InternalError::store_unsupported())
}

fn same_value_contract(
    source: &crate::db::schema::PersistedFieldSnapshot,
    target: &crate::db::schema::PersistedFieldSnapshot,
) -> bool {
    source.kind() == target.kind()
        && source.nullable() == target.nullable()
        && source.storage_decode() == target.storage_decode()
        && source.leaf_codec() == target.leaf_codec()
}

fn compatible_non_null_contract(
    source: &crate::db::schema::PersistedFieldSnapshot,
    target: &crate::db::schema::PersistedFieldSnapshot,
) -> bool {
    source.kind() == target.kind()
        && source.storage_decode() == target.storage_decode()
        && source.leaf_codec() == target.leaf_codec()
}

fn set_candidate_value(
    values: &mut [Option<Value>],
    slot: SchemaFieldSlot,
    value: Value,
) -> Result<(), PersistedSchemaMigrationTransformReason> {
    let target = values
        .get_mut(usize::from(slot.get()))
        .ok_or(PersistedSchemaMigrationTransformReason::ValueContract)?;
    if target.replace(value).is_some() {
        return Err(PersistedSchemaMigrationTransformReason::ValueContract);
    }
    Ok(())
}

const fn supported_cast_source(kind: &AcceptedFieldKind) -> bool {
    matches!(
        kind,
        AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Int128
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Nat128
            | AcceptedFieldKind::Decimal { .. }
    )
}

const fn supported_cast_target(kind: &AcceptedFieldKind) -> bool {
    supported_cast_source(kind)
}

fn checked_cast_value(
    source: &Value,
    target: &AcceptedFieldKind,
) -> Result<Value, PersistedSchemaMigrationTransformReason> {
    if matches!(source, Value::Null) {
        return Err(PersistedSchemaMigrationTransformReason::NullSource);
    }
    if let AcceptedFieldKind::Decimal { scale } = target {
        return checked_cast_to_decimal(source, *scale).map(Value::Decimal);
    }
    if matches!(source, Value::Decimal(_)) {
        return Err(PersistedSchemaMigrationTransformReason::PrecisionLoss);
    }
    match target {
        AcceptedFieldKind::Int8 => signed_value(source, i8::MIN.into(), i8::MAX.into())
            .and_then(|value| {
                i64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Int64),
        AcceptedFieldKind::Int16 => signed_value(source, i16::MIN.into(), i16::MAX.into())
            .and_then(|value| {
                i64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Int64),
        AcceptedFieldKind::Int32 => signed_value(source, i32::MIN.into(), i32::MAX.into())
            .and_then(|value| {
                i64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Int64),
        AcceptedFieldKind::Int64 => signed_value(source, i64::MIN.into(), i64::MAX.into())
            .and_then(|value| {
                i64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Int64),
        AcceptedFieldKind::Int128 => signed_value(source, i128::MIN, i128::MAX).map(Value::Int128),
        AcceptedFieldKind::Nat8 => unsigned_value(source, u8::MAX.into())
            .and_then(|value| {
                u64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Nat64),
        AcceptedFieldKind::Nat16 => unsigned_value(source, u16::MAX.into())
            .and_then(|value| {
                u64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Nat64),
        AcceptedFieldKind::Nat32 => unsigned_value(source, u32::MAX.into())
            .and_then(|value| {
                u64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Nat64),
        AcceptedFieldKind::Nat64 => unsigned_value(source, u64::MAX.into())
            .and_then(|value| {
                u64::try_from(value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)
            })
            .map(Value::Nat64),
        AcceptedFieldKind::Nat128 => unsigned_value(source, u128::MAX).map(Value::Nat128),
        _ => Err(PersistedSchemaMigrationTransformReason::ValueContract),
    }
}

fn signed_value(
    source: &Value,
    minimum: i128,
    maximum: i128,
) -> Result<i128, PersistedSchemaMigrationTransformReason> {
    let value = match source {
        Value::Int64(value) => i128::from(*value),
        Value::Int128(value) => *value,
        Value::Nat64(value) => i128::from(*value),
        Value::Nat128(value) => {
            i128::try_from(*value).map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)?
        }
        _ => return Err(PersistedSchemaMigrationTransformReason::ValueContract),
    };
    if value < minimum || value > maximum {
        return Err(PersistedSchemaMigrationTransformReason::Overflow);
    }
    Ok(value)
}

fn unsigned_value(
    source: &Value,
    maximum: u128,
) -> Result<u128, PersistedSchemaMigrationTransformReason> {
    let value = match source {
        Value::Int64(value) => u128::try_from(*value)
            .map_err(|_| PersistedSchemaMigrationTransformReason::NegativeToUnsigned)?,
        Value::Int128(value) => u128::try_from(*value)
            .map_err(|_| PersistedSchemaMigrationTransformReason::NegativeToUnsigned)?,
        Value::Nat64(value) => u128::from(*value),
        Value::Nat128(value) => *value,
        _ => return Err(PersistedSchemaMigrationTransformReason::ValueContract),
    };
    if value > maximum {
        return Err(PersistedSchemaMigrationTransformReason::Overflow);
    }
    Ok(value)
}

fn checked_cast_to_decimal(
    source: &Value,
    target_scale: u32,
) -> Result<icydb_schema::Decimal, PersistedSchemaMigrationTransformReason> {
    let mantissa = match source {
        Value::Int64(value) => scale_integer(i128::from(*value), target_scale)?,
        Value::Int128(value) => scale_integer(*value, target_scale)?,
        Value::Nat64(value) => scale_integer(i128::from(*value), target_scale)?,
        Value::Nat128(value) => scale_integer(
            i128::try_from(*value)
                .map_err(|_| PersistedSchemaMigrationTransformReason::Overflow)?,
            target_scale,
        )?,
        Value::Decimal(value) => rescale_decimal(*value, target_scale)?,
        _ => return Err(PersistedSchemaMigrationTransformReason::ValueContract),
    };
    icydb_schema::Decimal::try_from_i128_with_scale(mantissa, target_scale)
        .ok_or(PersistedSchemaMigrationTransformReason::Overflow)
}

fn scale_integer(value: i128, scale: u32) -> Result<i128, PersistedSchemaMigrationTransformReason> {
    value
        .checked_mul(
            10_i128
                .checked_pow(scale)
                .ok_or(PersistedSchemaMigrationTransformReason::Overflow)?,
        )
        .ok_or(PersistedSchemaMigrationTransformReason::Overflow)
}

fn rescale_decimal(
    value: icydb_schema::Decimal,
    target_scale: u32,
) -> Result<i128, PersistedSchemaMigrationTransformReason> {
    if value.scale() <= target_scale {
        return value
            .scale_to_integer(target_scale)
            .ok_or(PersistedSchemaMigrationTransformReason::Overflow);
    }
    let divisor = 10_i128
        .checked_pow(value.scale() - target_scale)
        .ok_or(PersistedSchemaMigrationTransformReason::Overflow)?;
    if value.mantissa() % divisor != 0 {
        return Err(PersistedSchemaMigrationTransformReason::PrecisionLoss);
    }
    Ok(value.mantissa() / divisor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_cast_matrix_is_exact_for_sign_overflow_precision_and_null() {
        assert_eq!(
            checked_cast_value(&Value::Int64(-1), &AcceptedFieldKind::Nat8),
            Err(PersistedSchemaMigrationTransformReason::NegativeToUnsigned),
        );
        assert_eq!(
            checked_cast_value(&Value::Nat64(256), &AcceptedFieldKind::Nat8),
            Err(PersistedSchemaMigrationTransformReason::Overflow),
        );
        assert_eq!(
            checked_cast_value(
                &Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(15, 1)),
                &AcceptedFieldKind::Decimal { scale: 0 },
            ),
            Err(PersistedSchemaMigrationTransformReason::PrecisionLoss),
        );
        assert_eq!(
            checked_cast_value(&Value::Null, &AcceptedFieldKind::Int64),
            Err(PersistedSchemaMigrationTransformReason::NullSource),
        );
        assert_eq!(
            checked_cast_value(&Value::Int64(7), &AcceptedFieldKind::Int128),
            Ok(Value::Int128(7)),
        );
        assert_eq!(
            checked_cast_value(
                &Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(150, 2)),
                &AcceptedFieldKind::Decimal { scale: 1 },
            ),
            Ok(Value::Decimal(icydb_schema::Decimal::from_i128_with_scale(
                15, 1
            ))),
        );
    }

    #[test]
    fn unsupported_cast_domains_remain_closed() {
        assert!(!supported_cast_source(&AcceptedFieldKind::Timestamp));
        assert!(!supported_cast_target(&AcceptedFieldKind::Float64));
        assert_eq!(
            checked_cast_value(&Value::Text("7".to_string()), &AcceptedFieldKind::Int64),
            Err(PersistedSchemaMigrationTransformReason::ValueContract),
        );
    }
}
