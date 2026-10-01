//! Module: index::key::build
//! Responsibility: build `IndexId`/`IndexKey` values from entity + index models.
//! Does not own: raw key byte framing (codec) or index-store writes.
//! Boundary: planning/mutation paths call into this constructor layer.

use crate::db::schema::SchemaInfo;
use crate::db::schema::{SchemaExpressionIndexRebuildKey, SchemaExpressionIndexRebuildTarget};
use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::CanonicalSlotReader,
        index::{
            derive_index_expression_value,
            key::ordered::encode_canonical_index_component,
            key::{IndexId, IndexKey, IndexKeyEncodeError, IndexKeyKind, OrderedValueEncodeError},
        },
        key_taxonomy::PrimaryKeyValue,
        schema::{
            AcceptedFieldKind, AcceptedValueAdmissionContract, PersistedIndexExpressionOp,
            SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo,
            SchemaFieldPathIndexRebuildKey, SchemaFieldPathIndexRebuildTarget,
            SchemaIndexFieldPathInfo, SchemaIndexInfo, ValueAdmissionBudget,
            encode_unit_enum_equality_key,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
type AcceptedFieldPathComponentEncoder<'a> = dyn FnMut(&SchemaIndexInfo, &SchemaIndexFieldPathInfo) -> Result<Option<Vec<u8>>, InternalError>
    + 'a;
type AcceptedExpressionComponentEncoder<'a> =
    dyn FnMut(&SchemaExpressionIndexKeyItemInfo) -> Result<Option<Vec<u8>>, InternalError> + 'a;
type FieldPathRebuildComponentEncoder<'a> =
    dyn FnMut(&SchemaFieldPathIndexRebuildKey) -> Result<Option<Vec<u8>>, InternalError> + 'a;
type ExpressionRebuildComponentEncoder<'a> =
    dyn FnMut(&SchemaExpressionIndexRebuildKey) -> Result<Option<Vec<u8>>, InternalError> + 'a;

impl IndexKey {
    /// Build a field-path index key from one canonical slot reader using
    /// accepted index-contract slot authority and scalar-or-composite row
    /// identity.
    pub(crate) fn new_from_slots_with_accepted_field_path_index_primary_key_value(
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        accepted_index: &SchemaIndexInfo,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<Self>, InternalError> {
        build_accepted_field_path_index_key_from_slots(
            entity_tag,
            primary_key,
            accepted_index,
            slots,
        )
    }

    /// Build a field-path rebuild index key from one canonical slot reader
    /// using the accepted mutation target, not generated or runtime planner
    /// metadata.
    pub(crate) fn new_from_slots_with_field_path_rebuild_target(
        entity_tag: EntityTag,
        primary_key: impl Into<PrimaryKeyValue>,
        target: &SchemaFieldPathIndexRebuildTarget,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<Self>, InternalError> {
        let primary_key = primary_key.into();
        build_field_path_rebuild_target_key(entity_tag, &primary_key, target, &mut |field| {
            field_path_rebuild_component_bytes_from_slots(field, slots)
        })
    }

    /// Build an expression-index rebuild key from one canonical slot reader
    /// using the accepted mutation target, not generated or runtime planner
    /// metadata.
    pub(crate) fn new_from_slots_with_expression_rebuild_target(
        entity_tag: EntityTag,
        primary_key: impl Into<PrimaryKeyValue>,
        target: &SchemaExpressionIndexRebuildTarget,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<Self>, InternalError> {
        let primary_key = primary_key.into();
        build_expression_rebuild_target_key(entity_tag, &primary_key, target, &mut |key_item| {
            expression_rebuild_component_bytes_from_slots(key_item, slots)
        })
    }

    /// Build an expression index key from one canonical slot reader using
    /// accepted expression-index contract authority and scalar-or-composite row
    /// identity.
    pub(crate) fn new_from_slots_with_accepted_expression_index_primary_key_value(
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        accepted_index: &SchemaExpressionIndexInfo,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<Self>, InternalError> {
        build_accepted_expression_index_key_from_slots(
            entity_tag,
            primary_key,
            accepted_index,
            slots,
        )
    }

    #[must_use]
    pub(crate) fn empty_with_kind(index_id: &IndexId, key_kind: IndexKeyKind) -> Self {
        Self {
            key_kind,
            index_id: *index_id,
            components: Vec::new(),
            primary_key: Self::wildcard_low_pk(),
        }
    }

    pub(in crate::db) fn new_from_components_with_primary_key_value<C: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        components: &[C],
        primary_key: &PrimaryKeyValue,
    ) -> Result<Self, IndexKeyEncodeError> {
        Ok(Self {
            key_kind,
            index_id: *index_id,
            components: components
                .iter()
                .map(|component| component.as_ref().to_vec())
                .collect(),
            primary_key: Self::compact_primary_key_value_bytes(primary_key)?,
        })
    }

    /// Build an index key from already-lowered prefix components plus a
    /// semantic primary-key suffix.
    pub(in crate::db) fn new_from_existing_prefix_and_suffix_values_with_primary_key_value(
        prefix_start: &Self,
        prefix_len: usize,
        suffix_values: &[Value],
        primary_key: &PrimaryKeyValue,
    ) -> Result<Self, InternalError> {
        let mut components = Vec::with_capacity(prefix_len + suffix_values.len());
        for component_index in 0..prefix_len {
            let component = prefix_start
                .component(component_index)
                .ok_or_else(InternalError::query_executor_invariant)?;
            push_index_key_component(&mut components, component.to_vec())?;
        }
        for value in suffix_values {
            let Some(component) = encode_value_index_component_ref(value)? else {
                return Err(InternalError::query_executor_invariant());
            };
            push_index_key_component(&mut components, component)?;
        }

        Ok(Self {
            key_kind: prefix_start.key_kind,
            index_id: prefix_start.index_id,
            components,
            primary_key: Self::compact_primary_key_value_bytes(primary_key)?,
        })
    }

    #[must_use]
    pub(in crate::db) fn has_same_components(&self, other: &Self) -> bool {
        self.components == other.components
    }
}

fn accepted_field_path_component_bytes_from_slots(
    accepted_index: &SchemaIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source = slots.required_value_by_contract_cow(field.slot())?;
    let Some(source) = resolve_field_path_component(source.as_ref(), field.path())? else {
        return Ok(None);
    };

    encode_accepted_field_path_index_component(accepted_index, field, source)
}

fn encode_accepted_field_path_index_component(
    accepted_index: &SchemaIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !matches!(field.persisted_kind(), Some(AcceptedFieldKind::Enum { .. })) {
        return encode_value_index_component_ref(source);
    }
    let contract = accepted_index
        .accepted_field_contract(field)
        .ok_or_else(InternalError::index_unsupported)?;
    encode_admitted_unit_enum_index_component(&contract, source)
}

fn encode_accepted_expression_field_path_index_component(
    accepted_index: &SchemaExpressionIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !matches!(field.persisted_kind(), Some(AcceptedFieldKind::Enum { .. })) {
        return encode_value_index_component_ref(source);
    }
    let contract = accepted_index
        .accepted_field_contract(field)
        .ok_or_else(InternalError::index_unsupported)?;
    encode_admitted_unit_enum_index_component(&contract, source)
}

/// Encode one admitted literal against the exact accepted index component
/// contract that will encode stored rows for the same index position.
pub(in crate::db) fn encode_accepted_index_literal_component(
    schema_info: &SchemaInfo,
    index_name: &str,
    component_index: usize,
    value: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    if let Some(index) = schema_info
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == index_name)
    {
        let field = index
            .fields()
            .get(component_index)
            .ok_or_else(InternalError::query_executor_invariant)?;
        return encode_accepted_field_path_index_component(index, field, value);
    }

    if let Some(index) = schema_info
        .expression_indexes()
        .iter()
        .find(|index| index.name() == index_name)
    {
        let key_item = index
            .key_items()
            .get(component_index)
            .ok_or_else(InternalError::query_executor_invariant)?;
        return match key_item {
            SchemaExpressionIndexKeyItemInfo::FieldPath(field) => {
                encode_accepted_expression_field_path_index_component(index, field, value)
            }
            SchemaExpressionIndexKeyItemInfo::Expression(_) => {
                encode_value_index_component_ref(value)
            }
        };
    }

    encode_value_index_component_ref(value)
}

fn encode_admitted_unit_enum_index_component(
    contract: &AcceptedValueAdmissionContract<'_>,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    if matches!(source, Value::Null) {
        return Ok(None);
    }
    let mut budget = ValueAdmissionBudget::standard();
    let encoded = contract
        .with_validated(source, &mut budget, |proof| {
            encode_unit_enum_equality_key(&proof)
        })
        .map_err(|_| InternalError::index_unsupported())?
        .map_err(|_| InternalError::index_unsupported())?;

    Ok(Some(encoded.to_vec()))
}

fn accepted_expression_component_bytes_from_slots(
    accepted_index: &SchemaExpressionIndexInfo,
    key_item: &SchemaExpressionIndexKeyItemInfo,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    match key_item {
        SchemaExpressionIndexKeyItemInfo::FieldPath(field) => {
            let source = slots.required_value_by_contract_cow(field.slot())?;
            let Some(source) = resolve_field_path_component(source.as_ref(), field.path())? else {
                return Ok(None);
            };

            encode_accepted_expression_field_path_index_component(accepted_index, field, source)
        }
        SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
            expression_component_bytes_from_slots(
                expression.op(),
                expression.source().slot(),
                expression.source().path(),
                slots,
            )
        }
    }
}

fn field_path_rebuild_component_bytes_from_slots(
    field: &SchemaFieldPathIndexRebuildKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source = slots.required_value_by_contract_cow(usize::from(field.slot().get()))?;
    let Some(source) = resolve_field_path_component(source.as_ref(), field.path())? else {
        return Ok(None);
    };

    encode_value_index_component_ref(source)
}

fn expression_rebuild_component_bytes_from_slots(
    key_item: &SchemaExpressionIndexRebuildKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    match key_item {
        SchemaExpressionIndexRebuildKey::FieldPath(field) => {
            field_path_rebuild_component_bytes_from_slots(field, slots)
        }
        SchemaExpressionIndexRebuildKey::Expression(expression) => {
            expression_component_bytes_from_slots(
                expression.op(),
                usize::from(expression.source().slot().get()),
                expression.source().path(),
                slots,
            )
        }
    }
}

// Rebuild and ordinary maintenance must transform the same accepted leaf,
// including nested source paths and their NULL/non-membership boundary.
fn expression_component_bytes_from_slots(
    op: PersistedIndexExpressionOp,
    source_slot: usize,
    source_path: &[String],
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source = slots.required_value_by_contract_cow(source_slot)?;
    let Some(source) = resolve_field_path_component(source.as_ref(), source_path)? else {
        return Ok(None);
    };
    let Some(value) =
        derive_index_expression_value(op, source).map_err(|_| InternalError::index_invariant())?
    else {
        return Ok(None);
    };

    encode_value_index_component(value)
}

fn resolve_field_path_component<'a>(
    root: &'a Value,
    path: &[String],
) -> Result<Option<&'a Value>, InternalError> {
    let mut current = root;
    for segment in path.iter().skip(1) {
        if matches!(current, Value::Null) {
            return Ok(None);
        }
        let entries = current
            .as_map()
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        let Some((_, value)) = entries
            .iter()
            .find(|(key, _)| matches!(key, Value::Text(text) if text == segment))
        else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

fn build_accepted_field_path_index_key_from_slots(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaIndexInfo,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    build_accepted_field_path_index_key_from_components(
        entity_tag,
        primary_key,
        accepted_index,
        &mut |accepted_index, field| {
            accepted_field_path_component_bytes_from_slots(accepted_index, field, slots)
        },
    )
}

fn build_accepted_expression_index_key_from_slots(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaExpressionIndexInfo,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    build_accepted_expression_index_key_from_components(
        entity_tag,
        primary_key,
        accepted_index,
        &mut |key_item| {
            accepted_expression_component_bytes_from_slots(accepted_index, key_item, slots)
        },
    )
}

fn build_field_path_rebuild_target_key(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    target: &SchemaFieldPathIndexRebuildTarget,
    component_bytes: &mut FieldPathRebuildComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    build_user_index_key_from_components(
        IndexId::new_with_generation(entity_tag, target.ordinal(), target.physical_generation()),
        primary_key,
        target.key_paths(),
        component_bytes,
    )
}

fn build_expression_rebuild_target_key(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    target: &SchemaExpressionIndexRebuildTarget,
    component_bytes: &mut ExpressionRebuildComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    build_user_index_key_from_components(
        IndexId::new_with_generation(entity_tag, target.ordinal(), target.physical_generation()),
        primary_key,
        target.key_items(),
        component_bytes,
    )
}

fn build_accepted_expression_index_key_from_components(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaExpressionIndexInfo,
    component_bytes: &mut AcceptedExpressionComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    build_user_index_key_from_components(
        IndexId::new_with_generation(
            entity_tag,
            accepted_index.ordinal(),
            accepted_index.physical_generation(),
        ),
        primary_key,
        accepted_index.key_items(),
        component_bytes,
    )
}

fn build_accepted_field_path_index_key_from_components(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaIndexInfo,
    component_bytes: &mut AcceptedFieldPathComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    build_user_index_key_from_components(
        IndexId::new_with_generation(
            entity_tag,
            accepted_index.ordinal(),
            accepted_index.physical_generation(),
        ),
        primary_key,
        accepted_index.fields(),
        |field| component_bytes(accepted_index, field),
    )
}

// Assemble accepted and rebuild keys under one byte/diagnostic contract.
// Callers retain their distinct component authority; static dispatch preserves
// their existing encoder calls without boxing or extra component allocation.
fn build_user_index_key_from_components<T>(
    index_id: IndexId,
    primary_key: &PrimaryKeyValue,
    items: &[T],
    mut component_bytes: impl FnMut(&T) -> Result<Option<Vec<u8>>, InternalError>,
) -> Result<Option<IndexKey>, InternalError> {
    let component_count = items.len();
    if component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            index_id.entity_tag().value(),
            index_id.generation(),
            component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(component_count);
    for (component_index, item) in items.iter().enumerate() {
        let Some(component) = component_bytes(item)? else {
            return Ok(None);
        };
        if component.len() > IndexKey::MAX_COMPONENT_SIZE {
            return Err(InternalError::index_component_exceeds_max_size_at(
                index_id.entity_tag().value(),
                index_id.generation(),
                component_index,
                component.len(),
                IndexKey::MAX_COMPONENT_SIZE,
            ));
        }
        components.push(component);
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id,
        components,
        primary_key: IndexKey::compact_primary_key_value_bytes(primary_key)?,
    }))
}

// Push one canonical component after enforcing the shared size contract.
fn push_index_key_component(
    components: &mut Vec<Vec<u8>>,
    component: Vec<u8>,
) -> Result<(), InternalError> {
    if component.len() > IndexKey::MAX_COMPONENT_SIZE {
        return Err(InternalError::index_component_exceeds_max_size());
    }

    components.push(component);
    Ok(())
}

// Encode one owned runtime value into canonical index bytes.
fn encode_value_index_component(value: Value) -> Result<Option<Vec<u8>>, InternalError> {
    encode_value_index_component_ref(&value)
}

// Encode one borrowed runtime value into canonical index bytes without
// forcing callers to clone already-decoded structural slot values first.
fn encode_value_index_component_ref(value: &Value) -> Result<Option<Vec<u8>>, InternalError> {
    let encoded = match encode_canonical_index_component(value) {
        Ok(encoded) => encoded,
        Err(
            OrderedValueEncodeError::NullNotIndexable
            | OrderedValueEncodeError::UnsupportedValueKind,
        ) => {
            return Ok(None);
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Some(encoded))
}

#[cfg(test)]
mod assembly_tests {
    use super::*;
    use crate::db::key_taxonomy::PrimaryKeyComponent;

    #[test]
    fn user_key_limits_preserve_typed_diagnostics() {
        let id = IndexId::new_with_generation(EntityTag::new(23), 2, 5);
        let key = PrimaryKeyComponent::Nat64(7).into();
        let error =
            build_user_index_key_from_components(id, &key, &[(); MAX_INDEX_FIELDS + 1], |()| {
                panic!("field count must be checked before encoding")
            })
            .unwrap_err();
        let expected = InternalError::index_key_field_count_exceeds_max(
            23,
            5,
            MAX_INDEX_FIELDS + 1,
            MAX_INDEX_FIELDS,
        );
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());

        let error = build_user_index_key_from_components(
            id,
            &key,
            &[1, IndexKey::MAX_COMPONENT_SIZE + 1],
            |len| Ok(Some(vec![0; *len])),
        )
        .unwrap_err();
        let expected = InternalError::index_component_exceeds_max_size_at(
            23,
            5,
            1,
            IndexKey::MAX_COMPONENT_SIZE + 1,
            IndexKey::MAX_COMPONENT_SIZE,
        );
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }

    #[test]
    fn user_keys_preserve_identity_and_null_short_circuit() {
        let id = IndexId::new_with_generation(EntityTag::new(23), 2, 5);
        let primary_key = PrimaryKeyComponent::Nat64(7).into();
        let items = [Value::Nat64(11), Value::Text("key".into())];
        let key = build_user_index_key_from_components(
            id,
            &primary_key,
            &items,
            encode_value_index_component_ref,
        )
        .unwrap()
        .unwrap();
        assert_eq!(key.index_id, id);
        assert_eq!(key.key_kind, IndexKeyKind::User);
        assert_eq!(
            key.primary_key,
            IndexKey::compact_primary_key_value_bytes(&primary_key).unwrap()
        );
        assert_eq!(
            key.components,
            items
                .iter()
                .map(|item| encode_canonical_index_component(item).unwrap())
                .collect::<Vec<_>>()
        );

        let mut calls = 0;
        let absent = build_user_index_key_from_components(
            id,
            &primary_key,
            &[Value::Nat64(11), Value::Null, Value::Nat64(12)],
            |value| {
                calls += 1;
                encode_value_index_component_ref(value)
            },
        )
        .unwrap();
        assert!(absent.is_none());
        assert_eq!(calls, 2);
    }
}

#[cfg(test)]
mod accepted_enum_tests {
    use super::*;
    use crate::{
        db::schema::{
            AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract, AcceptedSchemaRevision,
            AcceptedValueCatalogHandle, FieldStorageDecode, LeafCodec, TestEnumDefinition,
            TestEnumVariant, build_accepted_enum_catalog_for_tests,
        },
        value::{ValueEnum, ValueTag},
    };

    fn unit_definition() -> TestEnumDefinition {
        TestEnumDefinition::new("index::Status", vec![TestEnumVariant::unit("Ready")])
    }

    fn payload_definition() -> TestEnumDefinition {
        TestEnumDefinition::new(
            "index::Payload",
            vec![
                TestEnumVariant::unit("Empty"),
                TestEnumVariant::payload(
                    "Value",
                    AcceptedFieldKind::Nat64,
                    FieldStorageDecode::ByKind,
                ),
            ],
        )
    }

    #[test]
    fn accepted_index_leaf_uses_catalog_ids_for_unit_enum_key() {
        let catalog = build_accepted_enum_catalog_for_tests(&[unit_definition()])
            .expect("unit enum catalog should build");
        let type_id = catalog
            .type_id("index::Status")
            .expect("status type should exist");
        let handle = AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let kind = AcceptedFieldKind::Enum { type_id };
        let field = AcceptedFieldDecodeContract::new(
            "status",
            &kind,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        );
        let persistence = AcceptedFieldPersistenceContract::new_for_tests(&handle, field)
            .expect("accepted index field should match the catalog");
        let value = Value::Enum(ValueEnum::test_unit(1, 1));

        assert_eq!(
            encode_admitted_unit_enum_index_component(persistence.admission_contract(), &value)
                .expect("accepted unit enum should produce an index component"),
            Some(vec![ValueTag::Enum.to_u8(), 1, 0, 0, 0, 1, 0, 0, 0, 1, 0]),
        );
    }

    #[test]
    fn accepted_index_leaf_rejects_payload_enum_without_stable_key_capability() {
        let catalog = build_accepted_enum_catalog_for_tests(&[payload_definition()])
            .expect("payload enum catalog should build");
        let type_id = catalog
            .type_id("index::Payload")
            .expect("payload type should exist");
        let handle = AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let kind = AcceptedFieldKind::Enum { type_id };
        let field = AcceptedFieldDecodeContract::new(
            "payload",
            &kind,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        );
        let persistence = AcceptedFieldPersistenceContract::new_for_tests(&handle, field)
            .expect("accepted index field should match the catalog");
        // Capability belongs to the complete definition, not the selected variant.
        for value in [
            Value::Enum(ValueEnum::test_unit(1, 1)),
            Value::Enum(ValueEnum::test_payload(1, 2, Value::Nat64(7))),
        ] {
            assert!(
                encode_admitted_unit_enum_index_component(persistence.admission_contract(), &value)
                    .is_err(),
                "payload enums must remain outside canonical index-key capability",
            );
        }
    }
}
