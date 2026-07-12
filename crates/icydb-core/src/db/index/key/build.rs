//! Module: index::key::build
//! Responsibility: build `IndexId`/`IndexKey` values from entity + index models.
//! Does not own: raw key byte framing (codec) or index-store writes.
//! Boundary: planning/mutation paths call into this constructor layer.

#[cfg(any(test, feature = "sql"))]
use crate::db::schema::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
    SchemaExpressionIndexRebuildTarget,
};
#[cfg(test)]
use crate::model::entity::EntityModel;
#[cfg(test)]
use crate::model::index::IndexModel;
use crate::{
    MAX_INDEX_FIELDS,
    db::{
        access::{
            SemanticIndexAccessContract, SemanticIndexExpression, SemanticIndexKeyItemRef,
            SemanticIndexKeyItemsRef,
        },
        data::CanonicalSlotReader,
        index::{
            IndexExpressionSourceClass, derive_index_expression_value,
            key::ordered::encode_canonical_index_component,
            key::{IndexId, IndexKey, IndexKeyEncodeError, IndexKeyKind, OrderedValueEncodeError},
        },
        key_taxonomy::PrimaryKeyValue,
        scalar_expr::{
            ScalarExprValue, ScalarIndexExpressionOp, derive_non_null_scalar_expression_value,
            scalar_expr_value_into_value,
        },
        schema::{
            AcceptedFieldKind, AcceptedValueContract, PersistedIndexExpressionOp,
            SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo,
            SchemaFieldPathIndexRebuildKey, SchemaFieldPathIndexRebuildTarget,
            SchemaIndexFieldPathInfo, SchemaIndexInfo, SchemaInfo, ValueAdmissionBudget,
            encode_unit_enum_equality_key, validate_canonical_value,
        },
    },
    error::InternalError,
    model::{
        field::FieldStorageDecode,
        index::{IndexExpression, IndexKeyItem, IndexKeyItemsRef},
    },
    types::EntityTag,
    value::Value,
};
#[cfg(test)]
use std::ops::Bound;

type AcceptedFieldPathComponentEncoder<'a> = dyn FnMut(&SchemaIndexInfo, &SchemaIndexFieldPathInfo) -> Result<Option<Vec<u8>>, InternalError>
    + 'a;
type AcceptedExpressionComponentEncoder<'a> =
    dyn FnMut(&SchemaExpressionIndexKeyItemInfo) -> Result<Option<Vec<u8>>, InternalError> + 'a;
type FieldPathRebuildComponentEncoder<'a> =
    dyn FnMut(&SchemaFieldPathIndexRebuildKey) -> Result<Option<Vec<u8>>, InternalError> + 'a;
#[cfg(any(test, feature = "sql"))]
type ExpressionRebuildComponentEncoder<'a> =
    dyn FnMut(&SchemaExpressionIndexRebuildKey) -> Result<Option<Vec<u8>>, InternalError> + 'a;

#[cfg(test)]
fn value_for_expression(
    index: &IndexModel,
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, InternalError> {
    value_for_expression_with_index_name(index.name(), expression, source)
}

fn value_for_expression_with_index_name(
    index_name: &str,
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, InternalError> {
    let source_label = source.canonical_tag().label();
    derive_index_expression_value(expression, source).map_err(|expected| {
        InternalError::index_expression_source_type_mismatch(
            index_name,
            expression,
            expected,
            source_label,
        )
    })
}

fn value_for_accepted_expression_with_index_name(
    index_name: &str,
    expression: &SemanticIndexExpression,
    source: Value,
) -> Result<Option<Value>, InternalError> {
    let source_label = source.canonical_tag().label();
    derive_accepted_index_expression_value(expression.op(), source).map_err(|expected| {
        InternalError::index_expression_source_type_mismatch(
            index_name,
            expression.canonical_order_text(),
            expected,
            source_label,
        )
    })
}

fn derive_accepted_index_expression_value(
    op: PersistedIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            derive_accepted_text_expression_value(accepted_expression_op(op), source)
        }
        PersistedIndexExpressionOp::Date
        | PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            derive_accepted_temporal_expression_value(accepted_expression_op(op), source)
        }
    }
}

fn derive_accepted_text_expression_value(
    op: ScalarIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let source = match source {
        Value::Null => return Ok(None),
        Value::Text(value) => ScalarExprValue::Text(value.into()),
        _ => return Err(IndexExpressionSourceClass::Text),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::Text)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

fn derive_accepted_temporal_expression_value(
    op: ScalarIndexExpressionOp,
    source: Value,
) -> Result<Option<Value>, IndexExpressionSourceClass> {
    let source = match source {
        Value::Null => return Ok(None),
        Value::Date(value) => ScalarExprValue::Date(value),
        Value::Timestamp(value) => ScalarExprValue::Timestamp(value),
        _ => return Err(IndexExpressionSourceClass::DateOrTimestamp),
    };

    derive_non_null_scalar_expression_value(op, source)
        .map_err(|_| IndexExpressionSourceClass::DateOrTimestamp)
        .map(scalar_expr_value_into_value)
        .map(Some)
}

const fn accepted_expression_op(op: PersistedIndexExpressionOp) -> ScalarIndexExpressionOp {
    match op {
        PersistedIndexExpressionOp::Lower => ScalarIndexExpressionOp::Lower,
        PersistedIndexExpressionOp::Upper => ScalarIndexExpressionOp::Upper,
        PersistedIndexExpressionOp::Trim => ScalarIndexExpressionOp::Trim,
        PersistedIndexExpressionOp::LowerTrim => ScalarIndexExpressionOp::LowerTrim,
        PersistedIndexExpressionOp::Date => ScalarIndexExpressionOp::Date,
        PersistedIndexExpressionOp::Year => ScalarIndexExpressionOp::Year,
        PersistedIndexExpressionOp::Month => ScalarIndexExpressionOp::Month,
        PersistedIndexExpressionOp::Day => ScalarIndexExpressionOp::Day,
    }
}

fn index_component_bytes_from_slot_ref_reader_with_access_contract<'a>(
    schema_info: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    key_item: SemanticIndexKeyItemRef<'_>,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Option<Vec<u8>>, InternalError> {
    let field = key_item.field();
    let Some(field_index) = schema_info.field_slot_index(field) else {
        return Err(InternalError::index_key_item_field_missing_on_entity_model(
            field,
        ));
    };

    let Some(source) = read_slot(field_index) else {
        return Err(InternalError::index_key_item_field_missing_on_lookup_row(
            field,
        ));
    };

    match key_item {
        SemanticIndexKeyItemRef::Field(field) => {
            encode_schema_field_index_component(schema_info, field, source)
        }
        SemanticIndexKeyItemRef::Expression(expression) => {
            value_for_expression_with_index_name(index.name(), expression, source.clone())?
                .map_or(Ok(None), encode_value_index_component)
        }
        SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
            value_for_accepted_expression_with_index_name(index.name(), expression, source.clone())?
                .map_or(Ok(None), encode_value_index_component)
        }
    }
}

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
    #[cfg(any(test, feature = "sql"))]
    pub(crate) fn new_from_slots_with_expression_rebuild_target(
        entity_tag: EntityTag,
        primary_key: impl Into<PrimaryKeyValue>,
        target: &SchemaExpressionIndexRebuildTarget,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<Self>, InternalError> {
        let primary_key = primary_key.into();
        build_expression_rebuild_target_key(entity_tag, &primary_key, target, &mut |key_item| {
            expression_rebuild_component_bytes_from_slots(target.name(), key_item, slots)
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

    /// Build an index key from one structural row slot reader plus runtime identity.
    /// Returns `Ok(None)` when indexed values are non-indexable.
    #[cfg(test)]
    pub(crate) fn new_from_slot_reader_with_primary_key_value(
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        entity_model: &EntityModel,
        index: &IndexModel,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> Result<Option<Self>, InternalError> {
        let mut component_bytes = |key_item: IndexKeyItem| {
            let field = key_item.field();
            let Some(field_index) = entity_model.resolve_field_slot(field) else {
                return Err(InternalError::index_key_item_field_missing_on_entity_model(
                    field,
                ));
            };

            let Some(source) = read_slot(field_index) else {
                return Err(InternalError::index_key_item_field_missing_on_lookup_row(
                    field,
                ));
            };

            let Some(value) = match key_item {
                IndexKeyItem::Field(_) => Ok(Some(source)),
                IndexKeyItem::Expression(expression) => {
                    value_for_expression(index, expression, source)
                }
            }?
            else {
                return Ok(None);
            };

            encode_value_index_component(value)
        };

        build_generated_model_index_key(entity_tag, primary_key, index, &mut component_bytes)
    }

    /// Build an index key from one structural row slot ref reader using the
    /// reduced selected access contract.
    pub(crate) fn new_from_slot_ref_reader_with_access_contract<'a>(
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        schema_info: &SchemaInfo,
        index: SemanticIndexAccessContract,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<Self>, InternalError> {
        build_index_key_from_access_contract(
            entity_tag,
            primary_key,
            schema_info,
            &index,
            read_slot,
        )
    }

    /// Build a field-path index key from one structural row slot ref reader
    /// using accepted index-contract slot authority.
    pub(crate) fn new_from_slot_ref_reader_with_accepted_field_path_index<'a>(
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        accepted_index: &SchemaIndexInfo,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<Self>, InternalError> {
        build_accepted_field_path_index_key(entity_tag, primary_key, accepted_index, read_slot)
    }

    /// Build an index key from a typed entity for test-only parity checks.
    /// `Value::Null` and unsupported canonical kinds are treated as non-indexable.
    #[cfg(test)]
    pub(crate) fn new<E: crate::traits::EntityKind + crate::traits::EntityValue>(
        entity: &E,
        index: &IndexModel,
    ) -> Result<Option<Self>, InternalError> {
        let entity_key = entity.id().key();
        let primary_key_value = crate::traits::PrimaryKeyCodec::to_primary_key_value(&entity_key)?;
        let mut read_slot = |slot| entity.get_value_by_index(slot);

        Self::new_from_slot_reader_with_primary_key_value(
            E::ENTITY_TAG,
            &primary_key_value,
            E::MODEL,
            index,
            &mut read_slot,
        )
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn empty(index_id: &IndexId) -> Self {
        Self::empty_with_kind(index_id, IndexKeyKind::User)
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db::index) fn bounds_for_prefix<C: AsRef<[u8]>>(
        index_id: &IndexId,
        index_len: usize,
        prefix: &[C],
    ) -> (Self, Self) {
        Self::bounds_for_prefix_with_kind(index_id, IndexKeyKind::User, index_len, prefix)
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::index) fn bounds_for_prefix_with_kind<C: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
    ) -> (Self, Self) {
        // Invalid inputs fail closed to an empty envelope sentinel.
        if index_len > MAX_INDEX_FIELDS || prefix.len() > index_len {
            debug_assert!(
                false,
                "invalid prefix bounds input: index_len={} prefix_len={} (max={})",
                index_len,
                prefix.len(),
                MAX_INDEX_FIELDS
            );
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (empty.clone(), empty);
        }

        let mut start_components = Vec::with_capacity(index_len);
        let mut end_components = Vec::with_capacity(index_len);

        for i in 0..index_len {
            if let Some(component) = prefix.get(i) {
                let component = component.as_ref();
                if component.is_empty() || component.len() > Self::MAX_COMPONENT_SIZE {
                    debug_assert!(
                        false,
                        "invalid prefix component size: len={} (max={})",
                        component.len(),
                        Self::MAX_COMPONENT_SIZE
                    );
                    let empty = Self::empty_with_kind(index_id, key_kind);
                    return (empty.clone(), empty);
                }

                start_components.push(component.to_vec());
                end_components.push(component.to_vec());
                continue;
            }

            start_components.push(Self::wildcard_low_component());
            end_components.push(Self::wildcard_high_component());
        }

        (
            Self {
                key_kind,
                index_id: *index_id,
                components: start_components,
                primary_key: Self::wildcard_low_pk(),
            },
            Self {
                key_kind,
                index_id: *index_id,
                components: end_components,
                primary_key: Self::wildcard_high_pk(),
            },
        )
    }

    #[must_use]
    pub(in crate::db) fn has_same_components(&self, other: &Self) -> bool {
        self.components == other.components
    }

    /// Build lexicographic key-space bounds for one ranged index component after an equality prefix.
    ///
    /// Shape:
    /// - `prefix` constrains components `0..prefix.len()`
    /// - bounds constrain component `prefix.len()`
    /// - remaining suffix components and PK are set to canonical min/max sentinels
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::index) fn bounds_for_prefix_component_range<
        C: AsRef<[u8]>,
        B: AsRef<[u8]>,
    >(
        index_id: &IndexId,
        index_len: usize,
        prefix: &[C],
        lower: &Bound<B>,
        upper: &Bound<B>,
    ) -> (Bound<Self>, Bound<Self>) {
        Self::bounds_for_prefix_component_range_with_kind(
            index_id,
            IndexKeyKind::User,
            index_len,
            prefix,
            lower,
            upper,
        )
    }

    /// Variant of `bounds_for_prefix_component_range` with explicit key kind.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::index) fn bounds_for_prefix_component_range_with_kind<
        C: AsRef<[u8]>,
        B: AsRef<[u8]>,
    >(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
        lower: &Bound<B>,
        upper: &Bound<B>,
    ) -> (Bound<Self>, Bound<Self>) {
        // Validate shape before bound materialization; fail closed in debug-invalid paths.
        if index_len == 0 || index_len > MAX_INDEX_FIELDS || prefix.len() >= index_len {
            debug_assert!(
                false,
                "invalid component-range bounds shape: index_len={} prefix_len={} (max={})",
                index_len,
                prefix.len(),
                MAX_INDEX_FIELDS
            );
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (Bound::Included(empty.clone()), Bound::Included(empty));
        }
        if prefix.iter().any(|component| {
            let component = component.as_ref();
            component.is_empty() || component.len() > Self::MAX_COMPONENT_SIZE
        }) {
            debug_assert!(
                false,
                "invalid component-range prefix component size encountered"
            );
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (Bound::Included(empty.clone()), Bound::Included(empty));
        }

        let Some(lower_component) = normalize_range_component_bound(lower, false) else {
            debug_assert!(
                false,
                "invalid lower component bound payload for encoded range"
            );
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (Bound::Included(empty.clone()), Bound::Included(empty));
        };
        let Some(upper_component) = normalize_range_component_bound(upper, true) else {
            debug_assert!(
                false,
                "invalid upper component bound payload for encoded range"
            );
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (Bound::Included(empty.clone()), Bound::Included(empty));
        };

        let mut start_components = Vec::with_capacity(index_len);
        let mut end_components = Vec::with_capacity(index_len);
        let lower_exclusive = matches!(lower, Bound::Excluded(_));
        let upper_exclusive = matches!(upper, Bound::Excluded(_));

        for i in 0..index_len {
            if let Some(component) = prefix.get(i) {
                let component = component.as_ref();
                start_components.push(component.to_vec());
                end_components.push(component.to_vec());
                continue;
            }

            if i == prefix.len() {
                start_components.push(lower_component.clone());
                end_components.push(upper_component.clone());
                continue;
            }

            start_components.push(if lower_exclusive {
                Self::wildcard_high_component()
            } else {
                Self::wildcard_low_component()
            });
            end_components.push(if upper_exclusive {
                Self::wildcard_low_component()
            } else {
                Self::wildcard_high_component()
            });
        }

        let lower_key = Self {
            key_kind,
            index_id: *index_id,
            components: start_components,
            primary_key: match lower {
                Bound::Excluded(_) => Self::wildcard_high_pk(),
                Bound::Included(_) | Bound::Unbounded => Self::wildcard_low_pk(),
            },
        };
        let upper_key = Self {
            key_kind,
            index_id: *index_id,
            components: end_components,
            primary_key: match upper {
                Bound::Excluded(_) => Self::wildcard_low_pk(),
                Bound::Included(_) | Bound::Unbounded => Self::wildcard_high_pk(),
            },
        };

        let lower_bound = match lower {
            Bound::Excluded(_) => Bound::Excluded(lower_key),
            Bound::Included(_) | Bound::Unbounded => Bound::Included(lower_key),
        };
        let upper_bound = match upper {
            Bound::Excluded(_) => Bound::Excluded(upper_key),
            Bound::Included(_) | Bound::Unbounded => Bound::Included(upper_key),
        };

        (lower_bound, upper_bound)
    }
}

fn accepted_field_path_component_bytes<'a>(
    accepted_index: &SchemaIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Option<Vec<u8>>, InternalError> {
    let Some(source) = read_slot(field.slot()) else {
        return Err(InternalError::index_key_item_field_missing_on_lookup_row(
            field.field_name(),
        ));
    };
    let Some(source) = resolve_accepted_field_path_component(source, field)? else {
        return Ok(None);
    };

    encode_accepted_field_path_index_component(accepted_index, field, source)
}

fn accepted_field_path_component_bytes_from_slots(
    accepted_index: &SchemaIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source = slots.required_value_by_contract_cow(field.slot())?;
    let Some(source) = resolve_accepted_field_path_component(source.as_ref(), field)? else {
        return Ok(None);
    };

    encode_accepted_field_path_index_component(accepted_index, field, source)
}

fn encode_schema_field_index_component(
    schema_info: &SchemaInfo,
    field_name: &str,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    let Some(field_contract) = schema_info.accepted_field_contract(field_name) else {
        return encode_value_index_component_ref(source);
    };
    if !matches!(field_contract.kind(), AcceptedFieldKind::Enum { .. }) {
        return encode_value_index_component_ref(source);
    }

    encode_admitted_unit_enum_index_component(
        field_contract.enum_catalog(),
        field_contract.value_contract(),
        source,
    )
}

fn encode_accepted_field_path_index_component(
    accepted_index: &SchemaIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    encode_accepted_index_leaf_component(
        accepted_index.enum_catalog(),
        field.persisted_kind(),
        source,
    )
}

fn encode_accepted_expression_field_path_index_component(
    accepted_index: &SchemaExpressionIndexInfo,
    field: &SchemaIndexFieldPathInfo,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    encode_accepted_index_leaf_component(
        accepted_index.enum_catalog(),
        field.persisted_kind(),
        source,
    )
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

fn encode_accepted_index_leaf_component(
    catalog: Option<&crate::db::schema::AcceptedEnumCatalogHandle>,
    kind: Option<&AcceptedFieldKind>,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    let Some(kind @ AcceptedFieldKind::Enum { .. }) = kind else {
        return encode_value_index_component_ref(source);
    };
    let Some(catalog) = catalog else {
        return Err(InternalError::index_unsupported());
    };
    let contract = AcceptedValueContract::from_accepted_field(
        catalog.catalog(),
        kind,
        FieldStorageDecode::ByKind,
    )
    .map_err(|_| InternalError::index_unsupported())?;

    encode_admitted_unit_enum_index_component(catalog, &contract, source)
}

fn encode_admitted_unit_enum_index_component(
    catalog: &crate::db::schema::AcceptedEnumCatalogHandle,
    contract: &AcceptedValueContract,
    source: &Value,
) -> Result<Option<Vec<u8>>, InternalError> {
    if matches!(source, Value::Null) {
        return Ok(None);
    }
    let mut budget = ValueAdmissionBudget::standard();
    let proof = validate_canonical_value(catalog, contract, source, &mut budget)
        .map_err(|_| InternalError::index_unsupported())?;
    let encoded =
        encode_unit_enum_equality_key(&proof).map_err(|_| InternalError::index_unsupported())?;

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
            let Some(source) = resolve_accepted_field_path_component(source.as_ref(), field)?
            else {
                return Ok(None);
            };

            encode_accepted_expression_field_path_index_component(accepted_index, field, source)
        }
        SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
            let source = slots.required_value_by_contract_cow(expression.source().slot())?;
            let semantic_expression = SemanticIndexExpression::new(
                expression.op(),
                accepted_field_path_term(
                    expression.source().field_name(),
                    expression.source().path(),
                ),
            );
            let Some(value) = value_for_accepted_expression_with_index_name(
                accepted_index.name(),
                &semantic_expression,
                source.into_owned(),
            )?
            else {
                return Ok(None);
            };

            encode_value_index_component(value)
        }
    }
}

fn field_path_rebuild_component_bytes_from_slots(
    field: &SchemaFieldPathIndexRebuildKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source = slots.required_value_by_contract_cow(usize::from(field.slot().get()))?;
    let Some(source) = resolve_field_path_rebuild_component(source.as_ref(), field)? else {
        return Ok(None);
    };

    encode_value_index_component_ref(source)
}

#[cfg(any(test, feature = "sql"))]
fn expression_rebuild_component_bytes_from_slots(
    index_name: &str,
    key_item: &SchemaExpressionIndexRebuildKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    match key_item {
        SchemaExpressionIndexRebuildKey::FieldPath(field) => {
            field_path_rebuild_component_bytes_from_slots(field, slots)
        }
        SchemaExpressionIndexRebuildKey::Expression(expression) => {
            expression_rebuild_expression_component_bytes_from_slots(index_name, expression, slots)
        }
    }
}

#[cfg(any(test, feature = "sql"))]
fn expression_rebuild_expression_component_bytes_from_slots(
    index_name: &str,
    expression: &SchemaExpressionIndexRebuildExpression,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Vec<u8>>, InternalError> {
    let source =
        slots.required_value_by_contract_cow(usize::from(expression.source().slot().get()))?;
    let Some(source) = resolve_field_path_rebuild_component(source.as_ref(), expression.source())?
    else {
        return Ok(None);
    };
    let semantic_expression = SemanticIndexExpression::new(
        expression.op(),
        accepted_field_path_term(expression.source().field_name(), expression.source().path()),
    );
    let Some(value) = value_for_accepted_expression_with_index_name(
        index_name,
        &semantic_expression,
        source.clone(),
    )?
    else {
        return Ok(None);
    };

    encode_value_index_component(value)
}

fn resolve_accepted_field_path_component<'a>(
    root: &'a Value,
    field: &SchemaIndexFieldPathInfo,
) -> Result<Option<&'a Value>, InternalError> {
    let mut current = root;
    for segment in field.path().iter().skip(1) {
        let entries = current.as_map().ok_or_else(|| {
            InternalError::persisted_row_field_decode_failed(
                field.field_name(),
                "field-path index traversal requires a map value",
            )
        })?;
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

fn resolve_field_path_rebuild_component<'a>(
    root: &'a Value,
    field: &SchemaFieldPathIndexRebuildKey,
) -> Result<Option<&'a Value>, InternalError> {
    let mut current = root;
    for segment in field.path().iter().skip(1) {
        let entries = current.as_map().ok_or_else(|| {
            InternalError::persisted_row_field_decode_failed(
                field.field_name(),
                "field-path rebuild traversal requires a map value",
            )
        })?;
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

fn accepted_field_path_term(field_name: &str, path: &[String]) -> String {
    if path.len() <= 1 {
        field_name.to_string()
    } else {
        path.join(".")
    }
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
    let component_count = target.key_paths().len();
    if component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            target.name(),
            component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(component_count);
    for field in target.key_paths() {
        let Some(component) = component_bytes(field)? else {
            return Ok(None);
        };

        if component.len() > IndexKey::MAX_COMPONENT_SIZE {
            return Err(InternalError::index_component_exceeds_max_size());
        }
        components.push(component);
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, target.ordinal()),
        components,
        primary_key: IndexKey::compact_primary_key_value_bytes(primary_key)?,
    }))
}

#[cfg(any(test, feature = "sql"))]
fn build_expression_rebuild_target_key(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    target: &SchemaExpressionIndexRebuildTarget,
    component_bytes: &mut ExpressionRebuildComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    let component_count = target.key_items().len();
    if component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            target.name(),
            component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(component_count);
    for key_item in target.key_items() {
        let Some(component) = component_bytes(key_item)? else {
            return Ok(None);
        };

        if component.len() > IndexKey::MAX_COMPONENT_SIZE {
            return Err(InternalError::index_component_exceeds_max_size());
        }
        components.push(component);
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, target.ordinal()),
        components,
        primary_key: IndexKey::compact_primary_key_value_bytes(primary_key)?,
    }))
}

fn build_accepted_field_path_index_key<'a>(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaIndexInfo,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Option<IndexKey>, InternalError> {
    build_accepted_field_path_index_key_from_components(
        entity_tag,
        primary_key,
        accepted_index,
        &mut |accepted_index, field| {
            accepted_field_path_component_bytes(accepted_index, field, read_slot)
        },
    )
}

fn build_accepted_expression_index_key_from_components(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaExpressionIndexInfo,
    component_bytes: &mut AcceptedExpressionComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    let component_count = accepted_index.key_items().len();
    if component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            accepted_index.name(),
            component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(component_count);
    for key_item in accepted_index.key_items() {
        let Some(component) = component_bytes(key_item)? else {
            return Ok(None);
        };

        if component.len() > IndexKey::MAX_COMPONENT_SIZE {
            return Err(InternalError::index_component_exceeds_max_size());
        }
        components.push(component);
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, accepted_index.ordinal()),
        components,
        primary_key: IndexKey::compact_primary_key_value_bytes(primary_key)?,
    }))
}

fn build_accepted_field_path_index_key_from_components(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    accepted_index: &SchemaIndexInfo,
    component_bytes: &mut AcceptedFieldPathComponentEncoder<'_>,
) -> Result<Option<IndexKey>, InternalError> {
    let component_count = accepted_index.fields().len();
    if component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            accepted_index.name(),
            component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(component_count);
    for field in accepted_index.fields() {
        let Some(component) = component_bytes(accepted_index, field)? else {
            return Ok(None);
        };

        if component.len() > IndexKey::MAX_COMPONENT_SIZE {
            return Err(InternalError::index_component_exceeds_max_size());
        }
        components.push(component);
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, accepted_index.ordinal()),
        components,
        primary_key: IndexKey::compact_primary_key_value_bytes(primary_key)?,
    }))
}

// Build one generated-model index key for test-only model parity checks.
#[cfg(test)]
fn build_generated_model_index_key(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    index: &IndexModel,
    component_bytes: &mut dyn FnMut(IndexKeyItem) -> Result<Option<Vec<u8>>, InternalError>,
) -> Result<Option<IndexKey>, InternalError> {
    // Phase 1: validate declared index shape and pre-size the component buffer.
    let index_component_count = match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => fields.len(),
        IndexKeyItemsRef::Items(items) => items.len(),
    };
    if index_component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            index.name(),
            index_component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(index_component_count);

    // Phase 2: materialize canonical key-item bytes in declaration order.
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            for &field in fields {
                let key_item = IndexKeyItem::Field(field);
                let Some(component) = component_bytes(key_item)? else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for &key_item in items {
                let Some(component) = component_bytes(key_item)? else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
    }

    // Phase 3: encode the primary key once and assemble the final user key.
    let primary_key = IndexKey::compact_primary_key_value_bytes(primary_key)?;

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, index.ordinal()),
        components,
        primary_key,
    }))
}

fn build_index_key_from_access_contract<'a>(
    entity_tag: EntityTag,
    primary_key: &PrimaryKeyValue,
    schema_info: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Option<IndexKey>, InternalError> {
    let index_component_count = index.key_arity();
    if index_component_count > MAX_INDEX_FIELDS {
        return Err(InternalError::index_key_field_count_exceeds_max(
            index.name(),
            index_component_count,
            MAX_INDEX_FIELDS,
        ));
    }

    let mut components = Vec::with_capacity(index_component_count);

    match index.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => {
            for field in fields {
                let key_item = SemanticIndexKeyItemRef::Field(field.as_str());
                let Some(component) =
                    index_component_bytes_from_slot_ref_reader_with_access_contract(
                        schema_info,
                        index,
                        key_item,
                        read_slot,
                    )?
                else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
        SemanticIndexKeyItemsRef::Accepted(items) => {
            for key_item in items {
                let semantic_key_item = key_item.as_ref();
                let Some(component) =
                    index_component_bytes_from_slot_ref_reader_with_access_contract(
                        schema_info,
                        index,
                        semantic_key_item,
                        read_slot,
                    )?
                else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            for &field in fields {
                let key_item = SemanticIndexKeyItemRef::Field(field);
                let Some(component) =
                    index_component_bytes_from_slot_ref_reader_with_access_contract(
                        schema_info,
                        index,
                        key_item,
                        read_slot,
                    )?
                else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
            for &key_item in items {
                let semantic_key_item = match key_item {
                    IndexKeyItem::Field(field) => SemanticIndexKeyItemRef::Field(field),
                    IndexKeyItem::Expression(expression) => {
                        SemanticIndexKeyItemRef::Expression(expression)
                    }
                };
                let Some(component) =
                    index_component_bytes_from_slot_ref_reader_with_access_contract(
                        schema_info,
                        index,
                        semantic_key_item,
                        read_slot,
                    )?
                else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, component)?;
            }
        }
    }

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, index.ordinal()),
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
fn normalize_range_component_bound<C: AsRef<[u8]>>(
    bound: &Bound<C>,
    high: bool,
) -> Option<Vec<u8>> {
    match bound {
        Bound::Unbounded => Some(if high {
            IndexKey::wildcard_high_component()
        } else {
            IndexKey::wildcard_low_component()
        }),
        Bound::Included(component) | Bound::Excluded(component) => {
            let component = component.as_ref();
            if component.is_empty() || component.len() > IndexKey::MAX_COMPONENT_SIZE {
                return None;
            }
            Some(component.to_vec())
        }
    }
}

#[cfg(test)]
mod accepted_enum_tests {
    use super::*;
    use crate::{
        db::schema::{
            AcceptedEnumCatalogHandle, AcceptedSchemaRevision,
            build_initial_accepted_enum_catalog_from_kinds_for_tests,
        },
        model::field::{EnumVariantModel, FieldKind},
        value::{ValueEnum, ValueTag},
    };

    static UNIT_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
        "Ready",
        None,
        FieldStorageDecode::ByKind,
    )];
    static UNIT_KIND: FieldKind = FieldKind::Enum {
        path: "index::Status",
        variants: &UNIT_VARIANTS,
    };
    static PAYLOAD_KIND: FieldKind = FieldKind::Nat64;
    static PAYLOAD_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
        "Value",
        Some(&PAYLOAD_KIND),
        FieldStorageDecode::ByKind,
    )];
    static PAYLOAD_ENUM_KIND: FieldKind = FieldKind::Enum {
        path: "index::Payload",
        variants: &PAYLOAD_VARIANTS,
    };

    #[test]
    fn accepted_index_leaf_uses_catalog_ids_for_unit_enum_key() {
        let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[UNIT_KIND])
            .expect("unit enum catalog should build");
        let handle =
            AcceptedEnumCatalogHandle::new_for_tests(catalog, AcceptedSchemaRevision::INITIAL);
        let kind = AcceptedFieldKind::from_model_kind(UNIT_KIND);
        let value = Value::Enum(ValueEnum::test_unit(1, 1));

        assert_eq!(
            encode_accepted_index_leaf_component(Some(&handle), Some(&kind), &value)
                .expect("accepted unit enum should produce an index component"),
            Some(vec![ValueTag::Enum.to_u8(), 1, 0, 0, 0, 1, 0, 0, 0, 1, 0]),
        );
    }

    #[test]
    fn accepted_index_leaf_rejects_payload_enum_without_stable_key_capability() {
        let catalog =
            build_initial_accepted_enum_catalog_from_kinds_for_tests(&[PAYLOAD_ENUM_KIND])
                .expect("payload enum catalog should build");
        let handle =
            AcceptedEnumCatalogHandle::new_for_tests(catalog, AcceptedSchemaRevision::INITIAL);
        let kind = AcceptedFieldKind::from_model_kind(PAYLOAD_ENUM_KIND);
        let value = Value::Enum(ValueEnum::test_payload(1, 1, Value::Nat64(7)));

        assert!(
            encode_accepted_index_leaf_component(Some(&handle), Some(&kind), &value).is_err(),
            "payload enums must remain outside canonical index-key capability",
        );
    }
}
