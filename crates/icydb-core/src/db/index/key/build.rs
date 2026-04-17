//! Module: index::key::build
//! Responsibility: build `IndexId`/`IndexKey` values from entity + index models.
//! Does not own: raw key byte framing (codec) or index-store writes.
//! Boundary: planning/mutation paths call into this constructor layer.

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::{CanonicalSlotReader, ScalarSlotValueRef, StorageKey},
        index::{
            derive_index_expression_value,
            key::ordered::encode_canonical_index_component,
            key::{
                IndexId, IndexKey, IndexKeyKind, OrderedValueEncodeError,
                encode_canonical_index_component_from_scalar,
            },
        },
        scalar_expr::{compile_scalar_index_key_item_program, eval_canonical_scalar_value_program},
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_field_slot},
        index::{IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

fn value_for_expression(
    index: &IndexModel,
    expression: IndexExpression,
    source: Value,
) -> Result<Option<Value>, InternalError> {
    let source_label = source.canonical_tag().label();
    derive_index_expression_value(expression, source).map_err(|expected| {
        InternalError::index_expression_source_type_mismatch(
            index.name(),
            expression,
            expected,
            source_label,
        )
    })
}

fn index_component_bytes_from_slot_ref_reader<'a>(
    entity_model: &EntityModel,
    index: &IndexModel,
    key_item: IndexKeyItem,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Option<Vec<u8>>, InternalError> {
    let field = key_item.field();
    let Some(field_index) = resolve_field_slot(entity_model, field) else {
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
        IndexKeyItem::Field(_) => encode_value_index_component_ref(source),
        IndexKeyItem::Expression(expression) => {
            value_for_expression(index, expression, source.clone())?
                .map_or(Ok(None), encode_value_index_component)
        }
    }
}

impl IndexKey {
    /// Build an index key from one structural slot reader plus runtime identity.
    /// Plain field key items read scalar slot values directly when available.
    pub(crate) fn new_from_slots(
        entity_tag: EntityTag,
        storage_key: StorageKey,
        slots: &dyn CanonicalSlotReader,
        index: &IndexModel,
    ) -> Result<Option<Self>, InternalError> {
        let mut component_bytes =
            |key_item| index_component_bytes_from_slots(slots, index, key_item);

        build_index_key(entity_tag, storage_key, index, &mut component_bytes)
    }

    /// Build an index key from one structural row slot reader plus runtime identity.
    /// Returns `Ok(None)` when indexed values are non-indexable.
    #[cfg(test)]
    pub(crate) fn new_from_slot_reader(
        entity_tag: EntityTag,
        storage_key: StorageKey,
        entity_model: &EntityModel,
        index: &IndexModel,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> Result<Option<Self>, InternalError> {
        let mut component_bytes = |key_item: IndexKeyItem| {
            let field = key_item.field();
            let Some(field_index) = resolve_field_slot(entity_model, field) else {
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

        build_index_key(entity_tag, storage_key, index, &mut component_bytes)
    }

    /// Build an index key from one structural row slot ref reader plus runtime identity.
    /// Returns `Ok(None)` when indexed values are non-indexable.
    pub(crate) fn new_from_slot_ref_reader<'a>(
        entity_tag: EntityTag,
        storage_key: StorageKey,
        entity_model: &EntityModel,
        index: &IndexModel,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<Self>, InternalError> {
        let mut component_bytes = |key_item| {
            index_component_bytes_from_slot_ref_reader(entity_model, index, key_item, read_slot)
        };

        build_index_key(entity_tag, storage_key, index, &mut component_bytes)
    }

    /// Build an index key from a typed entity for test-only parity checks.
    /// `Value::Null` and unsupported canonical kinds are treated as non-indexable.
    #[cfg(test)]
    pub(crate) fn new<E: crate::traits::EntityKind + crate::traits::EntityValue>(
        entity: &E,
        index: &IndexModel,
    ) -> Result<Option<Self>, InternalError> {
        let entity_key = entity.id().key();
        let entity_key_value = crate::traits::FieldValue::to_value(&entity_key);
        let storage_key = StorageKey::try_from_value(&entity_key_value)?;
        let mut read_slot = |slot| entity.get_value_by_index(slot);

        Self::new_from_slot_reader(E::ENTITY_TAG, storage_key, E::MODEL, index, &mut read_slot)
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

// Build one user-facing index key by sharing the canonical component walk
// across structural slot readers and typed slot-reader adapters.
fn build_index_key(
    entity_tag: EntityTag,
    storage_key: StorageKey,
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

                push_index_key_component(&mut components, key_item, component)?;
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for &key_item in items {
                let Some(component) = component_bytes(key_item)? else {
                    return Ok(None);
                };

                push_index_key_component(&mut components, key_item, component)?;
            }
        }
    }

    // Phase 3: encode the primary key once and assemble the final user key.
    let primary_key = storage_key.to_bytes()?.to_vec();

    Ok(Some(IndexKey {
        key_kind: IndexKeyKind::User,
        index_id: IndexId::new(entity_tag, index.ordinal()),
        components,
        primary_key,
    }))
}

// Push one canonical component after enforcing the shared size contract.
fn push_index_key_component(
    components: &mut Vec<Vec<u8>>,
    key_item: IndexKeyItem,
    component: Vec<u8>,
) -> Result<(), InternalError> {
    if component.len() > IndexKey::MAX_COMPONENT_SIZE {
        return Err(InternalError::index_component_exceeds_max_size(
            key_item.canonical_text(),
            component.len(),
            IndexKey::MAX_COMPONENT_SIZE,
        ));
    }

    components.push(component);
    Ok(())
}

// Build one canonical index component directly from one slot reader.
fn index_component_bytes_from_slots(
    slots: &dyn CanonicalSlotReader,
    index: &IndexModel,
    key_item: IndexKeyItem,
) -> Result<Option<Vec<u8>>, InternalError> {
    let field = key_item.field();

    if let Some(program) = compile_scalar_index_key_item_program(slots.model(), key_item) {
        let source = eval_canonical_scalar_value_program(&program, slots)?;

        return encode_scalar_index_component(source.as_slot_value_ref());
    }

    let Some(field_index) = resolve_field_slot(slots.model(), field) else {
        return Err(InternalError::index_key_item_field_missing_on_entity_model(
            field,
        ));
    };

    match key_item {
        IndexKeyItem::Field(_) => encode_value_index_component_ref(
            slots.required_value_by_contract_cow(field_index)?.as_ref(),
        ),
        IndexKeyItem::Expression(expression) => {
            let source = slots.required_value_by_contract_cow(field_index)?;
            let Some(value) = value_for_expression(index, expression, source.into_owned())? else {
                return Ok(None);
            };

            encode_value_index_component(value)
        }
    }
}

// Encode one scalar slot value into canonical index bytes.
fn encode_scalar_index_component(
    source: ScalarSlotValueRef<'_>,
) -> Result<Option<Vec<u8>>, InternalError> {
    match source {
        ScalarSlotValueRef::Null => Ok(None),
        ScalarSlotValueRef::Value(source) => {
            match encode_canonical_index_component_from_scalar(source) {
                Ok(component) => Ok(Some(component)),
                Err(
                    OrderedValueEncodeError::NullNotIndexable
                    | OrderedValueEncodeError::UnsupportedValueKind { .. },
                ) => Ok(None),
                Err(err) => Err(err.into()),
            }
        }
    }
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
            | OrderedValueEncodeError::UnsupportedValueKind { .. },
        ) => {
            return Ok(None);
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Some(encoded))
}

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
