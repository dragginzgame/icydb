use crate::{
    MAX_INDEX_FIELDS,
    db::{
        data::StorageKey,
        identity::{EntityName, IndexName},
        index::key::{EncodedValue, IndexId, IndexKey, IndexKeyKind, OrderedValueEncodeError},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue, FieldValue},
};
use std::ops::Bound;

impl IndexId {
    /// Build an index id from static entity metadata.
    ///
    /// This is the canonical constructor. All invariants are expected
    /// to hold for schema-defined indexes. Any violation is a programmer
    /// or schema error and will panic.
    #[must_use]
    pub(crate) fn new<E: EntityKind>(index: &IndexModel) -> Self {
        let entity = EntityName::try_from_str(E::ENTITY_NAME)
            .expect("EntityKind::ENTITY_NAME must be a valid EntityName");

        let name = IndexName::try_from_parts(&entity, index.fields)
            .expect("IndexModel must define a valid IndexName");

        Self(name)
    }
}

impl IndexKey {
    /// Build an index key; returns `Ok(None)` if any indexed field is missing or non-indexable.
    /// `Value::Null` and unsupported canonical kinds are treated as non-indexable.
    pub(crate) fn new<E: EntityKind + EntityValue>(
        entity: &E,
        index: &IndexModel,
    ) -> Result<Option<Self>, InternalError> {
        if index.fields.len() > MAX_INDEX_FIELDS {
            return Err(InternalError::index_invariant(format!(
                "index '{}' has {} fields (max {})",
                index.name,
                index.fields.len(),
                MAX_INDEX_FIELDS
            )));
        }

        let mut components = Vec::with_capacity(index.fields.len());

        for field in index.fields {
            let Some(value) = entity.get_value(field) else {
                return Ok(None);
            };

            let encoded = match EncodedValue::try_from_ref(&value) {
                Ok(encoded) => encoded,
                Err(
                    OrderedValueEncodeError::NullNotIndexable
                    | OrderedValueEncodeError::UnsupportedValueKind { .. },
                ) => {
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            };
            let component = encoded.encoded().to_vec();

            if component.len() > Self::MAX_COMPONENT_SIZE {
                return Err(InternalError::index_unsupported(format!(
                    "index component exceeds max size: field '{}' -> {} bytes (limit {})",
                    field,
                    component.len(),
                    Self::MAX_COMPONENT_SIZE
                )));
            }

            components.push(component);
        }

        let entity_key_value = entity.id().key().to_value();
        let storage_key = StorageKey::try_from_value(&entity_key_value)?;
        let primary_key = storage_key.to_bytes()?.to_vec();

        #[expect(clippy::cast_possible_truncation)]
        Ok(Some(Self {
            key_kind: IndexKeyKind::User,
            index_id: IndexId::new::<E>(index),
            component_count: components.len() as u8,
            components,
            primary_key,
        }))
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
            component_count: 0,
            components: Vec::new(),
            primary_key: Self::wildcard_low_pk(),
        }
    }

    #[must_use]
    pub(in crate::db::index) fn bounds_for_prefix<C: AsRef<[u8]>>(
        index_id: &IndexId,
        index_len: usize,
        prefix: &[C],
    ) -> (Self, Self) {
        Self::bounds_for_prefix_with_kind(index_id, IndexKeyKind::User, index_len, prefix)
    }

    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub(in crate::db::index) fn bounds_for_prefix_with_kind<C: AsRef<[u8]>>(
        index_id: &IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[C],
    ) -> (Self, Self) {
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

        let component_count = index_len as u8;

        (
            Self {
                key_kind,
                index_id: *index_id,
                component_count,
                components: start_components,
                primary_key: Self::wildcard_low_pk(),
            },
            Self {
                key_kind,
                index_id: *index_id,
                component_count,
                components: end_components,
                primary_key: Self::wildcard_high_pk(),
            },
        )
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
    #[expect(clippy::cast_possible_truncation)]
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

        let component_count = index_len as u8;
        let lower_key = Self {
            key_kind,
            index_id: *index_id,
            component_count,
            components: start_components,
            primary_key: match lower {
                Bound::Excluded(_) => Self::wildcard_high_pk(),
                Bound::Included(_) | Bound::Unbounded => Self::wildcard_low_pk(),
            },
        };
        let upper_key = Self {
            key_kind,
            index_id: *index_id,
            component_count,
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
