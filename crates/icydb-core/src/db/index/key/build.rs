use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, IndexName},
        index::key::{
            IndexId, IndexKey, IndexKeyKind,
            ordered::{OrderedValueEncodeError, encode_canonical_index_component},
        },
        store::StorageKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::{EntityKind, EntityValue, FieldValue},
};

impl IndexId {
    /// Build an index id from static entity metadata.
    ///
    /// This is the canonical constructor. All invariants are expected
    /// to hold for schema-defined indexes. Any violation is a programmer
    /// or schema error and will panic.
    #[must_use]
    pub fn new<E: EntityKind>(index: &IndexModel) -> Self {
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
    pub fn new<E: EntityKind + EntityValue>(
        entity: &E,
        index: &IndexModel,
    ) -> Result<Option<Self>, InternalError> {
        if index.fields.len() > MAX_INDEX_FIELDS {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Index,
                format!(
                    "index '{}' has {} fields (max {})",
                    index.name,
                    index.fields.len(),
                    MAX_INDEX_FIELDS
                ),
            ));
        }

        let mut components = Vec::with_capacity(index.fields.len());

        for field in index.fields {
            let Some(value) = entity.get_value(field) else {
                return Ok(None);
            };

            let component = match encode_canonical_index_component(&value) {
                Ok(component) => component,
                Err(
                    OrderedValueEncodeError::NullNotIndexable
                    | OrderedValueEncodeError::UnsupportedValueKind { .. },
                ) => {
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            };

            if component.len() > Self::MAX_COMPONENT_SIZE {
                return Err(InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!(
                        "index component exceeds max size: field '{}' -> {} bytes (limit {})",
                        field,
                        component.len(),
                        Self::MAX_COMPONENT_SIZE
                    ),
                ));
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

    #[must_use]
    pub fn empty(index_id: IndexId) -> Self {
        Self::empty_with_kind(index_id, IndexKeyKind::User)
    }

    #[must_use]
    pub fn empty_with_kind(index_id: IndexId, key_kind: IndexKeyKind) -> Self {
        Self {
            key_kind,
            index_id,
            component_count: 0,
            components: Vec::new(),
            primary_key: Self::wildcard_low_pk(),
        }
    }

    #[must_use]
    pub fn bounds_for_prefix(
        index_id: IndexId,
        index_len: usize,
        prefix: &[Vec<u8>],
    ) -> (Self, Self) {
        Self::bounds_for_prefix_with_kind(index_id, IndexKeyKind::User, index_len, prefix)
    }

    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn bounds_for_prefix_with_kind(
        index_id: IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[Vec<u8>],
    ) -> (Self, Self) {
        if index_len > MAX_INDEX_FIELDS || prefix.len() > index_len {
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (empty.clone(), empty);
        }

        let mut start_components = Vec::with_capacity(index_len);
        let mut end_components = Vec::with_capacity(index_len);

        for i in 0..index_len {
            if let Some(component) = prefix.get(i) {
                if component.is_empty() || component.len() > Self::MAX_COMPONENT_SIZE {
                    let empty = Self::empty_with_kind(index_id, key_kind);
                    return (empty.clone(), empty);
                }

                start_components.push(component.clone());
                end_components.push(component.clone());
                continue;
            }

            start_components.push(Self::wildcard_low_component());
            end_components.push(Self::wildcard_high_component());
        }

        let component_count = index_len as u8;

        (
            Self {
                key_kind,
                index_id,
                component_count,
                components: start_components,
                primary_key: Self::wildcard_low_pk(),
            },
            Self {
                key_kind,
                index_id,
                component_count,
                components: end_components,
                primary_key: Self::wildcard_high_pk(),
            },
        )
    }
}
