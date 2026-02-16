use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, IndexName},
        index::{
            fingerprint,
            key::{IndexId, IndexKey, IndexKeyKind},
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
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
    /// `Value::Null` is treated as non-indexable.
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

        let mut values = Vec::with_capacity(index.fields.len());

        for field in index.fields {
            let Some(value) = entity.get_value(field) else {
                return Ok(None);
            };
            let Some(fingerprint) = fingerprint::to_index_fingerprint(&value)? else {
                return Ok(None);
            };

            values.push(fingerprint);
        }

        #[allow(clippy::cast_possible_truncation)]
        Ok(Some(Self {
            key_kind: IndexKeyKind::User,
            index_id: IndexId::new::<E>(index),
            len: values.len() as u8,
            values,
        }))
    }

    #[must_use]
    pub const fn empty(index_id: IndexId) -> Self {
        Self::empty_with_kind(index_id, IndexKeyKind::User)
    }

    #[must_use]
    pub const fn empty_with_kind(index_id: IndexId, key_kind: IndexKeyKind) -> Self {
        Self {
            key_kind,
            index_id,
            len: 0,
            values: Vec::new(),
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn bounds_for_prefix(
        index_id: IndexId,
        index_len: usize,
        prefix: &[[u8; 16]],
    ) -> (Self, Self) {
        Self::bounds_for_prefix_with_kind(index_id, IndexKeyKind::User, index_len, prefix)
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn bounds_for_prefix_with_kind(
        index_id: IndexId,
        key_kind: IndexKeyKind,
        index_len: usize,
        prefix: &[[u8; 16]],
    ) -> (Self, Self) {
        if index_len > MAX_INDEX_FIELDS || prefix.len() > index_len {
            let empty = Self::empty_with_kind(index_id, key_kind);
            return (empty.clone(), empty);
        }

        let mut start_values = Vec::with_capacity(index_len);
        let mut end_values = Vec::with_capacity(index_len);

        for i in 0..index_len {
            if let Some(fingerprint) = prefix.get(i) {
                start_values.push(*fingerprint);
                end_values.push(*fingerprint);
                continue;
            }

            start_values.push([0; 16]);
            end_values.push([0xFF; 16]);
        }

        let len = index_len as u8;

        (
            Self {
                key_kind,
                index_id,
                len,
                values: start_values,
            },
            Self {
                key_kind,
                index_id,
                len,
                values: end_values,
            },
        )
    }
}
