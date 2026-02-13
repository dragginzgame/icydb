use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, IndexName},
        index::{
            fingerprint,
            key::{IndexId, IndexKey},
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

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        let mut len = 0usize;

        for field in index.fields {
            let Some(value) = entity.get_value(field) else {
                return Ok(None);
            };
            let Some(fingerprint) = fingerprint::to_index_fingerprint(&value)? else {
                return Ok(None);
            };

            values[len] = fingerprint;
            len += 1;
        }

        #[allow(clippy::cast_possible_truncation)]
        Ok(Some(Self {
            index_id: IndexId::new::<E>(index),
            len: len as u8,
            values,
        }))
    }

    #[must_use]
    pub const fn empty(index_id: IndexId) -> Self {
        Self {
            index_id,
            len: 0,
            values: [[0u8; 16]; MAX_INDEX_FIELDS],
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn bounds_for_prefix(
        index_id: IndexId,
        index_len: usize,
        prefix: &[[u8; 16]],
    ) -> (Self, Self) {
        let mut start = Self::empty(index_id);
        let mut end = Self::empty(index_id);

        for (i, fingerprint) in prefix.iter().enumerate() {
            start.values[i] = *fingerprint;
            end.values[i] = *fingerprint;
        }

        start.len = index_len as u8;
        end.len = start.len;

        for value in end.values.iter_mut().take(index_len).skip(prefix.len()) {
            *value = [0xFF; 16];
        }

        (start, end)
    }
}
