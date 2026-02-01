use crate::{
    MAX_INDEX_FIELDS,
    db::{
        identity::{EntityName, EntityNameError, IndexName, IndexNameError},
        index::fingerprint,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::{EntityKind, IndexModel},
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use derive_more::Display;
use std::borrow::Cow;
use thiserror::Error as ThisError;

///
/// IndexId
///
/// Logical identifier for an index.
/// Combines entity identity and indexed field set into a stable, ordered name.
/// Used as the prefix component of all index keys.
///

#[derive(Clone, Copy, Debug, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexId(pub IndexName);

impl IndexId {
    /// Build an index id from static entity metadata.
    ///
    /// This is the canonical constructor. Identity invariants are always enforced.
    pub fn try_new<E: EntityKind>(index: &IndexModel) -> Result<Self, IndexIdError> {
        let entity = EntityName::try_from_str(E::ENTITY_NAME)?;
        let name = IndexName::try_from_parts(&entity, index.fields)?;
        Ok(Self(name))
    }

    /// Build an index id from static metadata, panicking on invariant violation.
    ///
    /// This is intended for generated code and schema-defined indexes.
    #[must_use]
    pub fn new<E: EntityKind>(index: &IndexModel) -> Self {
        Self::try_new::<E>(index).expect("static IndexModel must define a valid IndexId")
    }

    /// Maximum sentinel value for stable-memory bounds.
    /// Used for upper-bound scans and fuzz validation.
    #[must_use]
    pub const fn max_storable() -> Self {
        Self(IndexName::max_storable())
    }
}

///
/// IndexIdError
/// Errors returned when constructing an [`IndexId`].
/// This surfaces identity validation failures.
///

#[derive(Debug, ThisError)]
pub enum IndexIdError {
    #[error("entity name invalid: {0}")]
    EntityName(#[from] EntityNameError),
    #[error("index name invalid: {0}")]
    IndexName(#[from] IndexNameError),
}

///
/// IndexKey
///
/// Fully-qualified index lookup key.
/// Fixed-size, manually encoded structure designed for stable-memory ordering.
/// Ordering of this type must exactly match byte-level ordering.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexKey {
    index_id: IndexId,
    len: u8,
    values: [[u8; 16]; MAX_INDEX_FIELDS],
}

#[expect(clippy::cast_possible_truncation)]
impl IndexKey {
    /// Fixed on-disk size in bytes (stable, protocol-level)
    pub const STORED_SIZE_BYTES: u64 =
        IndexName::STORED_SIZE_BYTES + 1 + (MAX_INDEX_FIELDS as u64 * 16);

    /// Fixed in-memory size (for buffers and arrays)
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    /// Build an index key; returns `Ok(None)` if any indexed field is missing or non-indexable.
    /// `Value::None` and `Value::Unsupported` are treated as non-indexable.
    pub fn new<E: EntityKind>(
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
            let Some(fp) = fingerprint::to_index_fingerprint(&value)? else {
                return Ok(None);
            };
            values[len] = fp;
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

        for (i, fp) in prefix.iter().enumerate() {
            start.values[i] = *fp;
            end.values[i] = *fp;
        }

        start.len = index_len as u8;
        end.len = start.len;

        for value in end.values.iter_mut().take(index_len).skip(prefix.len()) {
            *value = [0xFF; 16];
        }

        (start, end)
    }

    #[must_use]
    pub fn to_raw(&self) -> RawIndexKey {
        let mut buf = [0u8; Self::STORED_SIZE_USIZE];

        let name_bytes = self.index_id.0.to_bytes();
        buf[..name_bytes.len()].copy_from_slice(&name_bytes);

        let mut offset = IndexName::STORED_SIZE_USIZE;
        buf[offset] = self.len;
        offset += 1;

        for value in &self.values {
            buf[offset..offset + 16].copy_from_slice(value);
            offset += 16;
        }

        RawIndexKey(buf)
    }

    pub fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
        let bytes = &raw.0;
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("corrupted IndexKey: invalid size");
        }

        let mut offset = 0;

        let index_name =
            IndexName::from_bytes(&bytes[offset..offset + IndexName::STORED_SIZE_USIZE])
                .map_err(|_| "corrupted IndexKey: invalid IndexName bytes")?;
        offset += IndexName::STORED_SIZE_USIZE;

        let len = bytes[offset];
        offset += 1;

        if len as usize > MAX_INDEX_FIELDS {
            return Err("corrupted IndexKey: invalid index length");
        }

        let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
        for value in &mut values {
            value.copy_from_slice(&bytes[offset..offset + 16]);
            offset += 16;
        }

        let len_usize = len as usize;
        for value in values.iter().skip(len_usize) {
            if value.iter().any(|&b| b != 0) {
                return Err("corrupted IndexKey: non-zero fingerprint padding");
            }
        }

        Ok(Self {
            index_id: IndexId(index_name),
            len,
            values,
        })
    }
}

///
/// RawIndexKey
///
/// Fixed-size, stable-memory representation of IndexKey.
/// This is the form stored in BTreeMap keys.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawIndexKey([u8; IndexKey::STORED_SIZE_USIZE]);

impl RawIndexKey {
    /// Borrow the raw byte representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; IndexKey::STORED_SIZE_USIZE] {
        &self.0
    }
}

impl Storable for RawIndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut out = [0u8; IndexKey::STORED_SIZE_USIZE];
        if bytes.len() == out.len() {
            out.copy_from_slice(bytes.as_ref());
        }
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    #[expect(clippy::cast_possible_truncation)]
    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::STORED_SIZE_BYTES as u32,
        is_fixed_size: true,
    };
}
