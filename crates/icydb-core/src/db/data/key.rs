//! Module: data::key
//! Responsibility: canonical entity-aware data-key encoding and decoding.
//! Does not own: row payload bytes, commit sequencing, or query semantics.
//! Boundary: data::store persists `RawDataStoreKey`; higher layers use `DecodedDataStoreKey`.

#![expect(clippy::cast_possible_truncation)]

use crate::{
    db::{
        PrimaryKeyDecode, PrimaryKeyEncode, PrimaryKeyEncodeError,
        key_taxonomy::{
            COMPOSITE_PRIMARY_KEY_MAX_SIZE, CompositePrimaryKeyValue,
            CompositePrimaryKeyValueError, DataStoreKey, EncodedPrimaryKey, MAX_PRIMARY_KEY_FIELDS,
            PrimaryKeyComponent, PrimaryKeyValue, RawDataStoreKey, RawDataStoreKeyRange,
        },
    },
    error::InternalError,
    traits::{EntityKind, Storable},
    types::EntityTag,
    value::Value,
};
use ic_memory::stable_structures::storable::Bound as StorableBound;
use std::{
    borrow::Cow,
    cell::OnceCell,
    fmt::{self, Display},
    hash::{Hash, Hasher},
    mem::size_of,
    ops::Bound as RangeBound,
};

///
/// DecodedDataStoreKeyEncodeError
/// (serialize boundary)
///

#[derive(Debug)]
enum DecodedDataStoreKeyEncodeError {
    CompactKeyEncoding {
        key: DecodedDataStoreKey,
        source: crate::db::key_taxonomy::CompactPrimaryKeyEncodeError,
    },
}

impl From<DecodedDataStoreKeyEncodeError> for InternalError {
    fn from(err: DecodedDataStoreKeyEncodeError) -> Self {
        match err {
            DecodedDataStoreKeyEncodeError::CompactKeyEncoding { key, source } => {
                let _ = (key, source);
                Self::serialize_unsupported()
            }
        }
    }
}

///
/// PrimaryKeyValueDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug)]
enum PrimaryKeyValueDecodeError {
    InvalidCompactEncoding,
}

impl From<crate::db::key_taxonomy::CompactPrimaryKeyDecodeError> for PrimaryKeyValueDecodeError {
    fn from(source: crate::db::key_taxonomy::CompactPrimaryKeyDecodeError) -> Self {
        let _ = source;
        Self::InvalidCompactEncoding
    }
}

///
/// DecodedDataStoreKeyDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug)]
pub(in crate::db) enum DecodedDataStoreKeyDecodeError {
    Key,

    StoreKey,
}

impl From<PrimaryKeyValueDecodeError> for DecodedDataStoreKeyDecodeError {
    fn from(err: PrimaryKeyValueDecodeError) -> Self {
        let _ = err;
        Self::Key
    }
}

///
/// DecodedDataStoreKey
///

pub(in crate::db) struct DecodedDataStoreKey {
    entity: EntityTag,
    key: PrimaryKeyValue,
    raw: OnceCell<RawDataStoreKey>,
}

impl DecodedDataStoreKey {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Construct from runtime identity and a scalar-or-composite key payload.
    #[must_use]
    pub(in crate::db) const fn new(entity: EntityTag, key: &PrimaryKeyValue) -> Self {
        Self {
            entity,
            key: *key,
            raw: OnceCell::new(),
        }
    }

    /// Construct from runtime identity and a scalar-or-composite key payload.
    #[must_use]
    pub(in crate::db) const fn new_primary_key_value(
        entity: EntityTag,
        key: &PrimaryKeyValue,
    ) -> Self {
        Self::new(entity, key)
    }

    /// Construct one data key while freezing the already-known raw on-disk
    /// representation alongside the decoded scalar-or-composite primary key.
    #[must_use]
    pub(in crate::db) fn new_with_raw_primary_key_value(
        entity: EntityTag,
        key: &PrimaryKeyValue,
        raw: RawDataStoreKey,
    ) -> Self {
        let cache = OnceCell::new();
        let _ = cache.set(raw);

        Self {
            entity,
            key: *key,
            raw: cache,
        }
    }

    /// Construct using compile-time entity metadata.
    ///
    /// This requires that the entity key is persistable.
    pub(in crate::db) fn try_new<E>(key: E::Key) -> Result<Self, InternalError>
    where
        E: EntityKind,
    {
        Self::try_from_typed_key(E::ENTITY_TAG, &key)
    }

    /// Construct from one entity tag plus one typed field-value key.
    ///
    /// This keeps key encoding shared across entity-bound callers without
    /// forcing the data-key boundary itself to be generic over `E`.
    pub(in crate::db) fn try_from_typed_key<K>(
        entity: EntityTag,
        key: &K,
    ) -> Result<Self, InternalError>
    where
        K: PrimaryKeyEncode,
    {
        let key = key.to_primary_key_value()?;

        Ok(Self::new_primary_key_value(entity, &key))
    }

    /// Construct from one entity tag plus one structural planner key literal.
    ///
    /// This is the structural key-codec boundary used by execution paths that
    /// no longer carry typed entity keys.
    pub(in crate::db) fn try_from_structural_key(
        entity: EntityTag,
        key: &Value,
    ) -> Result<Self, InternalError> {
        let key = primary_key_value_from_structural_value(key)?;

        Ok(Self::new_primary_key_value(entity, &key))
    }

    /// Decode a raw entity key from this data key.
    ///
    /// This is a fallible boundary that validates entity identity and
    /// key compatibility against the target entity type.
    pub(in crate::db) fn try_key<E>(&self) -> Result<E::Key, InternalError>
    where
        E: EntityKind,
    {
        let expected = E::ENTITY_TAG;
        if self.entity != expected {
            return Err(InternalError::data_key_entity_mismatch());
        }

        <E::Key as PrimaryKeyDecode>::from_primary_key_value(&self.key)
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity
    }

    #[must_use]
    pub(in crate::db) const fn primary_key_value(&self) -> PrimaryKeyValue {
        self.key
    }

    pub(in crate::db) fn primary_key_runtime_value(&self) -> Value {
        self.key.as_runtime_value()
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn primary_key_component_runtime_value(
        &self,
        component_index: usize,
    ) -> Result<Value, InternalError> {
        self.key
            .component_runtime_value(component_index)
            .ok_or_else(InternalError::query_executor_invariant)
    }

    /// Compute the maximum on-disk entry size from value length.
    #[must_use]
    pub(in crate::db) const fn entry_size_bytes(value_len: u64) -> u64 {
        RawDataStoreKey::MAX_STORED_SIZE_BYTES + value_len
    }

    // ------------------------------------------------------------------
    // Encoding / decoding
    // ------------------------------------------------------------------

    /// Encode into compact on-disk representation.
    pub(in crate::db) fn to_raw(&self) -> Result<RawDataStoreKey, InternalError> {
        self.raw_key().cloned()
    }

    /// Borrow the compact on-disk representation, populating the local cache
    /// on first use. Hot row-read paths can use this to avoid cloning raw keys
    /// that were already recovered from primary/index traversal.
    pub(in crate::db) fn raw_key(&self) -> Result<&RawDataStoreKey, InternalError> {
        if let Some(raw) = self.raw.get() {
            return Ok(raw);
        }

        let raw = self.to_raw_compact_key_error().map_err(|err| {
            InternalError::from(DecodedDataStoreKeyEncodeError::CompactKeyEncoding {
                key: self.clone(),
                source: err,
            })
        })?;
        let _ = self.raw.set(raw);

        self.raw
            .get()
            .ok_or_else(InternalError::query_executor_invariant)
    }

    /// Encode into compact on-disk representation, returning compact-key
    /// encode errors directly.
    pub(in crate::db) fn to_raw_compact_key_error(
        &self,
    ) -> Result<RawDataStoreKey, crate::db::key_taxonomy::CompactPrimaryKeyEncodeError> {
        let primary_key = EncodedPrimaryKey::encode(self.key)?;
        let raw = DataStoreKey::new(self.entity, primary_key).to_raw();

        Ok(raw)
    }

    pub(in crate::db) fn try_from_raw(
        raw: &RawDataStoreKey,
    ) -> Result<Self, DecodedDataStoreKeyDecodeError> {
        let decoded = DataStoreKey::try_from_raw_bytes(raw.as_bytes()).map_err(|source| {
            let _ = source;
            DecodedDataStoreKeyDecodeError::StoreKey
        })?;
        let entity = decoded.entity_tag();
        let key = decoded
            .primary_key()
            .decode()
            .map_err(PrimaryKeyValueDecodeError::from)?;

        Ok(Self::new_with_raw_primary_key_value(
            entity,
            &key,
            raw.clone(),
        ))
    }
}

pub(in crate::db) fn primary_key_value_from_structural_value(
    value: &Value,
) -> Result<PrimaryKeyValue, InternalError> {
    match value {
        Value::List(values) => composite_primary_key_value_from_structural_values(values),
        _ => primary_key_component_from_structural_value(value).map(PrimaryKeyValue::Scalar),
    }
}

fn composite_primary_key_value_from_structural_values(
    values: &[Value],
) -> Result<PrimaryKeyValue, InternalError> {
    let count = values.len();
    if count < 2 {
        return Err(
            PrimaryKeyEncodeError::from(CompositePrimaryKeyValueError::TooFewComponents {
                count,
                min: 2,
            })
            .into(),
        );
    }
    if count > MAX_PRIMARY_KEY_FIELDS {
        return Err(PrimaryKeyEncodeError::from(
            CompositePrimaryKeyValueError::TooManyComponents {
                count,
                max: MAX_PRIMARY_KEY_FIELDS,
            },
        )
        .into());
    }

    let mut components = [PrimaryKeyComponent::Unit; MAX_PRIMARY_KEY_FIELDS];
    for (index, value) in values.iter().enumerate() {
        components[index] = primary_key_component_from_structural_value(value)?;
    }

    let composite = CompositePrimaryKeyValue::try_from_components(&components[..count])
        .map_err(PrimaryKeyEncodeError::from)?;

    Ok(PrimaryKeyValue::Composite(composite))
}

fn primary_key_component_from_structural_value(
    value: &Value,
) -> Result<PrimaryKeyComponent, InternalError> {
    PrimaryKeyComponent::from_runtime_value(value).ok_or_else(InternalError::store_unsupported)
}

impl Clone for DecodedDataStoreKey {
    fn clone(&self) -> Self {
        let cache = OnceCell::new();
        if let Some(raw) = self.raw.get() {
            let _ = cache.set(raw.clone());
        }

        Self {
            entity: self.entity,
            key: self.key,
            raw: cache,
        }
    }
}

impl fmt::Debug for DecodedDataStoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecodedDataStoreKey")
            .field("entity", &self.entity)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl PartialEq for DecodedDataStoreKey {
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity && self.key == other.key
    }
}

impl Eq for DecodedDataStoreKey {}

impl PartialOrd for DecodedDataStoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecodedDataStoreKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entity
            .cmp(&other.entity)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl Hash for DecodedDataStoreKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.entity.hash(state);
        self.key.hash(state);
    }
}

impl Display for DecodedDataStoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} ({:?})", self.entity.value(), self.key)
    }
}

impl RawDataStoreKey {
    /// `EntityTag` binary-width contract for raw on-disk key framing.
    pub(in crate::db) const ENTITY_TAG_SIZE_BYTES: u64 = size_of::<u64>() as u64;
    #[cfg(test)]
    pub(in crate::db) const ENTITY_TAG_SIZE_USIZE: usize = Self::ENTITY_TAG_SIZE_BYTES as usize;

    /// Maximum compact on-disk size in bytes.
    pub(in crate::db) const MAX_STORED_SIZE_BYTES: u64 =
        Self::ENTITY_TAG_SIZE_BYTES + COMPOSITE_PRIMARY_KEY_MAX_SIZE as u64;

    /// Maximum compact in-memory key size (for bounded storable metadata).
    pub(in crate::db) const MAX_STORED_SIZE_USIZE: usize = Self::MAX_STORED_SIZE_BYTES as usize;

    #[must_use]
    pub(in crate::db) fn from_store_range_bound(bytes: &[u8]) -> Self {
        Self::from_persisted_bytes(bytes.to_vec())
    }

    #[must_use]
    pub(in crate::db) fn store_range_bounds(
        range: &RawDataStoreKeyRange,
    ) -> (RangeBound<Self>, RangeBound<Self>) {
        let lower = RangeBound::Included(Self::from_store_range_bound(range.lower_inclusive()));
        let upper = range
            .upper_exclusive()
            .map_or(RangeBound::Unbounded, |upper| {
                RangeBound::Excluded(Self::from_store_range_bound(upper))
            });

        (lower, upper)
    }

    #[must_use]
    pub(in crate::db) fn store_range_lower_key(range: &RawDataStoreKeyRange) -> Self {
        Self::from_store_range_bound(range.lower_inclusive())
    }
}

impl Storable for RawDataStoreKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self::from_persisted_bytes(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: Self::MAX_STORED_SIZE_BYTES as u32,
        is_fixed_size: false,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
