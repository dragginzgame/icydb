//! Module: data::key
//! Responsibility: canonical entity-aware data-key encoding and decoding.
//! Does not own: row payload bytes, commit sequencing, or query semantics.
//! Boundary: data::store persists `RawDataStoreKey`; higher layers use `DecodedDataStoreKey`.

#![expect(clippy::cast_possible_truncation)]

use crate::{
    db::key_taxonomy::{
        CompactStoreKeyDecodeError, DataStoreKey, EncodedPrimaryKey, PrimaryKeyValue,
        RawDataStoreKey, RawDataStoreKeyRange,
    },
    error::InternalError,
    traits::{EntityKind, PrimaryKeyCodec, PrimaryKeyDecode, Storable},
    types::{Account, EntityTag},
    value::{
        StorageKey, StorageKeyDecodeError, StorageKeyEncodeError, Value,
        primary_key_value_from_runtime_value,
    },
};
use canic_cdk::structures::storable::Bound as StorableBound;
use std::{
    borrow::Cow,
    cell::OnceCell,
    fmt::{self, Display},
    hash::{Hash, Hasher},
    mem::size_of,
    ops::Bound as RangeBound,
};
use thiserror::Error as ThisError;

///
/// DecodedDataStoreKeyEncodeError
/// (serialize boundary)
///

#[derive(Debug, ThisError)]
enum DecodedDataStoreKeyEncodeError {
    #[error("compact data key encoding failed for {key}: {source}")]
    CompactKeyEncoding {
        key: DecodedDataStoreKey,
        source: crate::db::key_taxonomy::CompactPrimaryKeyEncodeError,
    },
}

impl From<DecodedDataStoreKeyEncodeError> for InternalError {
    fn from(err: DecodedDataStoreKeyEncodeError) -> Self {
        Self::serialize_unsupported(err.to_string())
    }
}

///
/// PrimaryKeyValueDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum PrimaryKeyValueDecodeError {
    #[error("invalid primary key encoding: {source}")]
    InvalidEncoding {
        #[source]
        source: StorageKeyDecodeError,
    },

    #[error("invalid compact primary key encoding: {source}")]
    InvalidCompactEncoding {
        #[source]
        source: crate::db::key_taxonomy::CompactPrimaryKeyDecodeError,
    },
}

impl From<StorageKeyDecodeError> for PrimaryKeyValueDecodeError {
    fn from(source: StorageKeyDecodeError) -> Self {
        Self::InvalidEncoding { source }
    }
}

impl From<crate::db::key_taxonomy::CompactPrimaryKeyDecodeError> for PrimaryKeyValueDecodeError {
    fn from(source: crate::db::key_taxonomy::CompactPrimaryKeyDecodeError) -> Self {
        Self::InvalidCompactEncoding { source }
    }
}

///
/// DecodedDataStoreKeyDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum DecodedDataStoreKeyDecodeError {
    #[error("invalid primary key")]
    Key(#[from] PrimaryKeyValueDecodeError),

    #[error("invalid data store key: {source}")]
    StoreKey {
        #[source]
        source: CompactStoreKeyDecodeError,
    },
}

///
/// DecodedDataStoreKey
///

pub(in crate::db) struct DecodedDataStoreKey {
    entity: EntityTag,
    key: StorageKey,
    raw: OnceCell<RawDataStoreKey>,
}

impl DecodedDataStoreKey {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Construct from runtime identity and key payload.
    #[must_use]
    pub(in crate::db) const fn new(entity: EntityTag, key: StorageKey) -> Self {
        Self {
            entity,
            key,
            raw: OnceCell::new(),
        }
    }

    /// Construct one data key while freezing the already-known raw on-disk
    /// representation alongside the decoded storage key.
    #[must_use]
    pub(in crate::db) fn new_with_raw(
        entity: EntityTag,
        key: StorageKey,
        raw: RawDataStoreKey,
    ) -> Self {
        let cache = OnceCell::new();
        let _ = cache.set(raw);

        Self {
            entity,
            key,
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
        K: PrimaryKeyCodec,
    {
        let key = key.to_primary_key_value()?;
        let key = scalar_storage_key_from_primary_key_value(&key)?;

        Ok(Self::new(entity, key))
    }

    /// Construct from one entity tag plus one structural planner key literal.
    ///
    /// This is the structural key-codec boundary used by execution paths that
    /// no longer carry typed entity keys.
    pub(in crate::db) fn try_from_structural_key(
        entity: EntityTag,
        key: &Value,
    ) -> Result<Self, InternalError> {
        let key = primary_key_value_from_runtime_value(key)?;

        Ok(Self::new(entity, key))
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
            return Err(InternalError::data_key_entity_mismatch(
                expected.value(),
                self.entity.value(),
            ));
        }

        <E::Key as PrimaryKeyDecode>::from_primary_key_value(&PrimaryKeyValue::from(self.key))
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity
    }

    #[must_use]
    pub(in crate::db) const fn storage_key(&self) -> StorageKey {
        self.key
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
        if let Some(raw) = self.raw.get() {
            return Ok(raw.clone());
        }

        self.to_raw_compact_key_error()
            .map_err(|err| {
                DecodedDataStoreKeyEncodeError::CompactKeyEncoding {
                    key: self.clone(),
                    source: err,
                }
                .into()
            })
            .inspect(|raw| {
                let _ = self.raw.set(raw.clone());
            })
    }

    /// Encode into compact on-disk representation, returning compact-key
    /// encode errors directly.
    pub(in crate::db) fn to_raw_compact_key_error(
        &self,
    ) -> Result<RawDataStoreKey, crate::db::key_taxonomy::CompactPrimaryKeyEncodeError> {
        let primary_key = EncodedPrimaryKey::encode(PrimaryKeyValue::from(self.key))?;
        let raw = DataStoreKey::new(self.entity, primary_key).to_raw();

        Ok(raw)
    }

    /// Encode into compact on-disk representation, retaining the historical
    /// direct storage-key error shape for callers that still report through
    /// storage-key encode boundaries.
    pub(in crate::db) fn to_raw_storage_key_error(
        &self,
    ) -> Result<RawDataStoreKey, StorageKeyEncodeError> {
        if let Some(raw) = self.raw.get() {
            return Ok(raw.clone());
        }

        self.key.to_bytes()?;
        let raw = self
            .to_raw_compact_key_error()
            .expect("storage-key encodable value must compact-encode");
        let _ = self.raw.set(raw.clone());

        Ok(raw)
    }

    /// Encode a raw data-store key from validated entity + storage-key parts.
    pub(in crate::db) fn raw_from_parts(
        entity: EntityTag,
        key: StorageKey,
    ) -> Result<RawDataStoreKey, StorageKeyEncodeError> {
        Self::new(entity, key).to_raw_storage_key_error()
    }

    pub(in crate::db) fn try_from_raw(
        raw: &RawDataStoreKey,
    ) -> Result<Self, DecodedDataStoreKeyDecodeError> {
        let decoded = DataStoreKey::try_from_raw_bytes(raw.as_bytes())
            .map_err(|source| DecodedDataStoreKeyDecodeError::StoreKey { source })?;
        let entity = decoded.entity_tag();
        let key = StorageKey::from(
            decoded
                .primary_key()
                .decode_component()
                .map_err(PrimaryKeyValueDecodeError::from)?,
        );

        Ok(Self::new_with_raw(entity, key, raw.clone()))
    }
}

fn scalar_storage_key_from_primary_key_value(
    key: &PrimaryKeyValue,
) -> Result<StorageKey, InternalError> {
    key.scalar_component().map(StorageKey::from).ok_or_else(|| {
        InternalError::store_unsupported(
            "composite primary keys are not routed through DecodedDataStoreKey yet",
        )
    })
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
            .then_with(|| PrimaryKeyValue::from(self.key).cmp(&PrimaryKeyValue::from(other.key)))
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
        Self::ENTITY_TAG_SIZE_BYTES + 1 + Account::STORED_SIZE as u64;

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
mod tests {
    use super::*;
    use crate::{
        error::{ErrorClass, ErrorOrigin, InternalError},
        traits::{KeyValueCodec, PrimaryKeyCodec, PrimaryKeyDecode},
        types::{Account, Principal, Subaccount, Timestamp, Ulid},
        value::{Value, primary_key_value_from_runtime_value},
    };
    use std::borrow::Cow;

    fn max_width_data_store_key_fixture() -> DecodedDataStoreKey {
        DecodedDataStoreKey::new(
            EntityTag::new(u64::MAX),
            StorageKey::Account(Account::from_parts(Principal::MAX, Some(Subaccount::MAX))),
        )
    }

    fn assert_constructor_equivalence<K>(entity: EntityTag, key: K)
    where
        K: KeyValueCodec + PrimaryKeyCodec + std::fmt::Debug,
    {
        let typed =
            DecodedDataStoreKey::try_from_typed_key(entity, &key).expect("typed key should encode");
        let structural = DecodedDataStoreKey::try_from_structural_key(entity, &key.to_key_value())
            .expect("structural key should encode");

        assert_eq!(
            typed, structural,
            "typed and structural data-key constructors must stay equivalent for {key:?}",
        );
    }

    fn assert_structural_dedup_matches_typed_dedup<K>(entity: EntityTag, keys: Vec<K>)
    where
        K: Clone + KeyValueCodec + PrimaryKeyCodec + Ord + std::fmt::Debug,
    {
        let mut typed_keys = keys.clone();
        typed_keys.sort();
        typed_keys.dedup();

        let mut typed_data_keys = typed_keys
            .iter()
            .map(|key| {
                DecodedDataStoreKey::try_from_typed_key(entity, key)
                    .expect("typed key should encode")
            })
            .collect::<Vec<_>>();
        typed_data_keys.sort();
        typed_data_keys.dedup();

        let mut structural_data_keys = keys
            .iter()
            .map(KeyValueCodec::to_key_value)
            .map(|key| {
                DecodedDataStoreKey::try_from_structural_key(entity, &key)
                    .expect("structural key should encode")
            })
            .collect::<Vec<_>>();
        structural_data_keys.sort();
        structural_data_keys.dedup();

        assert_eq!(
            structural_data_keys, typed_data_keys,
            "structural DecodedDataStoreKey dedup must match typed-key dedup semantics",
        );
    }

    fn assert_storage_key_roundtrip<K>(key: K)
    where
        K: Copy + Eq + std::fmt::Debug + PrimaryKeyCodec + PrimaryKeyDecode,
    {
        let primary_key_value = key.to_primary_key_value().expect("typed key should encode");
        let decoded =
            K::from_primary_key_value(&primary_key_value).expect("primary key should decode");

        assert_eq!(decoded, key);
    }

    fn taxonomy_range_contains_raw_key(
        range: &RawDataStoreKeyRange,
        key: &RawDataStoreKey,
    ) -> bool {
        key.as_bytes() >= range.lower_inclusive()
            && range
                .upper_exclusive()
                .is_none_or(|upper| key.as_bytes() < upper)
    }

    #[test]
    fn data_key_max_width_fixture_uses_max_compact_size() {
        let data_key = max_width_data_store_key_fixture();
        let size = data_key.to_raw().unwrap().as_bytes().len();
        assert_eq!(size, RawDataStoreKey::MAX_STORED_SIZE_USIZE);
    }

    #[test]
    fn data_key_golden_snapshot_entity_and_compact_primary_key_layout_is_stable() {
        let key = DecodedDataStoreKey::new(EntityTag::new(5), StorageKey::Int(-1));
        let raw = key.to_raw().expect("data key should encode");

        // Freeze the 0.159 on-disk wire contract:
        // [EntityTag(u64, big-endian)] + [EncodedPrimaryKey].
        let mut expected = Vec::new();
        expected.extend_from_slice(&5u64.to_be_bytes());
        expected.push(
            crate::db::key_taxonomy::PrimaryKeyComponent::Int(-1)
                .kind()
                .tag(),
        );
        expected.extend_from_slice(&0x7FFF_FFFF_FFFF_FFFFu64.to_be_bytes());

        assert_eq!(raw.as_bytes(), expected.as_slice());
    }

    #[test]
    fn data_key_ordering_matches_bytes() {
        let keys = vec![
            DecodedDataStoreKey::new(EntityTag::new(1), StorageKey::Int(0)),
            DecodedDataStoreKey::new(EntityTag::new(1), StorageKey::Int(0)),
            DecodedDataStoreKey::new(EntityTag::new(2), StorageKey::Int(0)),
            DecodedDataStoreKey::new(EntityTag::new(1), StorageKey::Nat(1)),
        ];

        let mut by_ord = keys.clone();
        by_ord.sort();

        let mut by_bytes = keys;
        by_bytes.sort_by(|a, b| {
            a.to_raw()
                .unwrap()
                .as_bytes()
                .cmp(b.to_raw().unwrap().as_bytes())
        });

        assert_eq!(by_ord, by_bytes);
    }

    #[test]
    fn data_key_structural_constructor_matches_typed_constructor() {
        let entity = EntityTag::new(17);

        assert_constructor_equivalence(entity, -42_i64);
        assert_constructor_equivalence(entity, 42_u64);
        assert_constructor_equivalence(entity, Principal::from_slice(&[1, 2, 3, 4]));
        assert_constructor_equivalence(entity, Subaccount::new([7; 32]));
        assert_constructor_equivalence(entity, Timestamp::from_millis(1_710_013_530_123));
        assert_constructor_equivalence(entity, Ulid::from_u128(42));
        assert_constructor_equivalence(
            entity,
            Account::from_parts(
                Principal::from_slice(&[9, 8, 7]),
                Some(Subaccount::new([5; 32])),
            ),
        );
        assert_constructor_equivalence(entity, ());
    }

    #[test]
    fn storage_key_decode_roundtrips_supported_typed_keys() {
        assert_storage_key_roundtrip(-42_i8);
        assert_storage_key_roundtrip(-43_i16);
        assert_storage_key_roundtrip(-44_i32);
        assert_storage_key_roundtrip(-45_i64);
        assert_storage_key_roundtrip(42_u8);
        assert_storage_key_roundtrip(43_u16);
        assert_storage_key_roundtrip(44_u32);
        assert_storage_key_roundtrip(45_u64);
        assert_storage_key_roundtrip(Principal::from_slice(&[1, 2, 3, 4]));
        assert_storage_key_roundtrip(Subaccount::new([7; 32]));
        assert_storage_key_roundtrip(Timestamp::from_millis(1_710_013_530_123));
        assert_storage_key_roundtrip(Ulid::from_u128(42));
        assert_storage_key_roundtrip(Account::from_parts(
            Principal::from_slice(&[9, 8, 7]),
            Some(Subaccount::new([5; 32])),
        ));
        assert_storage_key_roundtrip(());
    }

    #[test]
    fn primary_key_decode_rejects_variant_mismatch_and_out_of_range_keys() {
        let variant_err = u64::from_primary_key_value(&StorageKey::Int(7).into())
            .expect_err("nat decode must reject signed storage-key variants");
        let range_err = u8::from_primary_key_value(&StorageKey::Nat(300).into())
            .expect_err("narrow integer decode must reject out-of-range values");

        assert_eq!(variant_err.class(), ErrorClass::Corruption);
        assert_eq!(variant_err.origin(), ErrorOrigin::Store);
        assert_eq!(range_err.class(), ErrorClass::Corruption);
        assert_eq!(range_err.origin(), ErrorOrigin::Store);
    }

    #[test]
    fn data_key_constructors_reject_non_storage_key_values_consistently() {
        let entity = EntityTag::new(23);
        let unsupported_values = [
            Value::Text("not-a-storage-key".to_string()),
            Value::Bool(true),
            Value::List(vec![Value::Nat(1)]),
            Value::Null,
        ];

        for value in unsupported_values {
            let typed_err = InternalError::from(
                primary_key_value_from_runtime_value(&value)
                    .expect_err("runtime bridge must reject non-primary-key values"),
            );
            let structural_err = DecodedDataStoreKey::try_from_structural_key(entity, &value)
                .expect_err("structural constructor must reject non-primary-key values");

            assert_eq!(typed_err.class(), ErrorClass::Unsupported);
            assert_eq!(typed_err.origin(), ErrorOrigin::Serialize);
            assert_eq!(structural_err.class(), ErrorClass::Unsupported);
            assert_eq!(structural_err.origin(), ErrorOrigin::Serialize);
            assert_eq!(
                typed_err.class(),
                structural_err.class(),
                "typed and structural constructors must classify the same rejection for {value:?}",
            );
            assert_eq!(
                typed_err.origin(),
                structural_err.origin(),
                "typed and structural constructors must report the same rejection origin for {value:?}",
            );
        }
    }

    #[test]
    fn data_key_raw_prefix_bounds_cover_supported_structural_key_domain() {
        let entity = EntityTag::new(29);
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        let supported_values = [
            Value::Account(Account::from_parts(
                Principal::from_slice(&[3, 1, 4]),
                Some(Subaccount::new([1; 32])),
            )),
            Value::Int(-17),
            Value::Principal(Principal::from_slice(&[1, 2, 3])),
            Value::Subaccount(Subaccount::new([2; 32])),
            Value::Timestamp(Timestamp::from_secs(7)),
            Value::Nat(42),
            Value::Ulid(Ulid::from_u128(99)),
            Value::Unit,
        ];

        assert_eq!(
            range.lower_inclusive(),
            entity.value().to_be_bytes().as_slice()
        );
        assert_eq!(
            range.upper_exclusive().expect("ordinary entity has upper"),
            (entity.value() + 1).to_be_bytes().as_slice(),
        );

        for value in supported_values {
            let data_key = DecodedDataStoreKey::try_from_structural_key(entity, &value)
                .expect("supported structural key should encode");
            let raw_key = data_key.to_raw().expect("supported key should encode");
            assert!(
                taxonomy_range_contains_raw_key(&range, &raw_key),
                "supported structural key {value:?} must stay within entity bounds",
            );
        }
    }

    #[test]
    fn data_key_structural_dedup_matches_typed_key_dedup() {
        let entity = EntityTag::new(31);

        assert_structural_dedup_matches_typed_dedup(entity, vec![7_u64, 1, 7, 3, 1, 9]);
        assert_structural_dedup_matches_typed_dedup(
            entity,
            vec![
                Ulid::from_u128(9),
                Ulid::from_u128(1),
                Ulid::from_u128(9),
                Ulid::from_u128(2),
                Ulid::from_u128(1),
            ],
        );
    }

    #[test]
    fn data_key_entity_tag_roundtrip_is_big_endian() {
        let mut raw_bytes = max_width_data_store_key_fixture()
            .to_raw()
            .unwrap()
            .into_bytes();
        raw_bytes[..RawDataStoreKey::ENTITY_TAG_SIZE_USIZE]
            .copy_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        let raw = RawDataStoreKey::from_persisted_bytes(raw_bytes);
        let decoded =
            DecodedDataStoreKey::try_from_raw(&raw).expect("entity tag bytes should decode");
        assert_eq!(decoded.entity_tag().value(), 0x0102_0304_0506_0708u64);
    }

    #[test]
    fn data_key_rejects_corrupt_key() {
        let mut raw_bytes = max_width_data_store_key_fixture()
            .to_raw()
            .unwrap()
            .into_bytes();
        let off = RawDataStoreKey::ENTITY_TAG_SIZE_USIZE;
        raw_bytes[off] = 0xFF;
        let raw = RawDataStoreKey::from_persisted_bytes(raw_bytes);
        assert!(DecodedDataStoreKey::try_from_raw(&raw).is_err());
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn data_key_fuzz_roundtrip_is_canonical() {
        let mut seed = 0xDEAD_BEEF_u64;

        for _ in 0..1_000 {
            let mut bytes = [0u8; RawDataStoreKey::MAX_STORED_SIZE_USIZE];
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            let raw = RawDataStoreKey::from_persisted_bytes(bytes.to_vec());
            if let Ok(decoded) = DecodedDataStoreKey::try_from_raw(&raw) {
                let re = decoded.to_raw().unwrap();
                assert_eq!(raw.as_bytes(), re.as_bytes());
            }
        }
    }

    #[test]
    fn raw_data_store_key_storable_roundtrip() {
        let key = max_width_data_store_key_fixture().to_raw().unwrap();
        let bytes = key.to_bytes();
        let decoded = <RawDataStoreKey as Storable>::from_bytes(Cow::Borrowed(&bytes));
        assert_eq!(key, decoded);
    }

    #[test]
    fn raw_data_store_key_from_bytes_wrong_length_fails_closed() {
        let decoded = RawDataStoreKey::from_persisted_bytes(vec![1u8, 2u8, 3u8]);

        assert!(
            DecodedDataStoreKey::try_from_raw(&decoded).is_err(),
            "wrong-length raw bytes must not decode into a valid DecodedDataStoreKey"
        );
    }

    #[test]
    fn data_key_raw_entity_prefix_range_contains_only_matching_entity() {
        let entity = EntityTag::new(41);
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        let matching = DecodedDataStoreKey::new(entity, StorageKey::Nat(1))
            .to_raw()
            .expect("matching key should encode");
        let previous = DecodedDataStoreKey::new(EntityTag::new(40), StorageKey::Unit)
            .to_raw()
            .expect("previous key should encode");
        let next = DecodedDataStoreKey::new(EntityTag::new(42), StorageKey::Nat(0))
            .to_raw()
            .expect("next key should encode");

        assert!(taxonomy_range_contains_raw_key(&range, &matching));
        assert!(!taxonomy_range_contains_raw_key(&range, &previous));
        assert!(!taxonomy_range_contains_raw_key(&range, &next));
    }
}
