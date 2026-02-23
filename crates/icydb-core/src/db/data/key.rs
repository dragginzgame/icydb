#![expect(clippy::cast_possible_truncation)]
use crate::{
    db::{
        data::storage_key::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError},
        identity::{EntityName, IdentityDecodeError},
    },
    error::InternalError,
    traits::{EntityKind, FieldValue, Storable},
};
use canic_cdk::structures::storable::Bound;
use std::{
    borrow::Cow,
    fmt::{self, Display},
};
use thiserror::Error as ThisError;

///
/// DataKeyEncodeError
/// (serialize boundary)
///

#[derive(Debug, ThisError)]
pub(crate) enum DataKeyEncodeError {
    #[error("data key encoding failed for {key}: {source}")]
    KeyEncoding {
        key: DataKey,
        source: StorageKeyEncodeError,
    },
}

impl From<DataKeyEncodeError> for InternalError {
    fn from(err: DataKeyEncodeError) -> Self {
        Self::serialize_unsupported(err.to_string())
    }
}

///
/// KeyDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug, ThisError)]
pub(crate) enum KeyDecodeError {
    #[error("invalid primary key encoding: {source}")]
    InvalidEncoding {
        #[source]
        source: StorageKeyDecodeError,
    },
}

impl From<StorageKeyDecodeError> for KeyDecodeError {
    fn from(source: StorageKeyDecodeError) -> Self {
        Self::InvalidEncoding { source }
    }
}

///
/// DataKeyDecodeError
/// (decode / corruption boundary)
///

#[derive(Debug, ThisError)]
pub(crate) enum DataKeyDecodeError {
    #[error("invalid entity name")]
    Entity(#[from] IdentityDecodeError),

    #[error("invalid primary key")]
    Key(#[from] KeyDecodeError),
}

///
/// DataKey
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DataKey {
    entity: EntityName,
    key: StorageKey,
}

impl DataKey {
    /// Fixed on-disk size in bytes (stable, protocol-level)
    pub(crate) const STORED_SIZE_BYTES: u64 =
        EntityName::STORED_SIZE_BYTES + StorageKey::STORED_SIZE_BYTES;

    /// Fixed in-memory size (for buffers and arrays only)
    pub(crate) const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Construct using compile-time entity metadata.
    ///
    /// This requires that the entity key is persistable.
    pub(crate) fn try_new<E>(key: E::Key) -> Result<Self, InternalError>
    where
        E: EntityKind,
    {
        let value = key.to_value();
        let key = StorageKey::try_from_value(&value)?;

        Ok(Self {
            entity: Self::entity_for::<E>(),
            key,
        })
    }

    /// Decode a raw entity key from this data key.
    ///
    /// This is a fallible boundary that validates entity identity and
    /// key compatibility against the target entity type.
    pub(crate) fn try_key<E>(&self) -> Result<E::Key, InternalError>
    where
        E: EntityKind,
    {
        let expected = Self::entity_for::<E>();
        if self.entity != expected {
            return Err(InternalError::store_corruption(format!(
                "data key entity mismatch: expected {}, found {}",
                expected, self.entity
            )));
        }

        let value = self.key.as_value();
        <E::Key as FieldValue>::from_value(&value).ok_or_else(|| {
            InternalError::store_corruption(format!(
                "data key primary key decode failed: {value:?}"
            ))
        })
    }

    /// Construct a DataKey from a raw StorageKey using entity metadata.
    #[must_use]
    pub(crate) fn from_key<E: EntityKind>(key: StorageKey) -> Self {
        Self {
            entity: Self::entity_for::<E>(),
            key,
        }
    }

    #[must_use]
    pub(crate) fn lower_bound<E>() -> Self
    where
        E: EntityKind,
    {
        Self {
            entity: Self::entity_for::<E>(),
            key: StorageKey::MIN,
        }
    }

    #[must_use]
    pub(crate) fn upper_bound<E>() -> Self
    where
        E: EntityKind,
    {
        Self {
            entity: Self::entity_for::<E>(),
            key: StorageKey::upper_bound(),
        }
    }

    fn entity_for<E: EntityKind>() -> EntityName {
        // INVARIANT:
        // `E::ENTITY_NAME` is compile-time schema/codegen metadata. Runtime
        // user input cannot influence this value.
        // A failure here is an internal model/codegen contract break.
        EntityName::try_from_str(E::ENTITY_NAME)
            .expect("invariant violation: invalid E::ENTITY_NAME (schema/codegen contract broken)")
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    #[must_use]
    pub(crate) const fn entity_name(&self) -> &EntityName {
        &self.entity
    }

    #[must_use]
    pub(crate) const fn storage_key(&self) -> StorageKey {
        self.key
    }

    /// Compute on-disk entry size from value length.
    #[must_use]
    pub(crate) const fn entry_size_bytes(value_len: u64) -> u64 {
        Self::STORED_SIZE_BYTES + value_len
    }

    #[must_use]
    #[cfg(test)]
    fn max_storable() -> Self {
        Self {
            entity: EntityName::max_storable(),
            key: StorageKey::max_storable(),
        }
    }

    // ------------------------------------------------------------------
    // Encoding / decoding
    // ------------------------------------------------------------------

    /// Encode into fixed-size on-disk representation.
    pub(crate) fn to_raw(&self) -> Result<RawDataKey, InternalError> {
        self.to_raw_storage_key_error().map_err(|err| {
            DataKeyEncodeError::KeyEncoding {
                key: self.clone(),
                source: err,
            }
            .into()
        })
    }

    /// Encode into fixed-size on-disk representation, returning storage-key encode errors directly.
    pub(crate) fn to_raw_storage_key_error(&self) -> Result<RawDataKey, StorageKeyEncodeError> {
        let mut buf = [0u8; Self::STORED_SIZE_USIZE];

        let entity_bytes = self.entity.to_bytes();
        buf[..EntityName::STORED_SIZE_USIZE].copy_from_slice(&entity_bytes);

        let key_bytes = self.key.to_bytes()?;

        let key_offset = EntityName::STORED_SIZE_USIZE;
        buf[key_offset..key_offset + StorageKey::STORED_SIZE_USIZE].copy_from_slice(&key_bytes);

        Ok(RawDataKey(buf))
    }

    /// Encode a raw data key from validated entity + storage-key parts.
    pub(crate) fn raw_from_parts(
        entity: EntityName,
        key: StorageKey,
    ) -> Result<RawDataKey, StorageKeyEncodeError> {
        Self { entity, key }.to_raw_storage_key_error()
    }

    pub(crate) fn try_from_raw(raw: &RawDataKey) -> Result<Self, DataKeyDecodeError> {
        let bytes = &raw.0;

        let entity = EntityName::from_bytes(&bytes[..EntityName::STORED_SIZE_USIZE])?;

        let key = StorageKey::try_from_bytes(&bytes[EntityName::STORED_SIZE_USIZE..])
            .map_err(KeyDecodeError::from)?;

        Ok(Self { entity, key })
    }
}

impl Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} ({})", self.entity, self.key)
    }
}

///
/// RawDataKey
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawDataKey([u8; DataKey::STORED_SIZE_USIZE]);

impl RawDataKey {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DataKey::STORED_SIZE_USIZE] {
        &self.0
    }
}

impl Storable for RawDataKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        // Fixed-size storable contract: callers must provide exact-sized bytes.
        // Fail closed on any mismatch so malformed payloads cannot materialize
        // into a potentially valid-looking key.
        debug_assert_eq!(
            bytes.len(),
            DataKey::STORED_SIZE_USIZE,
            "RawDataKey::from_bytes received unexpected byte length",
        );

        if bytes.len() != DataKey::STORED_SIZE_USIZE {
            return Self([0u8; DataKey::STORED_SIZE_USIZE]);
        }

        let mut out = [0u8; DataKey::STORED_SIZE_USIZE];
        out.copy_from_slice(bytes.as_ref());
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: DataKey::STORED_SIZE_BYTES as u32,
        is_fixed_size: true,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn data_key_is_exactly_fixed_size() {
        let data_key = DataKey::max_storable();
        let size = data_key.to_raw().unwrap().as_bytes().len();
        assert_eq!(size, DataKey::STORED_SIZE_USIZE);
    }

    #[test]
    fn data_key_ordering_matches_bytes() {
        let keys = vec![
            DataKey {
                entity: EntityName::try_from_str("a").unwrap(),
                key: StorageKey::Int(0),
            },
            DataKey {
                entity: EntityName::try_from_str("aa").unwrap(),
                key: StorageKey::Int(0),
            },
            DataKey {
                entity: EntityName::try_from_str("b").unwrap(),
                key: StorageKey::Int(0),
            },
            DataKey {
                entity: EntityName::try_from_str("a").unwrap(),
                key: StorageKey::Uint(1),
            },
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
    fn data_key_rejects_corrupt_entity() {
        let mut raw = DataKey::max_storable().to_raw().unwrap();
        raw.0[0] = 0;
        assert!(DataKey::try_from_raw(&raw).is_err());
    }

    #[test]
    fn data_key_rejects_corrupt_key() {
        let mut raw = DataKey::max_storable().to_raw().unwrap();
        let off = EntityName::STORED_SIZE_USIZE;
        raw.0[off] = 0xFF;
        assert!(DataKey::try_from_raw(&raw).is_err());
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn data_key_fuzz_roundtrip_is_canonical() {
        let mut seed = 0xDEAD_BEEF_u64;

        for _ in 0..1_000 {
            let mut bytes = [0u8; DataKey::STORED_SIZE_USIZE];
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            let raw = RawDataKey(bytes);
            if let Ok(decoded) = DataKey::try_from_raw(&raw) {
                let re = decoded.to_raw().unwrap();
                assert_eq!(raw.as_bytes(), re.as_bytes());
            }
        }
    }

    #[test]
    fn raw_data_key_storable_roundtrip() {
        let key = DataKey::max_storable().to_raw().unwrap();
        let bytes = key.to_bytes();
        let decoded = RawDataKey::from_bytes(Cow::Borrowed(&bytes));
        assert_eq!(key, decoded);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "RawDataKey::from_bytes received unexpected byte length")]
    fn raw_data_key_from_bytes_wrong_length_debug_asserts() {
        let _ = RawDataKey::from_bytes(Cow::Borrowed(&[1u8, 2u8, 3u8]));
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn raw_data_key_from_bytes_wrong_length_fails_closed() {
        let decoded = RawDataKey::from_bytes(Cow::Borrowed(&[1u8, 2u8, 3u8]));

        assert!(
            DataKey::try_from_raw(&decoded).is_err(),
            "wrong-length raw bytes must not decode into a valid DataKey"
        );
    }
}
