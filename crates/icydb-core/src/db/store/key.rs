use crate::{db::identity::EntityName, key::Key, traits::Storable};
use canic_cdk::structures::storable::Bound;
use std::{
    borrow::Cow,
    fmt::{self, Display},
};

///
/// DataKey
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataKey {
    entity: EntityName,
    key: Key,
}

impl DataKey {
    #[allow(clippy::cast_possible_truncation)]
    pub const STORED_SIZE: u32 = EntityName::STORED_SIZE + Key::STORED_SIZE as u32;

    #[must_use]
    /// Build a data key for the given entity type and primary key.
    pub fn new<E: crate::traits::EntityKind>(key: impl Into<Key>) -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: key.into(),
        }
    }

    #[must_use]
    pub const fn lower_bound<E: crate::traits::EntityKind>() -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: Key::lower_bound(),
        }
    }

    #[must_use]
    pub const fn upper_bound<E: crate::traits::EntityKind>() -> Self {
        Self {
            entity: EntityName::from_static(E::ENTITY_NAME),
            key: Key::upper_bound(),
        }
    }

    /// Return the primary key component of this data key.
    #[must_use]
    pub const fn key(&self) -> Key {
        self.key
    }

    /// Entity name (stable, compile-time constant per entity type).
    #[must_use]
    pub const fn entity_name(&self) -> &EntityName {
        &self.entity
    }

    /// Compute the on-disk size used by a single data entry from its value length.
    /// Includes the bounded `DataKey` size and the value bytes.
    #[must_use]
    pub const fn entry_size_bytes(value_len: u64) -> u64 {
        Self::STORED_SIZE as u64 + value_len
    }

    #[must_use]
    /// Max sentinel key for sizing.
    pub fn max_storable() -> Self {
        Self {
            entity: EntityName::max_storable(),
            key: Key::max_storable(),
        }
    }

    #[must_use]
    pub fn to_raw(&self) -> RawDataKey {
        let mut buf = [0u8; Self::STORED_SIZE as usize];

        buf[0] = self.entity.len;
        let entity_end = EntityName::STORED_SIZE_USIZE;
        buf[1..entity_end].copy_from_slice(&self.entity.bytes);

        let key_bytes = self.key.to_bytes();
        debug_assert_eq!(
            key_bytes.len(),
            Key::STORED_SIZE,
            "Key serialization must be exactly fixed-size"
        );
        let key_offset = EntityName::STORED_SIZE_USIZE;
        buf[key_offset..key_offset + Key::STORED_SIZE].copy_from_slice(&key_bytes);

        RawDataKey(buf)
    }

    pub fn try_from_raw(raw: &RawDataKey) -> Result<Self, &'static str> {
        let bytes = &raw.0;

        let mut offset = 0;
        let entity = EntityName::from_bytes(&bytes[offset..offset + EntityName::STORED_SIZE_USIZE])
            .map_err(|_| "corrupted DataKey: invalid EntityName bytes")?;
        offset += EntityName::STORED_SIZE_USIZE;

        let key = Key::try_from_bytes(&bytes[offset..offset + Key::STORED_SIZE])
            .map_err(|_| "corrupted DataKey: invalid Key bytes")?;

        Ok(Self { entity, key })
    }
}

impl Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} ({})", self.entity, self.key)
    }
}

impl From<DataKey> for Key {
    fn from(key: DataKey) -> Self {
        key.key()
    }
}

///
/// RawDataKey
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawDataKey([u8; DataKey::STORED_SIZE as usize]);

impl RawDataKey {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DataKey::STORED_SIZE as usize] {
        &self.0
    }
}

impl Storable for RawDataKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut out = [0u8; DataKey::STORED_SIZE as usize];
        if bytes.len() == out.len() {
            out.copy_from_slice(bytes.as_ref());
        }
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: DataKey::STORED_SIZE,
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
        let size = data_key.to_raw().as_bytes().len();

        assert_eq!(
            size,
            DataKey::STORED_SIZE as usize,
            "DataKey must serialize to exactly STORED_SIZE bytes"
        );
    }

    #[test]
    fn data_key_ordering_matches_bytes() {
        let keys = vec![
            DataKey {
                entity: EntityName::from_static("a"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("aa"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("b"),
                key: Key::Int(0),
            },
            DataKey {
                entity: EntityName::from_static("a"),
                key: Key::Uint(1),
            },
        ];

        let mut sorted_by_ord = keys.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = keys;
        sorted_by_bytes.sort_by(|a, b| a.to_raw().as_bytes().cmp(b.to_raw().as_bytes()));

        assert_eq!(
            sorted_by_ord, sorted_by_bytes,
            "DataKey Ord and byte ordering diverged"
        );
    }

    #[test]
    fn data_key_rejects_undersized_bytes() {
        let buf = vec![0u8; DataKey::STORED_SIZE as usize - 1];
        let raw = RawDataKey::from_bytes(Cow::Borrowed(&buf));
        let err = DataKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    fn data_key_rejects_oversized_bytes() {
        let buf = vec![0u8; DataKey::STORED_SIZE as usize + 1];
        let raw = RawDataKey::from_bytes(Cow::Borrowed(&buf));
        let err = DataKey::try_from_raw(&raw).unwrap_err();
        assert!(
            err.contains("corrupted"),
            "expected corruption error, got: {err}"
        );
    }

    #[test]
    fn data_key_rejects_invalid_entity_len() {
        let mut raw = DataKey::max_storable().to_raw();
        raw.0[0] = 0;
        assert!(DataKey::try_from_raw(&raw).is_err());
    }

    #[test]
    fn data_key_rejects_non_ascii_entity_bytes() {
        let data_key = DataKey {
            entity: EntityName::from_static("a"),
            key: Key::Int(1),
        };
        let mut raw = data_key.to_raw();
        raw.0[1] = 0xFF;
        assert!(DataKey::try_from_raw(&raw).is_err());
    }

    #[test]
    fn data_key_rejects_entity_padding() {
        let data_key = DataKey {
            entity: EntityName::from_static("user"),
            key: Key::Int(1),
        };
        let mut raw = data_key.to_raw();
        let padding_offset = 1 + data_key.entity.len();
        raw.0[padding_offset] = b'x';
        assert!(DataKey::try_from_raw(&raw).is_err());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn data_key_decode_fuzz_roundtrip_is_canonical() {
        const RUNS: u64 = 1_000;

        let mut seed = 0xDEAD_BEEF_u64;
        for _ in 0..RUNS {
            let mut bytes = [0u8; DataKey::STORED_SIZE as usize];
            for b in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *b = (seed >> 24) as u8;
            }

            let raw = RawDataKey(bytes);
            if let Ok(decoded) = DataKey::try_from_raw(&raw) {
                let re = decoded.to_raw();
                assert_eq!(
                    raw.as_bytes(),
                    re.as_bytes(),
                    "decoded DataKey must be canonical"
                );
            }
        }
    }
}
