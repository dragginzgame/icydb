use crate::{
    MAX_INDEX_FIELDS,
    db::{identity::IndexName, index::key::IndexId},
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;

///
/// IndexKey
///
/// Fully-qualified index lookup key.
/// Fixed-size, manually encoded structure designed for stable-memory ordering.
/// Ordering of this type must exactly match byte-level ordering.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexKey {
    pub(super) index_id: IndexId,
    pub(super) len: u8,
    pub(super) values: [[u8; 16]; MAX_INDEX_FIELDS],
}

#[expect(clippy::cast_possible_truncation)]
impl IndexKey {
    /// Fixed on-disk size in bytes (stable, protocol-level)
    pub const STORED_SIZE_BYTES: u64 =
        IndexName::STORED_SIZE_BYTES + 1 + (MAX_INDEX_FIELDS as u64 * 16);

    /// Fixed in-memory size (for buffers and arrays)
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    #[must_use]
    pub fn to_raw(&self) -> RawIndexKey {
        let mut bytes = [0u8; Self::STORED_SIZE_USIZE];

        let name_bytes = self.index_id.0.to_bytes();
        bytes[..name_bytes.len()].copy_from_slice(&name_bytes);

        let mut offset = IndexName::STORED_SIZE_USIZE;
        bytes[offset] = self.len;
        offset += 1;

        for value in &self.values {
            bytes[offset..offset + 16].copy_from_slice(value);
            offset += 16;
        }

        RawIndexKey(bytes)
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
            if value.iter().any(|&byte| byte != 0) {
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        MAX_INDEX_FIELDS,
        db::{
            identity::{EntityName, IndexName},
            index::{IndexId, IndexKey, RawIndexKey},
        },
        traits::Storable,
    };
    use std::borrow::Cow;

    #[test]
    fn index_key_rejects_undersized_bytes() {
        let bytes = vec![0u8; IndexKey::STORED_SIZE_USIZE - 1];
        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("undersized key should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_oversized_bytes() {
        let bytes = vec![0u8; IndexKey::STORED_SIZE_USIZE + 1];
        let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("oversized key should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn index_key_rejects_len_over_max() {
        let key = IndexKey::empty(IndexId::max_storable());
        let raw = key.to_raw();
        let len_offset = IndexName::STORED_SIZE_BYTES as usize;
        let mut bytes = raw.as_bytes().to_vec();
        bytes[len_offset] = (MAX_INDEX_FIELDS as u8) + 1;
        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("oversized length should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_invalid_index_name() {
        let key = IndexKey::empty(IndexId::max_storable());
        let raw = key.to_raw();
        let mut bytes = raw.as_bytes().to_vec();
        bytes[0] = 0;
        bytes[1] = 0;
        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("invalid index name should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_fingerprint_padding() {
        let key = IndexKey::empty(IndexId::max_storable());
        let raw = key.to_raw();
        let values_offset = IndexName::STORED_SIZE_USIZE + 1;
        let mut bytes = raw.as_bytes().to_vec();
        bytes[values_offset] = 1;
        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("padding should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    #[expect(clippy::large_types_passed_by_value)]
    fn index_key_ordering_matches_bytes() {
        fn make_key(index_id: IndexId, value_count: u8, first: u8, second: u8) -> IndexKey {
            let mut bytes = [0u8; IndexKey::STORED_SIZE_USIZE];

            let name_bytes = index_id.0.to_bytes();
            bytes[..name_bytes.len()].copy_from_slice(&name_bytes);

            let mut offset = IndexName::STORED_SIZE_USIZE;
            bytes[offset] = value_count;
            offset += 1;

            let mut values = [[0u8; 16]; MAX_INDEX_FIELDS];
            values[0] = [first; 16];
            values[1] = [second; 16];

            for value in values {
                bytes[offset..offset + 16].copy_from_slice(&value);
                offset += 16;
            }

            let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
            IndexKey::try_from_raw(&raw).expect("valid key bytes should decode")
        }

        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let idx_a = IndexId(IndexName::try_from_parts(&entity, &["a"]).expect("index name"));
        let idx_b = IndexId(IndexName::try_from_parts(&entity, &["b"]).expect("index name"));

        let keys = vec![
            make_key(idx_a, 1, 1, 0),
            make_key(idx_a, 2, 1, 2),
            make_key(idx_a, 1, 2, 0),
            make_key(idx_b, 1, 0, 0),
        ];

        let mut sorted_by_ord = keys.clone();
        sorted_by_ord.sort();

        let mut sorted_by_bytes = keys;
        sorted_by_bytes.sort_by(|a, b| a.to_raw().as_bytes().cmp(b.to_raw().as_bytes()));

        assert_eq!(sorted_by_ord, sorted_by_bytes);
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn index_key_decode_fuzz_roundtrip_is_canonical() {
        const RUNS: u64 = 1_000;

        let mut seed = 0xBADC_0FFE_u64;
        for _ in 0..RUNS {
            let mut bytes = [0u8; IndexKey::STORED_SIZE_BYTES as usize];
            for byte in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *byte = (seed >> 24) as u8;
            }

            let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
            if let Ok(decoded) = IndexKey::try_from_raw(&raw) {
                let reencoded = decoded.to_raw();
                assert_eq!(raw.as_bytes(), reencoded.as_bytes());
            }
        }
    }
}
