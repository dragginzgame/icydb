use crate::{
    MAX_INDEX_FIELDS,
    db::{identity::IndexName, index::key::IndexId},
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;

const KEY_KIND_TAG_SIZE: usize = 1;
const INDEX_VALUE_SIZE: usize = 16;
const VALUE_COUNT_SIZE: usize = 1;
const INDEX_ID_SIZE: usize = IndexName::STORED_SIZE_USIZE;
const KEY_PREFIX_SIZE: usize = KEY_KIND_TAG_SIZE + INDEX_ID_SIZE + VALUE_COUNT_SIZE;

///
/// IndexKeyKind
///
/// Encoded discriminator for index key families.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum IndexKeyKind {
    User = 0,
    System = 1,
}

impl IndexKeyKind {
    const USER_TAG: u8 = 0;
    const SYSTEM_TAG: u8 = 1;

    #[must_use]
    const fn tag(self) -> u8 {
        self as u8
    }

    const fn from_tag(tag: u8) -> Result<Self, &'static str> {
        match tag {
            Self::USER_TAG => Ok(Self::User),
            Self::SYSTEM_TAG => Ok(Self::System),
            _ => Err("corrupted IndexKey: invalid key kind"),
        }
    }
}

///
/// IndexKey
///
/// Fully-qualified index lookup key.
/// Variable-length, manually encoded structure designed for stable-memory ordering.
/// Ordering of this type must exactly match byte-level ordering.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexKey {
    pub(super) key_kind: IndexKeyKind,
    pub(super) index_id: IndexId,
    pub(super) len: u8,
    pub(super) values: Vec<[u8; INDEX_VALUE_SIZE]>,
}

#[expect(clippy::cast_possible_truncation)]
impl IndexKey {
    /// Maximum on-disk size in bytes (stable, protocol-level bound)
    pub const STORED_SIZE_BYTES: u64 =
        (KEY_PREFIX_SIZE + (MAX_INDEX_FIELDS * INDEX_VALUE_SIZE)) as u64;

    /// Maximum in-memory size (for bounds checks)
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    /// Minimum encoded size for an empty index key.
    pub const MIN_STORED_SIZE_BYTES: u64 = KEY_PREFIX_SIZE as u64;

    /// Minimum encoded size for an empty index key.
    pub const MIN_STORED_SIZE_USIZE: usize = Self::MIN_STORED_SIZE_BYTES as usize;

    #[must_use]
    pub fn to_raw(&self) -> RawIndexKey {
        let value_count = usize::from(self.len);

        debug_assert_eq!(value_count, self.values.len());
        debug_assert!(value_count <= MAX_INDEX_FIELDS);

        let mut bytes = Vec::with_capacity(KEY_PREFIX_SIZE + (value_count * INDEX_VALUE_SIZE));

        bytes.push(self.key_kind.tag());

        let name_bytes = self.index_id.0.to_bytes();
        bytes.extend_from_slice(&name_bytes);

        bytes.push(self.len);

        for value in self.values.iter().take(value_count) {
            bytes.extend_from_slice(value);
        }

        RawIndexKey(bytes)
    }

    pub fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
        let bytes = raw.as_bytes();
        if bytes.len() < Self::MIN_STORED_SIZE_USIZE || bytes.len() > Self::STORED_SIZE_USIZE {
            return Err("corrupted IndexKey: invalid size");
        }

        let mut offset = 0;

        let key_kind = IndexKeyKind::from_tag(bytes[offset])?;
        offset += KEY_KIND_TAG_SIZE;

        let index_name =
            IndexName::from_bytes(&bytes[offset..offset + IndexName::STORED_SIZE_USIZE])
                .map_err(|_| "corrupted IndexKey: invalid IndexName bytes")?;
        offset += INDEX_ID_SIZE;

        let len = bytes[offset];
        offset += VALUE_COUNT_SIZE;

        let len_usize = usize::from(len);
        if len_usize > MAX_INDEX_FIELDS {
            return Err("corrupted IndexKey: invalid index length");
        }

        let expected_size = KEY_PREFIX_SIZE + (len_usize * INDEX_VALUE_SIZE);
        if bytes.len() != expected_size {
            return Err("corrupted IndexKey: invalid size");
        }

        let mut values = Vec::with_capacity(len_usize);
        for _ in 0..len_usize {
            let mut value = [0u8; INDEX_VALUE_SIZE];
            value.copy_from_slice(&bytes[offset..offset + INDEX_VALUE_SIZE]);
            values.push(value);
            offset += INDEX_VALUE_SIZE;
        }

        Ok(Self {
            key_kind,
            index_id: IndexId(index_name),
            len,
            values,
        })
    }

    #[must_use]
    pub(crate) fn uses_system_namespace(&self) -> bool {
        self.key_kind == IndexKeyKind::System
    }
}

///
/// RawIndexKey
///
/// Variable-length, stable-memory representation of IndexKey.
/// This is the form stored in BTreeMap keys.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawIndexKey(Vec<u8>);

impl RawIndexKey {
    /// Borrow the raw byte representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Storable for RawIndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    #[expect(clippy::cast_possible_truncation)]
    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::STORED_SIZE_BYTES as u32,
        is_fixed_size: false,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{KEY_KIND_TAG_SIZE, KEY_PREFIX_SIZE};
    use crate::{
        MAX_INDEX_FIELDS,
        db::{
            identity::{EntityName, IndexName},
            index::{IndexId, IndexKey, IndexKeyKind, RawIndexKey},
        },
        traits::Storable,
    };
    use std::borrow::Cow;

    #[test]
    fn index_key_rejects_undersized_bytes() {
        let bytes = vec![0u8; IndexKey::MIN_STORED_SIZE_USIZE.saturating_sub(1)];
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
    fn index_key_rejects_invalid_kind_tag() {
        let key = IndexKey::empty(IndexId::max_storable());
        let raw = key.to_raw();

        let mut bytes = raw.as_bytes().to_vec();
        bytes[0] = 0xFF;

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("invalid kind should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn index_key_rejects_len_over_max() {
        let key = IndexKey::empty(IndexId::max_storable());
        let raw = key.to_raw();

        let len_offset = KEY_KIND_TAG_SIZE + IndexName::STORED_SIZE_BYTES as usize;
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
        bytes[KEY_KIND_TAG_SIZE] = 0;
        bytes[KEY_KIND_TAG_SIZE + 1] = 0;

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("invalid index name should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_trailing_bytes() {
        let key = IndexKey::empty(IndexId::max_storable());

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes.push(42);

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("trailing bytes should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_truncated_payload() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let index_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));
        let prefix = [[1u8; 16], [2u8; 16]];
        let (key, _) = IndexKey::bounds_for_prefix(index_id, 2, &prefix);

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes.pop();

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("truncated payload should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_overlong_payload_for_declared_cardinality() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let index_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));
        let prefix = [[3u8; 16]];
        let (key, _) = IndexKey::bounds_for_prefix(index_id, 1, &prefix);

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes.extend_from_slice(&[9u8; 16]);

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("overlong payload should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_prefix_bounds_are_isolated_between_user_and_system_kinds() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let index_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));
        let prefix = [[0x33u8; 16]];

        let (user_start, user_end) =
            IndexKey::bounds_for_prefix_with_kind(index_id, IndexKeyKind::User, 2, &prefix);
        let (system_start, system_end) =
            IndexKey::bounds_for_prefix_with_kind(index_id, IndexKeyKind::System, 2, &prefix);

        let user_start_raw = user_start.to_raw();
        let user_end_raw = user_end.to_raw();
        let system_start_raw = system_start.to_raw();
        let system_end_raw = system_end.to_raw();

        assert!(user_start_raw <= user_end_raw);
        assert!(system_start_raw <= system_end_raw);
        assert!(user_end_raw < system_start_raw);
    }

    #[test]
    fn index_key_ordering_is_stable_across_cardinality_transitions() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let index_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));
        let first = [7u8; 16];

        let (len_one_key, _) = IndexKey::bounds_for_prefix(index_id, 1, &[first]);
        let (len_two_key, _) = IndexKey::bounds_for_prefix(index_id, 2, &[first, [0u8; 16]]);

        assert!(len_one_key < len_two_key);
        assert!(len_one_key.to_raw() < len_two_key.to_raw());
    }

    #[test]
    fn index_key_roundtrip_supports_max_cardinality() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let index_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));

        let mut prefix = Vec::with_capacity(MAX_INDEX_FIELDS);
        for i in 0..MAX_INDEX_FIELDS {
            #[allow(clippy::cast_possible_truncation)]
            let byte = i as u8;
            prefix.push([byte; 16]);
        }

        let (key, _) = IndexKey::bounds_for_prefix(index_id, MAX_INDEX_FIELDS, &prefix);
        let raw = key.to_raw();
        let decoded = IndexKey::try_from_raw(&raw).expect("max-cardinality key should decode");

        assert_eq!(decoded, key);
        assert_eq!(decoded.to_raw().as_bytes(), raw.as_bytes());
    }

    #[test]
    fn index_key_ordering_matches_bytes() {
        fn make_key(
            kind: IndexKeyKind,
            index_id: &IndexId,
            value_count: u8,
            first: u8,
            second: u8,
        ) -> IndexKey {
            let value_count_usize = usize::from(value_count);

            let mut bytes = Vec::with_capacity(KEY_PREFIX_SIZE + (value_count_usize * 16));
            bytes.push(kind as u8);

            let name_bytes = index_id.0.to_bytes();
            bytes.extend_from_slice(&name_bytes);

            bytes.push(value_count);

            for i in 0..value_count_usize {
                let value = match i {
                    0 => [first; 16],
                    1 => [second; 16],
                    _ => [0; 16],
                };
                bytes.extend_from_slice(&value);
            }

            let raw = RawIndexKey::from_bytes(Cow::Borrowed(&bytes));
            IndexKey::try_from_raw(&raw).expect("valid key bytes should decode")
        }

        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let idx_a = IndexId(IndexName::try_from_parts(&entity, &["a"]).expect("index name"));
        let idx_b = IndexId(IndexName::try_from_parts(&entity, &["b"]).expect("index name"));

        let keys = vec![
            make_key(IndexKeyKind::User, &idx_a, 1, 1, 0),
            make_key(IndexKeyKind::User, &idx_a, 2, 1, 2),
            make_key(IndexKeyKind::User, &idx_a, 1, 2, 0),
            make_key(IndexKeyKind::System, &idx_a, 1, 0, 0),
            make_key(IndexKeyKind::User, &idx_b, 1, 0, 0),
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
        let size_span = IndexKey::STORED_SIZE_USIZE - IndexKey::MIN_STORED_SIZE_USIZE + 1;

        for _ in 0..RUNS {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let len = IndexKey::MIN_STORED_SIZE_USIZE + ((seed as usize) % size_span);

            let mut bytes = vec![0u8; len];
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

    #[test]
    fn index_key_kind_is_explicit() {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");

        let user_id = IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"));
        let user_key = IndexKey::empty_with_kind(user_id, IndexKeyKind::User);
        assert_eq!(user_key.to_raw().as_bytes()[0], IndexKeyKind::User as u8);
        assert!(!user_key.uses_system_namespace());

        let system_id = IndexId(IndexName::try_from_parts(&entity, &["~ri"]).expect("index name"));
        let system_key = IndexKey::empty_with_kind(system_id, IndexKeyKind::System);
        assert_eq!(
            system_key.to_raw().as_bytes()[0],
            IndexKeyKind::System as u8
        );
        assert!(system_key.uses_system_namespace());

        let namespace_only_id =
            IndexId(IndexName::try_from_parts(&entity, &["~ri_shadow"]).expect("index name"));
        let namespace_only_key = IndexKey::empty_with_kind(namespace_only_id, IndexKeyKind::User);
        assert_eq!(
            namespace_only_key.to_raw().as_bytes()[0],
            IndexKeyKind::User as u8
        );
        assert!(!namespace_only_key.uses_system_namespace());
    }
}
