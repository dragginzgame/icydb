use crate::{
    MAX_INDEX_FIELDS,
    db::{data::StorageKey, identity::IndexName, index::key::IndexId},
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;

const KEY_KIND_TAG_SIZE: usize = 1;
const COMPONENT_COUNT_SIZE: usize = 1;
const SEGMENT_LEN_SIZE: usize = 2;
const INDEX_ID_SIZE: usize = IndexName::STORED_SIZE_USIZE;
const KEY_PREFIX_SIZE: usize = KEY_KIND_TAG_SIZE + INDEX_ID_SIZE + COMPONENT_COUNT_SIZE;

///
/// IndexKeyKind
///
/// Encoded discriminator for index key families.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(crate) enum IndexKeyKind {
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
pub(crate) struct IndexKey {
    pub(super) key_kind: IndexKeyKind,
    pub(super) index_id: IndexId,
    pub(super) component_count: u8,
    pub(super) components: Vec<Vec<u8>>,
    pub(super) primary_key: Vec<u8>,
}

#[expect(clippy::cast_possible_truncation)]
impl IndexKey {
    pub(crate) const MAX_COMPONENT_SIZE: usize = 4 * 1024;
    pub(crate) const MAX_PK_SIZE: usize = StorageKey::STORED_SIZE_USIZE;

    const MIN_SEGMENT_SIZE: usize = 1;

    /// Maximum on-disk size in bytes (stable, protocol-level bound)
    pub(crate) const MAX_INDEX_KEY_BYTES: u64 = (KEY_PREFIX_SIZE
        + (MAX_INDEX_FIELDS * (SEGMENT_LEN_SIZE + Self::MAX_COMPONENT_SIZE))
        + (SEGMENT_LEN_SIZE + Self::MAX_PK_SIZE))
        as u64;

    /// Maximum on-disk size in bytes (stable, protocol-level bound)
    pub(crate) const STORED_SIZE_BYTES: u64 = Self::MAX_INDEX_KEY_BYTES;

    /// Maximum in-memory size (for bounds checks)
    pub(crate) const STORED_SIZE_USIZE: usize = Self::STORED_SIZE_BYTES as usize;

    /// Minimum encoded size for an empty index key.
    pub(crate) const MIN_STORED_SIZE_BYTES: u64 =
        (KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + Self::MIN_SEGMENT_SIZE) as u64;

    /// Minimum encoded size for an empty index key.
    pub(crate) const MIN_STORED_SIZE_USIZE: usize = Self::MIN_STORED_SIZE_BYTES as usize;

    #[must_use]
    pub(crate) fn to_raw(&self) -> RawIndexKey {
        let component_count = usize::from(self.component_count);

        debug_assert_eq!(component_count, self.components.len());
        debug_assert!(component_count <= MAX_INDEX_FIELDS);
        debug_assert!(!self.primary_key.is_empty());
        debug_assert!(self.primary_key.len() <= Self::MAX_PK_SIZE);

        let mut capacity = KEY_PREFIX_SIZE + SEGMENT_LEN_SIZE + self.primary_key.len();
        for component in self.components.iter().take(component_count) {
            debug_assert!(!component.is_empty());
            debug_assert!(component.len() <= Self::MAX_COMPONENT_SIZE);
            capacity += SEGMENT_LEN_SIZE + component.len();
        }

        let mut bytes = Vec::with_capacity(capacity);

        bytes.push(self.key_kind.tag());

        let name_bytes = self.index_id.0.to_bytes();
        bytes.extend_from_slice(&name_bytes);

        bytes.push(self.component_count);

        for component in self.components.iter().take(component_count) {
            push_segment(&mut bytes, component);
        }

        push_segment(&mut bytes, &self.primary_key);

        RawIndexKey(bytes)
    }

    pub(crate) fn try_from_raw(raw: &RawIndexKey) -> Result<Self, &'static str> {
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

        let component_count = bytes[offset];
        offset += COMPONENT_COUNT_SIZE;

        let component_count_usize = usize::from(component_count);
        if component_count_usize > MAX_INDEX_FIELDS {
            return Err("corrupted IndexKey: invalid index length");
        }

        let mut components = Vec::with_capacity(component_count_usize);
        for _ in 0..component_count_usize {
            let component = read_segment(
                bytes,
                &mut offset,
                Self::MAX_COMPONENT_SIZE,
                "component segment",
            )?;
            components.push(component.to_vec());
        }

        let primary_key = read_segment(bytes, &mut offset, Self::MAX_PK_SIZE, "primary key")?;
        if offset != bytes.len() {
            return Err("corrupted IndexKey: trailing bytes");
        }

        Ok(Self {
            key_kind,
            index_id: IndexId(index_name),
            component_count,
            components,
            primary_key: primary_key.to_vec(),
        })
    }

    #[must_use]
    pub(crate) fn uses_system_namespace(&self) -> bool {
        self.key_kind == IndexKeyKind::System
    }

    #[must_use]
    pub(in crate::db) const fn key_kind(&self) -> IndexKeyKind {
        self.key_kind
    }

    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> &IndexId {
        &self.index_id
    }

    #[must_use]
    pub(in crate::db) const fn component_count(&self) -> usize {
        self.component_count as usize
    }

    pub(in crate::db) fn primary_storage_key(&self) -> Result<StorageKey, &'static str> {
        StorageKey::try_from_bytes(&self.primary_key)
    }

    #[must_use]
    pub(crate) fn wildcard_low_component() -> Vec<u8> {
        vec![0]
    }

    #[must_use]
    pub(crate) fn wildcard_high_component() -> Vec<u8> {
        vec![0xFF; Self::MAX_COMPONENT_SIZE]
    }

    #[must_use]
    pub(crate) fn wildcard_low_pk() -> Vec<u8> {
        vec![0]
    }

    #[must_use]
    pub(crate) fn wildcard_high_pk() -> Vec<u8> {
        vec![0xFF; Self::MAX_PK_SIZE]
    }
}

fn push_segment(bytes: &mut Vec<u8>, segment: &[u8]) {
    let Ok(len_u16) = u16::try_from(segment.len()) else {
        unreachable!("segment length overflowed u16 despite bounded invariants")
    };

    bytes.extend_from_slice(&len_u16.to_be_bytes());
    bytes.extend_from_slice(segment);
}

fn read_segment<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    max_len: usize,
    label: &str,
) -> Result<&'a [u8], &'static str> {
    if *offset + SEGMENT_LEN_SIZE > bytes.len() {
        return Err("corrupted IndexKey: truncated key");
    }

    let mut len_buf = [0u8; SEGMENT_LEN_SIZE];
    len_buf.copy_from_slice(&bytes[*offset..*offset + SEGMENT_LEN_SIZE]);
    *offset += SEGMENT_LEN_SIZE;

    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err("corrupted IndexKey: zero-length segment");
    }
    if len > max_len {
        return Err("corrupted IndexKey: overlong segment");
    }

    let end = (*offset)
        .checked_add(len)
        .ok_or("corrupted IndexKey: segment overflow")?;
    if end > bytes.len() {
        return Err("corrupted IndexKey: truncated key");
    }

    let out = &bytes[*offset..end];
    *offset = end;

    let _ = label;
    Ok(out)
}

///
/// RawIndexKey
///
/// Variable-length, stable-memory representation of IndexKey.
/// This is the form stored in BTreeMap keys.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RawIndexKey(Vec<u8>);

impl RawIndexKey {
    /// Borrow the raw byte representation.
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
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
    use super::{KEY_KIND_TAG_SIZE, KEY_PREFIX_SIZE, SEGMENT_LEN_SIZE};
    use crate::{
        MAX_INDEX_FIELDS,
        db::{
            identity::{EntityName, IndexName},
            index::{IndexId, IndexKey, IndexKeyKind, RawIndexKey},
        },
        traits::Storable,
        types::{Decimal, Float32, Float64, Int, Principal},
        value::{Value, ValueEnum},
    };
    use std::{borrow::Cow, cmp::Ordering, ops::Bound as RangeBound};

    fn index_id() -> IndexId {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        IndexId(IndexName::try_from_parts(&entity, &["email"]).expect("index name"))
    }

    fn other_index_id() -> IndexId {
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        IndexId(IndexName::try_from_parts(&entity, &["name"]).expect("index name"))
    }

    fn encode_component(value: &Value) -> Vec<u8> {
        super::super::ordered::encode_canonical_index_component(value)
            .expect("component should encode")
    }

    #[expect(clippy::cast_possible_truncation)]
    #[expect(clippy::large_types_passed_by_value)]
    fn key_with(
        kind: IndexKeyKind,
        id: IndexId,
        components: Vec<Vec<u8>>,
        pk: Vec<u8>,
    ) -> IndexKey {
        IndexKey {
            key_kind: kind,
            index_id: id,
            component_count: components.len() as u8,
            components,
            primary_key: pk,
        }
    }

    fn expected_index_id_entity_email_bytes() -> Vec<u8> {
        // Intentionally protocol-freezing the fixed IndexName byte layout.
        let mut out = vec![
            0x00, 0x0C, b'e', b'n', b't', b'i', b't', b'y', b'|', b'e', b'm', b'a', b'i', b'l',
        ];
        out.extend_from_slice(&[0u8; 312]);
        out
    }

    fn decode_must_fail_corrupted(bytes: Vec<u8>, label: &str) {
        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err(label);
        assert!(
            err.contains("corrupted"),
            "decode error should classify corruption, got: {err}"
        );
    }

    fn make_component(byte: u8) -> Vec<u8> {
        vec![byte; 3]
    }

    fn make_pk(byte: u8) -> Vec<u8> {
        vec![byte; IndexKey::MAX_PK_SIZE]
    }

    fn next_random_u64(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    fn mixed_component_value(seed: u64, slot: u8) -> Value {
        let selector = seed % 4;
        match slot {
            0 => match selector {
                0 => Value::Int(-7),
                1 => Value::Int(-2),
                2 => Value::Int(0),
                _ => Value::Int(7),
            },
            1 => match selector {
                0 => Value::Text("aa".to_string()),
                1 => Value::Text("ab".to_string()),
                2 => Value::Text("mm".to_string()),
                _ => Value::Text("zz".to_string()),
            },
            2 => match selector {
                0 => Value::Int(-9),
                1 => Value::Int(-1),
                2 => Value::Int(1),
                _ => Value::Int(9),
            },
            3 => match selector {
                0 => Value::Text("ka".to_string()),
                1 => Value::Text("kb".to_string()),
                2 => Value::Text("mb".to_string()),
                _ => Value::Text("zz".to_string()),
            },
            _ => unreachable!("randomized mixed-composite fixture uses exactly four slots"),
        }
    }

    fn len_offset() -> usize {
        KEY_KIND_TAG_SIZE + IndexName::STORED_SIZE_USIZE
    }

    fn first_component_len_offset() -> usize {
        KEY_PREFIX_SIZE
    }

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
    fn index_key_rejects_unknown_kind_tag() {
        let key = IndexKey::empty(&index_id());
        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes[0] = 0xFF;

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("unknown kind tag should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn index_key_rejects_len_over_max() {
        let key = IndexKey {
            key_kind: IndexKeyKind::User,
            index_id: index_id(),
            component_count: 1,
            components: vec![make_component(1)],
            primary_key: make_pk(2),
        };

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes[len_offset()] = (MAX_INDEX_FIELDS as u8) + 1;

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("oversized length should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_invalid_index_name() {
        let key = IndexKey::empty(&IndexId::max_storable());
        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes[KEY_KIND_TAG_SIZE] = 0;
        bytes[KEY_KIND_TAG_SIZE + 1] = 0;

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("invalid index name should fail");
        assert!(err.contains("corrupted"));
    }

    #[test]
    fn index_key_rejects_truncated_key() {
        let key = IndexKey {
            key_kind: IndexKeyKind::User,
            index_id: index_id(),
            component_count: 1,
            components: vec![make_component(1)],
            primary_key: make_pk(2),
        };

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes.pop();

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("truncated payload should fail");
        assert!(err.contains("truncated"));
    }

    #[test]
    fn index_key_rejects_overlong_key_segments() {
        let key = IndexKey {
            key_kind: IndexKeyKind::User,
            index_id: index_id(),
            component_count: 1,
            components: vec![make_component(3)],
            primary_key: make_pk(4),
        };

        let mut bytes = key.to_raw().as_bytes().to_vec();
        let offset = first_component_len_offset();

        #[expect(clippy::cast_possible_truncation)]
        let overlong = (IndexKey::MAX_COMPONENT_SIZE + 1) as u16;
        bytes[offset..offset + SEGMENT_LEN_SIZE].copy_from_slice(&overlong.to_be_bytes());

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("overlong payload should fail");
        assert!(err.contains("overlong"));
    }

    #[test]
    fn index_key_rejects_trailing_bytes() {
        let key = IndexKey {
            key_kind: IndexKeyKind::User,
            index_id: index_id(),
            component_count: 1,
            components: vec![make_component(3)],
            primary_key: make_pk(4),
        };

        let mut bytes = key.to_raw().as_bytes().to_vec();
        bytes.push(42);

        let raw = RawIndexKey::from_bytes(Cow::Owned(bytes));
        let err = IndexKey::try_from_raw(&raw).expect_err("trailing bytes should fail");
        assert!(err.contains("trailing"));
    }

    #[test]
    fn index_key_prefix_bounds_are_isolated_between_user_and_system_kinds() {
        let prefix = vec![vec![0x33u8, 0x44, 0x55]];

        let (user_start, user_end) =
            IndexKey::bounds_for_prefix_with_kind(&index_id(), IndexKeyKind::User, 2, &prefix);
        let (system_start, system_end) =
            IndexKey::bounds_for_prefix_with_kind(&index_id(), IndexKeyKind::System, 2, &prefix);

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
        let first = vec![7u8, 7u8, 7u8];

        let (len_one_key, _) =
            IndexKey::bounds_for_prefix(&index_id(), 1, std::slice::from_ref(&first));
        let (len_two_key, _) =
            IndexKey::bounds_for_prefix(&index_id(), 2, &[first, vec![0u8, 0u8, 0u8]]);

        assert!(len_one_key < len_two_key);
        assert!(len_one_key.to_raw() < len_two_key.to_raw());
    }

    #[test]
    fn index_key_roundtrip_supports_max_cardinality() {
        let mut prefix = Vec::with_capacity(MAX_INDEX_FIELDS);
        for i in 0..MAX_INDEX_FIELDS {
            #[expect(clippy::cast_possible_truncation)]
            let byte = i as u8;
            prefix.push(vec![byte, byte.wrapping_add(1), byte.wrapping_add(2)]);
        }

        let (key, _) = IndexKey::bounds_for_prefix(&index_id(), MAX_INDEX_FIELDS, &prefix);
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
            components: Vec<Vec<u8>>,
            pk: Vec<u8>,
        ) -> IndexKey {
            #[expect(clippy::cast_possible_truncation)]
            let component_count = components.len() as u8;
            IndexKey {
                key_kind: kind,
                index_id: *index_id,
                component_count,
                components,
                primary_key: pk,
            }
        }

        let idx_a = index_id();
        let entity = EntityName::try_from_str("entity").expect("entity name should parse");
        let idx_b = IndexId(IndexName::try_from_parts(&entity, &["name"]).expect("index name"));

        let keys = vec![
            make_key(
                IndexKeyKind::User,
                &idx_a,
                vec![vec![1u8, 1u8], vec![2u8, 2u8]],
                make_pk(1),
            ),
            make_key(
                IndexKeyKind::User,
                &idx_a,
                vec![vec![1u8, 1u8], vec![3u8, 3u8]],
                make_pk(1),
            ),
            make_key(IndexKeyKind::User, &idx_a, vec![vec![2u8, 2u8]], make_pk(1)),
            make_key(
                IndexKeyKind::System,
                &idx_a,
                vec![vec![0u8, 0u8]],
                make_pk(1),
            ),
            make_key(IndexKeyKind::User, &idx_b, vec![vec![0u8, 0u8]], make_pk(1)),
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
        let user_key = IndexKey::empty_with_kind(&user_id, IndexKeyKind::User);
        assert_eq!(user_key.to_raw().as_bytes()[0], IndexKeyKind::User as u8);
        assert!(!user_key.uses_system_namespace());

        let system_id = IndexId(IndexName::try_from_parts(&entity, &["~ri"]).expect("index name"));
        let system_key = IndexKey::empty_with_kind(&system_id, IndexKeyKind::System);
        assert_eq!(
            system_key.to_raw().as_bytes()[0],
            IndexKeyKind::System as u8
        );
        assert!(system_key.uses_system_namespace());

        let namespace_only_id =
            IndexId(IndexName::try_from_parts(&entity, &["~ri_shadow"]).expect("index name"));
        let namespace_only_key = IndexKey::empty_with_kind(&namespace_only_id, IndexKeyKind::User);
        assert_eq!(
            namespace_only_key.to_raw().as_bytes()[0],
            IndexKeyKind::User as u8
        );
        assert!(!namespace_only_key.uses_system_namespace());
    }

    #[test]
    fn index_key_golden_snapshot_user_single_decimal_normalized() {
        let key = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![encode_component(&Value::Decimal(Decimal::new(10, 1)))],
            vec![0xAA, 0xBB, 0xCC],
        );

        let mut expected = vec![IndexKeyKind::User as u8];
        expected.extend_from_slice(&expected_index_id_entity_email_bytes());
        expected.push(1);
        expected.extend_from_slice(&[0x00, 0x09]);
        expected.extend_from_slice(&[0x05, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x31]);
        expected.extend_from_slice(&[0x00, 0x03, 0xAA, 0xBB, 0xCC]);

        assert_eq!(key.to_raw().as_bytes(), expected.as_slice());
    }

    #[test]
    fn index_key_golden_snapshot_system_two_component_text_and_identifier() {
        let key = key_with(
            IndexKeyKind::System,
            index_id(),
            vec![
                encode_component(&Value::Text("alpha".to_string())),
                encode_component(&Value::Principal(Principal::from_slice(&[1u8, 2, 3]))),
            ],
            vec![0x10],
        );

        let mut expected = vec![IndexKeyKind::System as u8];
        expected.extend_from_slice(&expected_index_id_entity_email_bytes());
        expected.push(2);
        expected.extend_from_slice(&[0x00, 0x08]);
        expected.extend_from_slice(&[0x14, b'a', b'l', b'p', b'h', b'a', 0x00, 0x00]);
        expected.extend_from_slice(&[0x00, 0x06]);
        expected.extend_from_slice(&[0x12, 0x01, 0x02, 0x03, 0x00, 0x00]);
        expected.extend_from_slice(&[0x00, 0x01, 0x10]);

        assert_eq!(key.to_raw().as_bytes(), expected.as_slice());
    }

    #[test]
    fn index_key_golden_snapshot_user_max_cardinality_mixed_components() {
        assert_eq!(
            MAX_INDEX_FIELDS, 4,
            "golden snapshot freezes the v0.10 max-cardinality contract"
        );

        let key = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                encode_component(&Value::Float64(
                    Float64::try_new(0.0).expect("finite float should construct"),
                )),
                encode_component(&Value::IntBig(Int::from(999i32))),
                encode_component(&Value::IntBig(Int::from(-7i32))),
                encode_component(&Value::Enum(
                    ValueEnum::new("State", Some("MyPath")).with_payload(Value::Int(7)),
                )),
            ],
            vec![0x42, 0x43],
        );

        let mut expected = vec![IndexKeyKind::User as u8];
        expected.extend_from_slice(&expected_index_id_entity_email_bytes());
        expected.push(0x04);
        expected.extend_from_slice(&[0x00, 0x09]);
        expected.extend_from_slice(&[0x0B, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        expected.extend_from_slice(&[0x00, 0x07]);
        expected.extend_from_slice(&[0x0E, 0x02, 0x00, 0x03, b'9', b'9', b'9']);
        expected.extend_from_slice(&[0x00, 0x05]);
        expected.extend_from_slice(&[0x0E, 0x00, 0xFF, 0xFE, 0xC8]);
        expected.extend_from_slice(&[0x00, 0x1D]);
        expected.extend_from_slice(&[
            0x07, b'S', b't', b'a', b't', b'e', 0x00, 0x00, 0x01, b'M', b'y', b'P', b'a', b't',
            b'h', 0x00, 0x00, 0x01, 0x00, 0x09, 0x0C, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x07,
        ]);
        expected.extend_from_slice(&[0x00, 0x02, 0x42, 0x43]);

        assert_eq!(key.to_raw().as_bytes(), expected.as_slice());
    }

    #[test]
    fn index_key_component_boundary_corruption_is_rejected() {
        let key = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![vec![0xA1, 0xA2, 0xA3, 0xA4], vec![0xB1, 0xB2, 0xB3]],
            vec![0xCC, 0xDD, 0xEE],
        );
        let base = key.to_raw().as_bytes().to_vec();

        let first_len_offset = KEY_PREFIX_SIZE;
        let first_payload_offset = first_len_offset + SEGMENT_LEN_SIZE;
        let second_len_offset = first_payload_offset + 4;
        let second_payload_offset = second_len_offset + SEGMENT_LEN_SIZE;
        let pk_len_offset = second_payload_offset + 3;

        let mut shorter_first = base.clone();
        shorter_first[first_len_offset..first_len_offset + SEGMENT_LEN_SIZE]
            .copy_from_slice(&(3u16).to_be_bytes());
        decode_must_fail_corrupted(
            shorter_first,
            "shortened component length should reject as corrupted",
        );

        let mut longer_first = base.clone();
        longer_first[first_len_offset..first_len_offset + SEGMENT_LEN_SIZE]
            .copy_from_slice(&(5u16).to_be_bytes());
        decode_must_fail_corrupted(
            longer_first,
            "lengthened component length should reject as corrupted",
        );

        let mut middle_byte_removed = base.clone();
        middle_byte_removed.remove(first_payload_offset + 1);
        decode_must_fail_corrupted(
            middle_byte_removed,
            "missing middle component byte should reject as corrupted",
        );

        let mut truncated_pk_len = base.clone();
        truncated_pk_len.remove(pk_len_offset);
        decode_must_fail_corrupted(
            truncated_pk_len,
            "truncated pk length segment should reject as corrupted",
        );

        let mut trailing = base;
        trailing.push(0x99);
        decode_must_fail_corrupted(trailing, "trailing bytes should reject as corrupted");
    }

    #[test]
    fn index_key_ordering_cartesian_semantic_vs_bytes() {
        #[derive(Clone)]
        struct Fixture {
            key: IndexKey,
            values: Vec<Value>,
            pk: Vec<u8>,
        }

        let numerics = [Value::Int(-2), Value::Int(7)];
        let texts = [Value::Text("aa".to_string()), Value::Text("zz".to_string())];
        let decimals = [
            Value::Decimal(Decimal::new(10, 1)),
            Value::Decimal(Decimal::new(11, 1)),
        ];
        let enums = [
            Value::Enum(ValueEnum::new("A", Some("EnumPath"))),
            Value::Enum(ValueEnum::new("B", Some("EnumPath")).with_payload(Value::Int(1))),
        ];

        let mut fixtures = Vec::new();
        let mut ordinal = 0u8;
        for numeric in numerics {
            for text in &texts {
                for decimal in &decimals {
                    for enum_value in &enums {
                        let values = vec![
                            numeric.clone(),
                            text.clone(),
                            decimal.clone(),
                            enum_value.clone(),
                        ];
                        let components = values.iter().map(encode_component).collect::<Vec<_>>();
                        let pk = vec![ordinal];
                        ordinal = ordinal.wrapping_add(1);

                        fixtures.push(Fixture {
                            key: key_with(IndexKeyKind::User, index_id(), components, pk.clone()),
                            values,
                            pk,
                        });
                    }
                }
            }
        }

        let mut semantic_sorted = fixtures.clone();
        semantic_sorted.sort_by(|left, right| {
            for (left_value, right_value) in left.values.iter().zip(&right.values) {
                let cmp = Value::canonical_cmp_key(left_value, right_value);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            left.pk.cmp(&right.pk)
        });

        let mut sorted_by_ord = fixtures.clone();
        sorted_by_ord.sort_by(|left, right| left.key.cmp(&right.key));

        let mut sorted_by_bytes = fixtures;
        sorted_by_bytes.sort_by(|left, right| {
            left.key
                .to_raw()
                .as_bytes()
                .cmp(right.key.to_raw().as_bytes())
        });

        let semantic_bytes = semantic_sorted
            .iter()
            .map(|fixture| fixture.key.to_raw().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let ord_bytes = sorted_by_ord
            .iter()
            .map(|fixture| fixture.key.to_raw().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let raw_bytes = sorted_by_bytes
            .iter()
            .map(|fixture| fixture.key.to_raw().as_bytes().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(ord_bytes, raw_bytes);
        assert_eq!(semantic_bytes, raw_bytes);
    }

    #[test]
    fn index_key_ordering_randomized_mixed_composite_semantic_vs_bytes() {
        #[derive(Clone)]
        struct Fixture {
            key: IndexKey,
            values: Vec<Value>,
            pk: Vec<u8>,
        }

        const SAMPLE_COUNT: usize = 256;
        const COMPONENT_COUNT: usize = 4;

        let mut fixtures = Vec::with_capacity(SAMPLE_COUNT);
        let mut state = 0xA11C_E5ED_0BAD_5EEDu64;

        for ordinal in 0..SAMPLE_COUNT {
            let values = (0..COMPONENT_COUNT)
                .map(|slot| {
                    mixed_component_value(
                        next_random_u64(&mut state),
                        u8::try_from(slot).expect("component slot should fit u8"),
                    )
                })
                .collect::<Vec<_>>();
            let components = values.iter().map(encode_component).collect::<Vec<_>>();
            let pk = u16::try_from(ordinal)
                .expect("sample ordinal should fit u16")
                .to_be_bytes()
                .to_vec();

            fixtures.push(Fixture {
                key: key_with(IndexKeyKind::User, index_id(), components, pk.clone()),
                values,
                pk,
            });
        }

        let mut semantic_sorted = fixtures.clone();
        semantic_sorted.sort_by(|left, right| {
            for (left_value, right_value) in left.values.iter().zip(&right.values) {
                let cmp = Value::canonical_cmp_key(left_value, right_value);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            left.pk.cmp(&right.pk)
        });

        let mut byte_sorted = fixtures;
        byte_sorted.sort_by(|left, right| {
            left.key
                .to_raw()
                .as_bytes()
                .cmp(right.key.to_raw().as_bytes())
        });

        let semantic_bytes = semantic_sorted
            .iter()
            .map(|fixture| fixture.key.to_raw().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let raw_bytes = byte_sorted
            .iter()
            .map(|fixture| fixture.key.to_raw().as_bytes().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(semantic_bytes, raw_bytes);
    }

    #[test]
    fn index_key_prefix_scan_simulation_matches_expected_and_is_isolated() {
        let first_component = encode_component(&Value::Text("alpha".to_string()));
        let second_component_low = encode_component(&Value::Int(1));
        let second_component_high = encode_component(&Value::Int(2));
        let other_first_component = encode_component(&Value::Text("beta".to_string()));

        let user_a = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![first_component.clone(), second_component_low.clone()],
            vec![0x01],
        );
        let user_b = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![first_component.clone(), second_component_low],
            vec![0x02],
        );
        let user_c = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![first_component.clone(), second_component_high],
            vec![0x03],
        );
        let user_other_component = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![other_first_component, encode_component(&Value::Int(1))],
            vec![0x04],
        );
        let system_same_prefix = key_with(
            IndexKeyKind::System,
            index_id(),
            vec![first_component.clone(), encode_component(&Value::Int(1))],
            vec![0x05],
        );
        let user_other_index = key_with(
            IndexKeyKind::User,
            other_index_id(),
            vec![first_component.clone(), encode_component(&Value::Int(1))],
            vec![0x06],
        );

        let all_keys = [
            user_a.clone(),
            user_b.clone(),
            user_c.clone(),
            user_other_component,
            system_same_prefix,
            user_other_index,
        ];
        let all_raw = all_keys.iter().map(IndexKey::to_raw).collect::<Vec<_>>();

        let (start, end) = IndexKey::bounds_for_prefix(&index_id(), 2, &[first_component]);
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());
        let mut matched = all_raw
            .iter()
            .filter(|raw| **raw >= start_raw && **raw <= end_raw)
            .map(|raw| raw.as_bytes().to_vec())
            .collect::<Vec<_>>();
        matched.sort();

        let mut expected = vec![
            user_a.to_raw().as_bytes().to_vec(),
            user_b.to_raw().as_bytes().to_vec(),
            user_c.to_raw().as_bytes().to_vec(),
        ];
        expected.sort();

        assert_eq!(matched, expected);
    }

    #[test]
    fn index_key_pk_terminal_tie_break_and_prefix_visibility() {
        let components = vec![
            encode_component(&Value::Text("dup".to_string())),
            encode_component(&Value::Int(9)),
        ];

        let lower_pk = key_with(
            IndexKeyKind::User,
            index_id(),
            components.clone(),
            vec![0x00, 0x01],
        );
        let higher_pk = key_with(IndexKeyKind::User, index_id(), components, vec![0x00, 0xFF]);

        assert!(lower_pk < higher_pk);
        assert!(lower_pk.to_raw() < higher_pk.to_raw());

        let prefix = vec![
            encode_component(&Value::Text("dup".to_string())),
            encode_component(&Value::Int(9)),
        ];
        let (start, end) = IndexKey::bounds_for_prefix(&index_id(), 2, &prefix);
        let start_raw = start.to_raw();
        let end_raw = end.to_raw();

        let mut hits = vec![lower_pk.to_raw(), higher_pk.to_raw()]
            .into_iter()
            .filter(|raw| *raw >= start_raw && *raw <= end_raw)
            .map(|raw| raw.as_bytes().to_vec())
            .collect::<Vec<_>>();
        hits.sort();

        assert_eq!(hits.len(), 2);
    }

    fn in_range(
        raw: &RawIndexKey,
        lower: &RangeBound<RawIndexKey>,
        upper: &RangeBound<RawIndexKey>,
    ) -> bool {
        let lower_ok = match lower {
            RangeBound::Unbounded => true,
            RangeBound::Included(bound) => raw >= bound,
            RangeBound::Excluded(bound) => raw > bound,
        };
        let upper_ok = match upper {
            RangeBound::Unbounded => true,
            RangeBound::Included(bound) => raw <= bound,
            RangeBound::Excluded(bound) => raw < bound,
        };
        lower_ok && upper_ok
    }

    #[test]
    fn index_key_component_range_excluded_upper_skips_entire_upper_value_group() {
        let prefix_a = encode_component(&Value::Uint(7));
        let b10 = encode_component(&Value::Uint(10));
        let b11 = encode_component(&Value::Uint(11));
        let b20 = encode_component(&Value::Uint(20));
        let c1 = encode_component(&Value::Uint(1));
        let c9 = encode_component(&Value::Uint(9));

        let k_b10_lo = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_a.clone(), b10.clone(), c1.clone()],
            vec![0x01],
        );
        let k_b10_hi = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_a.clone(), b10, c9.clone()],
            vec![0xFF],
        );
        let k_b11 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_a.clone(), b11, c1],
            vec![0x44],
        );
        let k_b20 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_a, b20, c9],
            vec![0x99],
        );

        let (lower, upper) = IndexKey::bounds_for_prefix_component_range(
            &index_id(),
            3,
            &[encode_component(&Value::Uint(7))],
            RangeBound::Included(encode_component(&Value::Uint(10))),
            RangeBound::Excluded(encode_component(&Value::Uint(20))),
        );

        let keys = [
            k_b10_lo.to_raw(),
            k_b10_hi.to_raw(),
            k_b11.to_raw(),
            k_b20.to_raw(),
        ];
        let hits = keys
            .iter()
            .filter(|raw| {
                in_range(
                    raw,
                    &raw_index_key_bound(lower.clone()),
                    &raw_index_key_bound(upper.clone()),
                )
            })
            .count();

        assert_eq!(
            hits, 3,
            "b=10 and b=11 should match; b=20 should be excluded"
        );
    }

    #[test]
    fn index_key_component_range_excluded_lower_skips_entire_lower_value_group() {
        let b10 = encode_component(&Value::Uint(10));
        let b11 = encode_component(&Value::Uint(11));
        let b20 = encode_component(&Value::Uint(20));

        let k_b10 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                encode_component(&Value::Uint(7)),
                b10,
                encode_component(&Value::Uint(1)),
            ],
            vec![0x01],
        );
        let k_b11 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                encode_component(&Value::Uint(7)),
                b11,
                encode_component(&Value::Uint(1)),
            ],
            vec![0x02],
        );
        let k_b20 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                encode_component(&Value::Uint(7)),
                b20,
                encode_component(&Value::Uint(1)),
            ],
            vec![0x03],
        );

        let (lower, upper) = IndexKey::bounds_for_prefix_component_range(
            &index_id(),
            3,
            &[encode_component(&Value::Uint(7))],
            RangeBound::Excluded(encode_component(&Value::Uint(10))),
            RangeBound::Included(encode_component(&Value::Uint(20))),
        );

        let keys = [k_b10.to_raw(), k_b11.to_raw(), k_b20.to_raw()];
        let hits = keys
            .iter()
            .filter(|raw| {
                in_range(
                    raw,
                    &raw_index_key_bound(lower.clone()),
                    &raw_index_key_bound(upper.clone()),
                )
            })
            .count();

        assert_eq!(
            hits, 2,
            "b=10 should be excluded; b=11 and b=20 should match"
        );
    }

    #[test]
    fn index_key_component_range_inclusive_extremes_cover_min_and_max_groups() {
        let prefix_7 = encode_component(&Value::Uint(7));
        let b0 = encode_component(&Value::Uint(0));
        let b1 = encode_component(&Value::Uint(1));
        let b_max = encode_component(&Value::Uint(u64::from(u32::MAX)));

        let k_b0 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7.clone(), b0, encode_component(&Value::Uint(1))],
            vec![0x11],
        );
        let k_b1 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7.clone(), b1, encode_component(&Value::Uint(1))],
            vec![0x12],
        );
        let k_b_max = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7.clone(), b_max, encode_component(&Value::Uint(1))],
            vec![0x13],
        );
        let k_other_prefix = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                encode_component(&Value::Uint(8)),
                encode_component(&Value::Uint(0)),
                encode_component(&Value::Uint(1)),
            ],
            vec![0x14],
        );

        let (lower, upper) = IndexKey::bounds_for_prefix_component_range(
            &index_id(),
            3,
            &[prefix_7],
            RangeBound::Included(encode_component(&Value::Uint(0))),
            RangeBound::Included(encode_component(&Value::Uint(u64::from(u32::MAX)))),
        );

        let keys = [
            k_b0.to_raw(),
            k_b1.to_raw(),
            k_b_max.to_raw(),
            k_other_prefix.to_raw(),
        ];
        let hits = keys
            .iter()
            .filter(|raw| {
                in_range(
                    raw,
                    &raw_index_key_bound(lower.clone()),
                    &raw_index_key_bound(upper.clone()),
                )
            })
            .count();

        assert_eq!(
            hits, 3,
            "inclusive [0, u32::MAX] range should include min/max groups for the selected prefix"
        );
    }

    #[test]
    fn index_key_component_range_exclusive_extremes_skip_min_and_max_groups() {
        let prefix_7 = encode_component(&Value::Uint(7));
        let b0 = encode_component(&Value::Uint(0));
        let b1 = encode_component(&Value::Uint(1));
        let b_max_minus_1 = encode_component(&Value::Uint(u64::from(u32::MAX) - 1));
        let b_max = encode_component(&Value::Uint(u64::from(u32::MAX)));

        let k_b0 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7.clone(), b0, encode_component(&Value::Uint(1))],
            vec![0x21],
        );
        let k_b1 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7.clone(), b1, encode_component(&Value::Uint(1))],
            vec![0x22],
        );
        let k_b_max_minus_1 = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![
                prefix_7.clone(),
                b_max_minus_1,
                encode_component(&Value::Uint(1)),
            ],
            vec![0x23],
        );
        let k_b_max = key_with(
            IndexKeyKind::User,
            index_id(),
            vec![prefix_7, b_max, encode_component(&Value::Uint(1))],
            vec![0x24],
        );

        let (lower, upper) = IndexKey::bounds_for_prefix_component_range(
            &index_id(),
            3,
            &[encode_component(&Value::Uint(7))],
            RangeBound::Excluded(encode_component(&Value::Uint(0))),
            RangeBound::Excluded(encode_component(&Value::Uint(u64::from(u32::MAX)))),
        );

        let keys = [
            k_b0.to_raw(),
            k_b1.to_raw(),
            k_b_max_minus_1.to_raw(),
            k_b_max.to_raw(),
        ];
        let hits = keys
            .iter()
            .filter(|raw| {
                in_range(
                    raw,
                    &raw_index_key_bound(lower.clone()),
                    &raw_index_key_bound(upper.clone()),
                )
            })
            .count();

        assert_eq!(
            hits, 2,
            "exclusive (0, u32::MAX) range should skip both edge groups"
        );
    }

    fn raw_index_key_bound(bound: RangeBound<IndexKey>) -> RangeBound<RawIndexKey> {
        match bound {
            RangeBound::Unbounded => RangeBound::Unbounded,
            RangeBound::Included(key) => RangeBound::Included(key.to_raw()),
            RangeBound::Excluded(key) => RangeBound::Excluded(key.to_raw()),
        }
    }

    #[test]
    fn index_key_float_nan_policy_and_zero_canonicalization_are_frozen() {
        assert!(Float64::try_new(f64::NAN).is_none());
        assert!(Float32::try_new(f32::NAN).is_none());

        let plus_zero = encode_component(&Value::Float64(
            Float64::try_new(0.0).expect("finite float should construct"),
        ));
        let minus_zero = encode_component(&Value::Float64(
            Float64::try_new(-0.0).expect("finite float should construct"),
        ));
        assert_eq!(plus_zero, minus_zero);
        assert_eq!(
            plus_zero,
            vec![0x0B, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn index_key_size_bound_enforcement_accepts_max_and_rejects_over_max() {
        let components = (0..MAX_INDEX_FIELDS)
            .map(|_| vec![0xAB; IndexKey::MAX_COMPONENT_SIZE])
            .collect::<Vec<_>>();
        let key = key_with(
            IndexKeyKind::User,
            index_id(),
            components,
            vec![0xCD; IndexKey::MAX_PK_SIZE],
        );
        let raw = key.to_raw();

        assert_eq!(raw.as_bytes().len(), IndexKey::STORED_SIZE_USIZE);
        let decoded = IndexKey::try_from_raw(&raw).expect("max-sized key should decode");
        assert_eq!(decoded.to_raw().as_bytes(), raw.as_bytes());

        let mut over_max = raw.as_bytes().to_vec();
        over_max.push(0x00);
        decode_must_fail_corrupted(over_max, "over-max key bytes should be rejected");
    }

    #[test]
    fn index_key_cross_index_isolation_keeps_ranges_separate() {
        let component = encode_component(&Value::Text("same".to_string()));

        let idx_a = index_id();
        let idx_b = other_index_id();

        let a1 = key_with(
            IndexKeyKind::User,
            idx_a,
            vec![component.clone()],
            vec![0x01],
        );
        let a2 = key_with(
            IndexKeyKind::User,
            idx_a,
            vec![component.clone()],
            vec![0x02],
        );
        let b1 = key_with(
            IndexKeyKind::User,
            idx_b,
            vec![component.clone()],
            vec![0x01],
        );
        let b2 = key_with(IndexKeyKind::User, idx_b, vec![component], vec![0x02]);

        let mut raws = vec![a1.to_raw(), a2.to_raw(), b1.to_raw(), b2.to_raw()];
        raws.sort();

        let a_bytes = [
            a1.to_raw().as_bytes().to_vec(),
            a2.to_raw().as_bytes().to_vec(),
        ];
        let b_bytes = [
            b1.to_raw().as_bytes().to_vec(),
            b2.to_raw().as_bytes().to_vec(),
        ];
        if idx_a < idx_b {
            assert!(a_bytes.iter().all(|a| b_bytes.iter().all(|b| a < b)));
        } else {
            assert!(b_bytes.iter().all(|b| a_bytes.iter().all(|a| b < a)));
        }

        let (start, end) = IndexKey::bounds_for_prefix(
            &idx_a,
            1,
            &[encode_component(&Value::Text("same".to_string()))],
        );
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());
        let matched = raws
            .into_iter()
            .filter(|raw| *raw >= start_raw && *raw <= end_raw)
            .collect::<Vec<_>>();

        assert_eq!(matched.len(), 2);
        for raw in matched {
            let decoded = IndexKey::try_from_raw(&raw).expect("matched key should decode");
            assert_eq!(decoded.index_id, idx_a);
        }
    }
}
