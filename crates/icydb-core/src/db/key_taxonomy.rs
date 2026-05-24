//! Module: db::key_taxonomy
//! Responsibility: compact key vocabulary and canonical primary-key encoder
//! proof, including the 0.162 scalar-or-composite primary-key value model.
//! Does not own: index-entry value ownership or cursor semantics.
//! Boundary: storage-format layers consume these wrappers instead of treating
//! the old fixed-width `StorageKey` frame as the conceptual API.
//!
//! Invariant:
//! One accepted entity primary-key namespace has exactly one logical
//! `PrimaryKeyKind`. Heterogeneous primary-key kinds inside one entity remain
//! unsupported; the persisted kind tag exists for validation, diagnostics,
//! cursor/index suffix decoding, and corruption handling.

#![cfg_attr(not(test), allow(dead_code))]

use crate::{
    MAX_INDEX_FIELDS,
    db::index::IndexId,
    traits::Repr,
    types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
    value::StorageKey,
};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

const TAG_SIZE: usize = 1;
const NAT_SIZE: usize = 8;
const INT_SIZE: usize = 8;
const TIMESTAMP_SIZE: usize = 8;
const ULID_SIZE: usize = 16;
const SUBACCOUNT_SIZE: usize = 32;
const ACCOUNT_SIZE: usize = Account::STORED_SIZE as usize;
const LENGTH_PREFIX_SIZE: usize = size_of::<u16>();
const INDEX_COMPONENT_MAX_SIZE: usize = 4 * 1024;
pub(in crate::db) const MAX_PRIMARY_KEY_FIELDS: usize = 4;
const SCALAR_PRIMARY_KEY_MAX_SIZE: usize = TAG_SIZE + ACCOUNT_SIZE;
const MAX_ENCODED_PRIMARY_KEY_COMPONENT_SIZE: usize = SCALAR_PRIMARY_KEY_MAX_SIZE;
const COMPOSITE_PRIMARY_KEY_HEADER_SIZE: usize = TAG_SIZE + TAG_SIZE;
pub(in crate::db) const COMPOSITE_PRIMARY_KEY_MAX_SIZE: usize = COMPOSITE_PRIMARY_KEY_HEADER_SIZE
    + (MAX_PRIMARY_KEY_FIELDS * MAX_ENCODED_PRIMARY_KEY_COMPONENT_SIZE);
const INDEX_PRIMARY_KEY_MAX_SIZE: usize = COMPOSITE_PRIMARY_KEY_MAX_SIZE;

type BorrowedIndexStoreKeySegments<'a> = (IndexStoreKeyKind, IndexId, Vec<&'a [u8]>, &'a [u8]);

//
// PrimaryKeyKind
//

/// Logical primary-key discriminator for compact canonical primary-key bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(in crate::db) enum PrimaryKeyKind {
    Nat = 0x01,
    Int = 0x02,
    Timestamp = 0x03,
    Ulid = 0x04,
    Principal = 0x05,
    Subaccount = 0x06,
    Account = 0x07,
    Unit = 0x08,
    Composite = 0x09,
}

impl PrimaryKeyKind {
    #[must_use]
    pub(in crate::db) const fn tag(self) -> u8 {
        self as u8
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Nat),
            0x02 => Some(Self::Int),
            0x03 => Some(Self::Timestamp),
            0x04 => Some(Self::Ulid),
            0x05 => Some(Self::Principal),
            0x06 => Some(Self::Subaccount),
            0x07 => Some(Self::Account),
            0x08 => Some(Self::Unit),
            0x09 => Some(Self::Composite),
            _ => None,
        }
    }

    const fn fixed_payload_len(self) -> Option<usize> {
        match self {
            Self::Nat => Some(NAT_SIZE),
            Self::Int => Some(INT_SIZE),
            Self::Timestamp => Some(TIMESTAMP_SIZE),
            Self::Ulid => Some(ULID_SIZE),
            Self::Principal | Self::Composite => None,
            Self::Subaccount => Some(SUBACCOUNT_SIZE),
            Self::Account => Some(ACCOUNT_SIZE),
            Self::Unit => Some(0),
        }
    }

    const fn fixed_length_expectation(self) -> &'static str {
        match self {
            Self::Nat | Self::Int | Self::Timestamp => "tag + 8-byte payload",
            Self::Ulid => "tag + 16-byte payload",
            Self::Principal => "tag + one-byte length + principal bytes",
            Self::Subaccount => "tag + 32-byte payload",
            Self::Account => "tag + 62-byte account payload",
            Self::Unit => "tag only",
            Self::Composite => "tag + component count + encoded scalar components",
        }
    }
}

//
// PrimaryKeyComponent
//

/// One admitted scalar primary-key component before compact canonical encoding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PrimaryKeyComponent {
    Nat(u64),
    Int(i64),
    Timestamp(Timestamp),
    Ulid(Ulid),
    Principal(Principal),
    Subaccount(Subaccount),
    Account(Account),
    Unit,
}

impl PrimaryKeyComponent {
    #[must_use]
    pub(in crate::db) const fn kind(self) -> PrimaryKeyKind {
        match self {
            Self::Nat(_) => PrimaryKeyKind::Nat,
            Self::Int(_) => PrimaryKeyKind::Int,
            Self::Timestamp(_) => PrimaryKeyKind::Timestamp,
            Self::Ulid(_) => PrimaryKeyKind::Ulid,
            Self::Principal(_) => PrimaryKeyKind::Principal,
            Self::Subaccount(_) => PrimaryKeyKind::Subaccount,
            Self::Account(_) => PrimaryKeyKind::Account,
            Self::Unit => PrimaryKeyKind::Unit,
        }
    }
}

impl From<StorageKey> for PrimaryKeyComponent {
    fn from(value: StorageKey) -> Self {
        match value {
            StorageKey::Nat(value) => Self::Nat(value),
            StorageKey::Int(value) => Self::Int(value),
            StorageKey::Timestamp(value) => Self::Timestamp(value),
            StorageKey::Ulid(value) => Self::Ulid(value),
            StorageKey::Principal(value) => Self::Principal(value),
            StorageKey::Subaccount(value) => Self::Subaccount(value),
            StorageKey::Account(value) => Self::Account(value),
            StorageKey::Unit => Self::Unit,
        }
    }
}

impl From<PrimaryKeyComponent> for StorageKey {
    fn from(value: PrimaryKeyComponent) -> Self {
        match value {
            PrimaryKeyComponent::Nat(value) => Self::Nat(value),
            PrimaryKeyComponent::Int(value) => Self::Int(value),
            PrimaryKeyComponent::Timestamp(value) => Self::Timestamp(value),
            PrimaryKeyComponent::Ulid(value) => Self::Ulid(value),
            PrimaryKeyComponent::Principal(value) => Self::Principal(value),
            PrimaryKeyComponent::Subaccount(value) => Self::Subaccount(value),
            PrimaryKeyComponent::Account(value) => Self::Account(value),
            PrimaryKeyComponent::Unit => Self::Unit,
        }
    }
}

impl Ord for PrimaryKeyComponent {
    fn cmp(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Nat(a), Self::Nat(b)) => a.cmp(&b),
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(&b),
            (Self::Ulid(a), Self::Ulid(b)) => a.cmp(&b),
            (Self::Principal(a), Self::Principal(b)) => a.cmp(&b),
            (Self::Subaccount(a), Self::Subaccount(b)) => a.cmp(&b),
            (Self::Account(a), Self::Account(b)) => a.cmp(&b),
            (Self::Unit, Self::Unit) => Ordering::Equal,
            _ => self.kind().cmp(&other.kind()),
        }
    }
}

impl PartialOrd for PrimaryKeyComponent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//
// CompositePrimaryKeyValue
//

/// Fixed-capacity composite primary-key component list.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompositePrimaryKeyValue {
    len: u8,
    components: [PrimaryKeyComponent; MAX_PRIMARY_KEY_FIELDS],
}

impl CompositePrimaryKeyValue {
    pub fn try_from_components(
        components: &[PrimaryKeyComponent],
    ) -> Result<Self, CompositePrimaryKeyValueError> {
        if components.len() < 2 {
            return Err(CompositePrimaryKeyValueError::TooFewComponents {
                count: components.len(),
                min: 2,
            });
        }
        if components.len() > MAX_PRIMARY_KEY_FIELDS {
            return Err(CompositePrimaryKeyValueError::TooManyComponents {
                count: components.len(),
                max: MAX_PRIMARY_KEY_FIELDS,
            });
        }
        if let Some(index) = components
            .iter()
            .position(|component| matches!(component, PrimaryKeyComponent::Unit))
        {
            return Err(CompositePrimaryKeyValueError::UnitComponent { index });
        }

        let mut stored = [PrimaryKeyComponent::Unit; MAX_PRIMARY_KEY_FIELDS];
        stored[..components.len()].copy_from_slice(components);
        let len = u8::try_from(components.len())
            .expect("MAX_PRIMARY_KEY_FIELDS must fit in u8 for compact composite keys");

        Ok(Self {
            len,
            components: stored,
        })
    }

    #[must_use]
    pub const fn len(self) -> usize {
        self.len as usize
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn components(&self) -> &[PrimaryKeyComponent] {
        &self.components[..self.len()]
    }
}

impl Ord for CompositePrimaryKeyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.components().cmp(other.components())
    }
}

impl PartialOrd for CompositePrimaryKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//
// PrimaryKeyValue
//

/// Complete logical primary-key value before compact canonical encoding.
///
/// Scalar keys remain the one-component case. Composite keys are represented
/// separately so scalar-only paths cannot silently treat them as the historical
/// decoded `StorageKey`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "0.162 primary-key values stay Copy and allocation-free on hot encode/decode paths"
)]
pub enum PrimaryKeyValue {
    Scalar(PrimaryKeyComponent),
    Composite(CompositePrimaryKeyValue),
}

impl PrimaryKeyValue {
    #[must_use]
    pub(in crate::db) const fn kind(self) -> PrimaryKeyKind {
        match self {
            Self::Scalar(value) => value.kind(),
            Self::Composite(_) => PrimaryKeyKind::Composite,
        }
    }

    #[must_use]
    pub const fn scalar_component(self) -> Option<PrimaryKeyComponent> {
        match self {
            Self::Scalar(value) => Some(value),
            Self::Composite(_) => None,
        }
    }
}

impl From<PrimaryKeyComponent> for PrimaryKeyValue {
    fn from(value: PrimaryKeyComponent) -> Self {
        Self::Scalar(value)
    }
}

impl From<StorageKey> for PrimaryKeyValue {
    fn from(value: StorageKey) -> Self {
        Self::Scalar(PrimaryKeyComponent::from(value))
    }
}

impl Ord for PrimaryKeyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Scalar(a), Self::Scalar(b)) => a.cmp(&b),
            (Self::Composite(a), Self::Composite(b)) => a.cmp(&b),
            _ => self.kind().cmp(&other.kind()),
        }
    }
}

impl PartialOrd for PrimaryKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//
// Errors
//

#[derive(Debug, ThisError)]
pub enum CompositePrimaryKeyValueError {
    #[error("composite primary key has too few components: {count} (minimum {min})")]
    TooFewComponents { count: usize, min: usize },

    #[error("composite primary key has too many components: {count} (limit {max})")]
    TooManyComponents { count: usize, max: usize },

    #[error("unit is not admitted as composite primary-key component {index}")]
    UnitComponent { index: usize },
}

#[derive(Debug, ThisError)]
pub(in crate::db) enum CompactPrimaryKeyEncodeError {
    #[error("account primary key encoding failed: {reason}")]
    InvalidAccount { reason: &'static str },

    #[error("principal primary key exceeds max length: {len} bytes (limit {max})")]
    PrincipalTooLarge { len: usize, max: usize },
}

#[derive(Debug, ThisError)]
pub(in crate::db) enum CompactPrimaryKeyDecodeError {
    #[error("encoded primary key is empty")]
    Empty,

    #[error("encoded primary key has unknown kind tag: {tag}")]
    UnknownKind { tag: u8 },

    #[error(
        "encoded primary key has invalid length for {kind:?}: {len} bytes (expected {expected})"
    )]
    InvalidLength {
        kind: PrimaryKeyKind,
        len: usize,
        expected: &'static str,
    },

    #[error("encoded principal primary key has invalid length: {len} bytes (limit {max})")]
    InvalidPrincipalLength { len: usize, max: usize },

    #[error("encoded account primary key is invalid: {reason}")]
    InvalidAccount { reason: &'static str },

    #[error(
        "encoded composite primary key has invalid component count: {count} (expected {expected})"
    )]
    InvalidCompositeCount {
        count: usize,
        expected: &'static str,
    },

    #[error("unit is not admitted as encoded composite primary-key component {index}")]
    UnitCompositeComponent { index: usize },

    #[error("nested composite primary-key component is not admitted")]
    NestedComposite,

    #[error("encoded composite primary key is not a scalar component")]
    CompositeNotScalar,

    #[error("encoded composite primary key has trailing bytes: {len}")]
    TrailingCompositeBytes { len: usize },
}

#[derive(Debug, ThisError)]
pub(in crate::db) enum CompactStoreKeyEncodeError {
    #[error("index store key has too many components: {count} (limit {max})")]
    TooManyIndexComponents { count: usize, max: usize },
}

#[derive(Debug, ThisError)]
pub(in crate::db) enum CompactStoreKeyDecodeError {
    #[error("raw data store key is too short: {len} bytes (minimum {min})")]
    DataStoreKeyTooShort { len: usize, min: usize },

    #[error("raw index store key is truncated while reading {segment}")]
    TruncatedIndexSegment { segment: &'static str },

    #[error("raw index store key has unknown key kind: {kind}")]
    UnknownIndexKeyKind { kind: u8 },

    #[error("raw index store key has empty {segment}")]
    EmptyIndexSegment { segment: &'static str },

    #[error("raw index store key has invalid index id bytes")]
    InvalidIndexId,

    #[error("raw index store key has trailing bytes: {len}")]
    TrailingIndexBytes { len: usize },

    #[error("raw index store key has too many components: {count} (limit {max})")]
    TooManyIndexComponents { count: usize, max: usize },

    #[error("raw index store key {segment} is too large: {len} bytes (limit {max})")]
    IndexSegmentTooLarge {
        segment: &'static str,
        len: usize,
        max: usize,
    },

    #[error("raw store key contains invalid primary key: {0}")]
    InvalidPrimaryKey(#[from] CompactPrimaryKeyDecodeError),
}

//
// EncodedPrimaryKey
//

/// Compact canonical primary-key bytes: `key_kind_tag + canonical_payload`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct EncodedPrimaryKey {
    bytes: Vec<u8>,
}

impl EncodedPrimaryKey {
    pub(in crate::db) fn encode(
        value: impl Into<PrimaryKeyValue>,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        match value.into() {
            PrimaryKeyValue::Scalar(value) => Self::encode_component(value),
            PrimaryKeyValue::Composite(value) => Self::encode_composite(&value),
        }
    }

    pub(in crate::db) fn encode_component(
        value: PrimaryKeyComponent,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(max_encoded_primary_key_len(value.kind()));
        encode_primary_key_component(value, &mut bytes)?;
        Ok(Self { bytes })
    }

    pub(in crate::db) fn encode_composite(
        value: &CompositePrimaryKeyValue,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(COMPOSITE_PRIMARY_KEY_MAX_SIZE);
        encode_composite_primary_key_value(value, &mut bytes)?;
        Ok(Self { bytes })
    }

    pub(in crate::db) fn decode(&self) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        match self.kind()? {
            PrimaryKeyKind::Composite => {
                decode_composite_primary_key_value(&self.bytes).map(PrimaryKeyValue::Composite)
            }
            _ => decode_primary_key_component(&self.bytes).map(PrimaryKeyValue::Scalar),
        }
    }

    pub(in crate::db) fn decode_component(
        &self,
    ) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
        decode_primary_key_component(&self.bytes)
    }

    pub(in crate::db) fn decode_composite(
        &self,
    ) -> Result<CompositePrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        decode_composite_primary_key_value(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(in crate::db) fn kind(&self) -> Result<PrimaryKeyKind, CompactPrimaryKeyDecodeError> {
        let Some(&tag) = self.bytes.first() else {
            return Err(CompactPrimaryKeyDecodeError::Empty);
        };

        PrimaryKeyKind::from_tag(tag).ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })
    }

    pub(in crate::db) fn payload(&self) -> Result<&[u8], CompactPrimaryKeyDecodeError> {
        let _ = self.kind()?;
        Ok(&self.bytes[TAG_SIZE..])
    }
}

impl TryFrom<&[u8]> for EncodedPrimaryKey {
    type Error = CompactPrimaryKeyDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let this = Self {
            bytes: bytes.to_vec(),
        };
        let _ = this.decode()?;
        Ok(this)
    }
}

impl From<EncodedPrimaryKey> for Vec<u8> {
    fn from(value: EncodedPrimaryKey) -> Self {
        value.bytes
    }
}

//
// EncodedIndexComponent
//

/// Canonical ordered bytes for one secondary-index component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct EncodedIndexComponent {
    bytes: Vec<u8>,
}

impl EncodedIndexComponent {
    pub(in crate::db) fn encode_primary_overlap(
        value: PrimaryKeyComponent,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(max_encoded_primary_key_len(value.kind()));
        encode_primary_key_component(value, &mut bytes)?;
        Ok(Self { bytes })
    }

    #[must_use]
    pub(in crate::db) const fn from_canonical_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(in crate::db) fn payload(&self) -> Result<&[u8], CompactPrimaryKeyDecodeError> {
        let Some(&tag) = self.bytes.first() else {
            return Err(CompactPrimaryKeyDecodeError::Empty);
        };
        let _ = PrimaryKeyKind::from_tag(tag)
            .ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })?;
        Ok(&self.bytes[TAG_SIZE..])
    }
}

impl TryFrom<&[u8]> for EncodedIndexComponent {
    type Error = CompactPrimaryKeyDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(CompactPrimaryKeyDecodeError::Empty);
        }

        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }
}

//
// Store-key wrappers
//

/// Logical data-store key: `EntityTag + EncodedPrimaryKey`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct DataStoreKey {
    entity_tag: EntityTag,
    primary_key: EncodedPrimaryKey,
}

impl DataStoreKey {
    #[must_use]
    pub(in crate::db) const fn new(entity_tag: EntityTag, primary_key: EncodedPrimaryKey) -> Self {
        Self {
            entity_tag,
            primary_key,
        }
    }

    #[must_use]
    pub(in crate::db) fn to_raw(&self) -> RawDataStoreKey {
        let mut bytes = Vec::with_capacity(size_of::<u64>() + self.primary_key.as_bytes().len());
        bytes.extend_from_slice(&self.entity_tag.value().to_be_bytes());
        bytes.extend_from_slice(self.primary_key.as_bytes());
        RawDataStoreKey { bytes }
    }

    pub(in crate::db) fn try_from_raw_bytes(
        bytes: &[u8],
    ) -> Result<Self, CompactStoreKeyDecodeError> {
        const ENTITY_TAG_SIZE: usize = size_of::<u64>();
        const MIN_RAW_DATA_STORE_KEY_SIZE: usize = ENTITY_TAG_SIZE + TAG_SIZE;

        if bytes.len() < MIN_RAW_DATA_STORE_KEY_SIZE {
            return Err(CompactStoreKeyDecodeError::DataStoreKeyTooShort {
                len: bytes.len(),
                min: MIN_RAW_DATA_STORE_KEY_SIZE,
            });
        }

        let mut entity_bytes = [0u8; ENTITY_TAG_SIZE];
        entity_bytes.copy_from_slice(&bytes[..ENTITY_TAG_SIZE]);
        let primary_key = EncodedPrimaryKey::try_from(&bytes[ENTITY_TAG_SIZE..])?;

        Ok(Self::new(
            EntityTag::new(u64::from_be_bytes(entity_bytes)),
            primary_key,
        ))
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) const fn primary_key(&self) -> &EncodedPrimaryKey {
        &self.primary_key
    }
}

/// Raw persisted data-store key bytes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RawDataStoreKey {
    bytes: Vec<u8>,
}

impl RawDataStoreKey {
    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = DataStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    #[must_use]
    pub(in crate::db) const fn from_persisted_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(in crate::db) fn decode(&self) -> Result<DataStoreKey, CompactStoreKeyDecodeError> {
        DataStoreKey::try_from_raw_bytes(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub(in crate::db) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Variable-width entity-local data-store scan range.
///
/// Full entity scans use raw entity-prefix bounds. They do not synthesize fake
/// minimum/maximum primary-key sentinels, because compact primary keys are
/// variable-width and self-describing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct RawDataStoreKeyRange {
    lower_inclusive: Vec<u8>,
    upper_exclusive: Option<Vec<u8>>,
}

impl RawDataStoreKeyRange {
    #[must_use]
    pub(in crate::db) fn entity_prefix(entity_tag: EntityTag) -> Self {
        let lower_inclusive = entity_tag.value().to_be_bytes().to_vec();
        let upper_exclusive = entity_tag
            .value()
            .checked_add(1)
            .map(|next| next.to_be_bytes().to_vec());

        Self {
            lower_inclusive,
            upper_exclusive,
        }
    }

    #[must_use]
    pub(in crate::db) fn contains(&self, key: &RawDataStoreKey) -> bool {
        key.as_bytes() >= self.lower_inclusive.as_slice()
            && self
                .upper_exclusive
                .as_deref()
                .is_none_or(|upper| key.as_bytes() < upper)
    }

    #[must_use]
    pub(in crate::db) fn lower_inclusive(&self) -> &[u8] {
        &self.lower_inclusive
    }

    #[must_use]
    pub(in crate::db) fn upper_exclusive(&self) -> Option<&[u8]> {
        self.upper_exclusive.as_deref()
    }
}

/// Logical index-store key namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(in crate::db) enum IndexStoreKeyKind {
    User = 0x00,
    System = 0x01,
}

impl IndexStoreKeyKind {
    #[must_use]
    pub(in crate::db) const fn tag(self) -> u8 {
        self as u8
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x00 => Some(Self::User),
            0x01 => Some(Self::System),
            _ => None,
        }
    }
}

/// Logical index-store key:
/// `key_kind + IndexId + EncodedIndexComponent[] + EncodedPrimaryKey`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct IndexStoreKey {
    key_kind: IndexStoreKeyKind,
    index_id: IndexId,
    components: Vec<EncodedIndexComponent>,
    primary_key: EncodedPrimaryKey,
}

impl IndexStoreKey {
    #[must_use]
    pub(in crate::db) const fn new(
        index_id: IndexId,
        components: Vec<EncodedIndexComponent>,
        primary_key: EncodedPrimaryKey,
    ) -> Self {
        Self::new_with_kind(IndexStoreKeyKind::User, index_id, components, primary_key)
    }

    #[must_use]
    pub(in crate::db) const fn new_with_kind(
        key_kind: IndexStoreKeyKind,
        index_id: IndexId,
        components: Vec<EncodedIndexComponent>,
        primary_key: EncodedPrimaryKey,
    ) -> Self {
        Self {
            key_kind,
            index_id,
            components,
            primary_key,
        }
    }

    pub(in crate::db) fn to_raw(&self) -> Result<RawIndexStoreKey, CompactStoreKeyEncodeError> {
        let component_count = u8::try_from(self.components.len()).map_err(|_| {
            CompactStoreKeyEncodeError::TooManyIndexComponents {
                count: self.components.len(),
                max: u8::MAX as usize,
            }
        })?;
        let primary_len = self.primary_key.as_bytes().len();
        let component_len: usize = self
            .components
            .iter()
            .map(|component| LENGTH_PREFIX_SIZE + component.as_bytes().len())
            .sum();
        let mut bytes = Vec::with_capacity(
            TAG_SIZE
                + IndexId::STORED_SIZE_USIZE
                + TAG_SIZE
                + component_len
                + LENGTH_PREFIX_SIZE
                + primary_len,
        );

        bytes.push(self.key_kind.tag());
        bytes.extend_from_slice(&self.index_id.to_bytes());
        bytes.push(component_count);
        for component in &self.components {
            push_len_prefixed(component.as_bytes(), &mut bytes);
        }
        push_len_prefixed(self.primary_key.as_bytes(), &mut bytes);

        Ok(RawIndexStoreKey { bytes })
    }

    pub(in crate::db) fn try_from_raw_bytes(
        bytes: &[u8],
    ) -> Result<Self, CompactStoreKeyDecodeError> {
        let mut input = bytes;

        let key_kind = take_exact(&mut input, TAG_SIZE, "key kind")?[0];
        let key_kind = IndexStoreKeyKind::from_tag(key_kind)
            .ok_or(CompactStoreKeyDecodeError::UnknownIndexKeyKind { kind: key_kind })?;

        let index_id = IndexId::from_bytes(take_exact(
            &mut input,
            IndexId::STORED_SIZE_USIZE,
            "index id",
        )?)
        .ok_or(CompactStoreKeyDecodeError::InvalidIndexId)?;

        let component_count = usize::from(take_exact(&mut input, TAG_SIZE, "component count")?[0]);
        let mut components = Vec::with_capacity(component_count);
        for _ in 0..component_count {
            let component_bytes = take_len_prefixed(&mut input, "index component")?;
            components.push(EncodedIndexComponent::try_from(component_bytes)?);
        }

        let primary_key =
            EncodedPrimaryKey::try_from(take_len_prefixed(&mut input, "primary key suffix")?)?;
        if !input.is_empty() {
            return Err(CompactStoreKeyDecodeError::TrailingIndexBytes { len: input.len() });
        }

        Ok(Self::new_with_kind(
            key_kind,
            index_id,
            components,
            primary_key,
        ))
    }

    #[must_use]
    pub(in crate::db) const fn key_kind(&self) -> IndexStoreKeyKind {
        self.key_kind
    }

    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db) fn components(&self) -> &[EncodedIndexComponent] {
        &self.components
    }

    #[must_use]
    pub(in crate::db) const fn primary_key(&self) -> &EncodedPrimaryKey {
        &self.primary_key
    }
}

/// Raw persisted index-store key bytes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RawIndexStoreKey {
    bytes: Vec<u8>,
}

impl RawIndexStoreKey {
    #[must_use]
    pub(in crate::db) const fn from_persisted_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = IndexStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    pub(in crate::db) fn decode(&self) -> Result<IndexStoreKey, CompactStoreKeyDecodeError> {
        IndexStoreKey::try_from_raw_bytes(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub(in crate::db) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Ord for RawIndexStoreKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_raw_index_store_key_bytes(&self.bytes, &other.bytes)
    }
}

impl PartialOrd for RawIndexStoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Secondary-index value. Primary-key membership belongs to the key, so this
/// value carries only a storage-owned presence/existence witness.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct IndexEntryValue {
    bytes: Vec<u8>,
}

impl IndexEntryValue {
    #[must_use]
    pub(in crate::db) fn presence_only() -> Self {
        Self { bytes: vec![0] }
    }

    #[must_use]
    pub(in crate::db) const fn from_persisted_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub(in crate::db) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

fn encode_primary_key_component(
    value: PrimaryKeyComponent,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    out.push(value.kind().tag());
    encode_primary_key_payload(value, out)
}

fn encode_composite_primary_key_value(
    value: &CompositePrimaryKeyValue,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    out.push(PrimaryKeyKind::Composite.tag());
    out.push(value.len);
    for component in value.components() {
        encode_primary_key_component(*component, out)?;
    }

    Ok(())
}

fn encode_primary_key_payload(
    value: PrimaryKeyComponent,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    match value {
        PrimaryKeyComponent::Nat(value) => out.extend_from_slice(&value.to_be_bytes()),
        PrimaryKeyComponent::Int(value) => out.extend_from_slice(&encode_ordered_i64(value)),
        PrimaryKeyComponent::Timestamp(value) => {
            out.extend_from_slice(&encode_ordered_i64(value.repr()));
        }
        PrimaryKeyComponent::Ulid(value) => out.extend_from_slice(&value.to_bytes()),
        PrimaryKeyComponent::Principal(value) => encode_principal(value, out)?,
        PrimaryKeyComponent::Subaccount(value) => out.extend_from_slice(value.as_slice()),
        PrimaryKeyComponent::Account(value) => {
            let bytes = value.to_stored_bytes().map_err(|_| {
                CompactPrimaryKeyEncodeError::InvalidAccount {
                    reason: "account payload failed fixed stored encoding",
                }
            })?;
            out.extend_from_slice(&bytes);
        }
        PrimaryKeyComponent::Unit => {}
    }

    Ok(())
}

fn decode_primary_key_component(
    bytes: &[u8],
) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::Empty);
    };
    let kind =
        PrimaryKeyKind::from_tag(tag).ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })?;

    if let Some(expected) = kind.fixed_payload_len()
        && payload.len() != expected
    {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind,
            len: bytes.len(),
            expected: kind.fixed_length_expectation(),
        });
    }

    match kind {
        PrimaryKeyKind::Nat => {
            let mut buf = [0u8; NAT_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Nat(u64::from_be_bytes(buf)))
        }
        PrimaryKeyKind::Int => {
            let mut buf = [0u8; INT_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Int(decode_ordered_i64(buf)))
        }
        PrimaryKeyKind::Timestamp => {
            let mut buf = [0u8; TIMESTAMP_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Timestamp(Timestamp::from_repr(
                decode_ordered_i64(buf),
            )))
        }
        PrimaryKeyKind::Ulid => {
            let mut buf = [0u8; ULID_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Ulid(Ulid::from_bytes(buf)))
        }
        PrimaryKeyKind::Principal => decode_principal(payload),
        PrimaryKeyKind::Subaccount => {
            let mut buf = [0u8; SUBACCOUNT_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Subaccount(Subaccount::from_array(buf)))
        }
        PrimaryKeyKind::Account => Ok(PrimaryKeyComponent::Account(
            Account::try_from_bytes(payload)
                .map_err(|reason| CompactPrimaryKeyDecodeError::InvalidAccount { reason })?,
        )),
        PrimaryKeyKind::Unit => Ok(PrimaryKeyComponent::Unit),
        PrimaryKeyKind::Composite => Err(CompactPrimaryKeyDecodeError::CompositeNotScalar),
    }
}

fn decode_composite_primary_key_value(
    bytes: &[u8],
) -> Result<CompositePrimaryKeyValue, CompactPrimaryKeyDecodeError> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::Empty);
    };
    let kind =
        PrimaryKeyKind::from_tag(tag).ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })?;
    if kind != PrimaryKeyKind::Composite {
        return Err(CompactPrimaryKeyDecodeError::InvalidCompositeCount {
            count: 0,
            expected: "composite primary-key tag",
        });
    }

    let Some((&count, rest)) = payload.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind,
            len: bytes.len(),
            expected: kind.fixed_length_expectation(),
        });
    };
    let count = usize::from(count);
    if !(2..=MAX_PRIMARY_KEY_FIELDS).contains(&count) {
        return Err(CompactPrimaryKeyDecodeError::InvalidCompositeCount {
            count,
            expected: "2..=MAX_PRIMARY_KEY_FIELDS",
        });
    }

    let mut input = rest;
    let mut components = [PrimaryKeyComponent::Unit; MAX_PRIMARY_KEY_FIELDS];
    for (index, component) in components.iter_mut().take(count).enumerate() {
        let bytes = take_encoded_primary_key_component(&mut input)?;
        let value = decode_primary_key_component(bytes)?;
        if matches!(value, PrimaryKeyComponent::Unit) {
            return Err(CompactPrimaryKeyDecodeError::UnitCompositeComponent { index });
        }
        *component = value;
    }
    if !input.is_empty() {
        return Err(CompactPrimaryKeyDecodeError::TrailingCompositeBytes { len: input.len() });
    }

    CompositePrimaryKeyValue::try_from_components(&components[..count]).map_err(|err| match err {
        CompositePrimaryKeyValueError::TooFewComponents { count, .. }
        | CompositePrimaryKeyValueError::TooManyComponents { count, .. } => {
            CompactPrimaryKeyDecodeError::InvalidCompositeCount {
                count,
                expected: "2..=MAX_PRIMARY_KEY_FIELDS",
            }
        }
        CompositePrimaryKeyValueError::UnitComponent { index } => {
            CompactPrimaryKeyDecodeError::UnitCompositeComponent { index }
        }
    })
}

fn take_encoded_primary_key_component<'a>(
    input: &mut &'a [u8],
) -> Result<&'a [u8], CompactPrimaryKeyDecodeError> {
    let Some(&tag) = input.first() else {
        return Err(CompactPrimaryKeyDecodeError::Empty);
    };
    let kind =
        PrimaryKeyKind::from_tag(tag).ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })?;
    if kind == PrimaryKeyKind::Composite {
        return Err(CompactPrimaryKeyDecodeError::NestedComposite);
    }

    let total_len = match kind {
        PrimaryKeyKind::Principal => {
            let Some(&len) = input.get(TAG_SIZE) else {
                return Err(CompactPrimaryKeyDecodeError::InvalidLength {
                    kind,
                    len: input.len(),
                    expected: kind.fixed_length_expectation(),
                });
            };
            let len = usize::from(len);
            if len > Principal::MAX_LENGTH_IN_BYTES as usize {
                return Err(CompactPrimaryKeyDecodeError::InvalidPrincipalLength {
                    len,
                    max: Principal::MAX_LENGTH_IN_BYTES as usize,
                });
            }
            TAG_SIZE + TAG_SIZE + len
        }
        PrimaryKeyKind::Composite => unreachable!("composite handled above"),
        _ => {
            TAG_SIZE
                + kind
                    .fixed_payload_len()
                    .expect("scalar primary-key kind must have fixed payload len")
        }
    };
    if input.len() < total_len {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind,
            len: input.len(),
            expected: kind.fixed_length_expectation(),
        });
    }

    let (component, rest) = input.split_at(total_len);
    *input = rest;

    Ok(component)
}

fn encode_principal(
    value: Principal,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    let bytes =
        value
            .stored_bytes()
            .map_err(|_| CompactPrimaryKeyEncodeError::PrincipalTooLarge {
                len: value.as_slice().len(),
                max: Principal::MAX_LENGTH_IN_BYTES as usize,
            })?;
    let len =
        u8::try_from(bytes.len()).map_err(|_| CompactPrimaryKeyEncodeError::PrincipalTooLarge {
            len: bytes.len(),
            max: Principal::MAX_LENGTH_IN_BYTES as usize,
        })?;

    out.push(len);
    out.extend_from_slice(bytes);

    Ok(())
}

fn decode_principal(payload: &[u8]) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
    let Some((&len, bytes)) = payload.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
            len: TAG_SIZE,
            expected: PrimaryKeyKind::Principal.fixed_length_expectation(),
        });
    };

    let len = usize::from(len);
    if len > Principal::MAX_LENGTH_IN_BYTES as usize {
        return Err(CompactPrimaryKeyDecodeError::InvalidPrincipalLength {
            len,
            max: Principal::MAX_LENGTH_IN_BYTES as usize,
        });
    }

    if bytes.len() != len {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
            len: TAG_SIZE + payload.len(),
            expected: PrimaryKeyKind::Principal.fixed_length_expectation(),
        });
    }

    Ok(PrimaryKeyComponent::Principal(Principal::from_slice(bytes)))
}

#[must_use]
const fn encode_ordered_i64(value: i64) -> [u8; INT_SIZE] {
    (value.cast_unsigned() ^ (1u64 << 63)).to_be_bytes()
}

#[must_use]
const fn decode_ordered_i64(bytes: [u8; INT_SIZE]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1u64 << 63)).cast_signed()
}

fn push_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    let len = u16::try_from(bytes.len()).expect("compact key segment fits in u16");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

const fn take_exact<'a>(
    input: &mut &'a [u8],
    len: usize,
    segment: &'static str,
) -> Result<&'a [u8], CompactStoreKeyDecodeError> {
    if input.len() < len {
        return Err(CompactStoreKeyDecodeError::TruncatedIndexSegment { segment });
    }

    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_len_prefixed<'a>(
    input: &mut &'a [u8],
    segment: &'static str,
) -> Result<&'a [u8], CompactStoreKeyDecodeError> {
    let len_bytes = take_exact(input, LENGTH_PREFIX_SIZE, segment)?;
    let mut len = [0u8; LENGTH_PREFIX_SIZE];
    len.copy_from_slice(len_bytes);
    let len = usize::from(u16::from_be_bytes(len));
    if len == 0 {
        return Err(CompactStoreKeyDecodeError::EmptyIndexSegment { segment });
    }
    take_exact(input, len, segment)
}

fn compare_raw_index_store_key_bytes(left: &[u8], right: &[u8]) -> Ordering {
    let Ok((left_kind, left_index_id, left_components, left_primary_key)) =
        decode_raw_index_store_key_segments(left)
    else {
        return left.cmp(right);
    };
    let Ok((right_kind, right_index_id, right_components, right_primary_key)) =
        decode_raw_index_store_key_segments(right)
    else {
        return left.cmp(right);
    };

    left_kind
        .cmp(&right_kind)
        .then_with(|| left_index_id.cmp(&right_index_id))
        .then_with(|| left_components.len().cmp(&right_components.len()))
        .then_with(|| compare_segments(&left_components, &right_components))
        .then_with(|| left_primary_key.cmp(right_primary_key))
}

fn decode_raw_index_store_key_segments(
    bytes: &[u8],
) -> Result<BorrowedIndexStoreKeySegments<'_>, CompactStoreKeyDecodeError> {
    let mut input = bytes;

    let key_kind = take_exact(&mut input, TAG_SIZE, "key kind")?[0];
    let key_kind = IndexStoreKeyKind::from_tag(key_kind)
        .ok_or(CompactStoreKeyDecodeError::UnknownIndexKeyKind { kind: key_kind })?;

    let index_id = IndexId::from_bytes(take_exact(
        &mut input,
        IndexId::STORED_SIZE_USIZE,
        "index id",
    )?)
    .ok_or(CompactStoreKeyDecodeError::InvalidIndexId)?;

    let component_count = usize::from(take_exact(&mut input, TAG_SIZE, "component count")?[0]);
    if component_count > MAX_INDEX_FIELDS {
        return Err(CompactStoreKeyDecodeError::TooManyIndexComponents {
            count: component_count,
            max: MAX_INDEX_FIELDS,
        });
    }

    let mut components = Vec::with_capacity(component_count);
    for _ in 0..component_count {
        let component = take_len_prefixed(&mut input, "index component")?;
        if component.len() > INDEX_COMPONENT_MAX_SIZE {
            return Err(CompactStoreKeyDecodeError::IndexSegmentTooLarge {
                segment: "index component",
                len: component.len(),
                max: INDEX_COMPONENT_MAX_SIZE,
            });
        }
        components.push(component);
    }

    let primary_key = take_len_prefixed(&mut input, "primary key suffix")?;
    if primary_key.len() > INDEX_PRIMARY_KEY_MAX_SIZE {
        return Err(CompactStoreKeyDecodeError::IndexSegmentTooLarge {
            segment: "primary key suffix",
            len: primary_key.len(),
            max: INDEX_PRIMARY_KEY_MAX_SIZE,
        });
    }
    if !input.is_empty() {
        return Err(CompactStoreKeyDecodeError::TrailingIndexBytes { len: input.len() });
    }

    Ok((key_kind, index_id, components, primary_key))
}

fn compare_segments(left: &[&[u8]], right: &[&[u8]]) -> Ordering {
    for (left_segment, right_segment) in left.iter().zip(right.iter()) {
        let segment_order = left_segment.cmp(right_segment);
        if segment_order != Ordering::Equal {
            return segment_order;
        }
    }

    Ordering::Equal
}

const fn max_encoded_primary_key_len(kind: PrimaryKeyKind) -> usize {
    TAG_SIZE
        + match kind {
            PrimaryKeyKind::Principal => TAG_SIZE + Principal::MAX_LENGTH_IN_BYTES as usize,
            PrimaryKeyKind::Nat | PrimaryKeyKind::Int | PrimaryKeyKind::Timestamp => NAT_SIZE,
            PrimaryKeyKind::Ulid => ULID_SIZE,
            PrimaryKeyKind::Subaccount => SUBACCOUNT_SIZE,
            PrimaryKeyKind::Account => ACCOUNT_SIZE,
            PrimaryKeyKind::Unit => 0,
            PrimaryKeyKind::Composite => COMPOSITE_PRIMARY_KEY_MAX_SIZE - TAG_SIZE,
        }
}

#[cfg(test)]
mod tests {
    use super::{
        COMPOSITE_PRIMARY_KEY_MAX_SIZE, CompactPrimaryKeyDecodeError, CompactStoreKeyDecodeError,
        CompositePrimaryKeyValue, CompositePrimaryKeyValueError, DataStoreKey,
        EncodedIndexComponent, EncodedPrimaryKey, IndexEntryValue, IndexStoreKey,
        IndexStoreKeyKind, MAX_PRIMARY_KEY_FIELDS, PrimaryKeyComponent, PrimaryKeyKind,
        PrimaryKeyValue, RawDataStoreKey, RawDataStoreKeyRange, RawIndexStoreKey,
    };
    use crate::{
        db::{
            data::DecodedDataStoreKey,
            index::{IndexId, IndexKey, IndexKeyKind},
        },
        traits::Repr,
        types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
        value::StorageKey,
    };

    fn account_fixture(seed: u8) -> Account {
        Account::from_parts(
            Principal::from_slice(&[seed]),
            Some(Subaccount::from_array([seed; 32])),
        )
    }

    fn composite_primary_key_fixture() -> CompositePrimaryKeyValue {
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(7),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(11)),
        ])
        .expect("composite primary key should construct")
    }

    fn roundtrip(value: PrimaryKeyComponent) {
        let encoded = EncodedPrimaryKey::encode(value).expect("primary key should encode");
        assert_eq!(encoded.kind().expect("kind should decode"), value.kind());
        assert_eq!(
            encoded
                .decode_component()
                .expect("primary key should decode"),
            value
        );
    }

    #[test]
    fn composite_primary_key_value_keeps_fixed_capacity_components() {
        let components = [
            PrimaryKeyComponent::Nat(7),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(9)),
        ];
        let key = CompositePrimaryKeyValue::try_from_components(&components)
            .expect("valid composite primary key should construct");

        assert_eq!(key.len(), 2);
        assert!(!key.is_empty());
        assert_eq!(key.components(), components);
    }

    #[test]
    fn composite_primary_key_value_rejects_invalid_component_counts() {
        let empty = CompositePrimaryKeyValue::try_from_components(&[])
            .expect_err("empty composite primary key should reject");
        assert!(matches!(
            empty,
            CompositePrimaryKeyValueError::TooFewComponents { count: 0, min: 2 }
        ));

        let one = CompositePrimaryKeyValue::try_from_components(&[PrimaryKeyComponent::Nat(1)])
            .expect_err("single-component composite primary key should reject");
        assert!(matches!(
            one,
            CompositePrimaryKeyValueError::TooFewComponents { count: 1, min: 2 }
        ));

        let too_many = [PrimaryKeyComponent::Nat(1); MAX_PRIMARY_KEY_FIELDS + 1];
        let err = CompositePrimaryKeyValue::try_from_components(&too_many)
            .expect_err("overwide composite primary key should reject");
        assert!(matches!(
            err,
            CompositePrimaryKeyValueError::TooManyComponents { count, max }
                if count == MAX_PRIMARY_KEY_FIELDS + 1 && max == MAX_PRIMARY_KEY_FIELDS
        ));
    }

    #[test]
    fn composite_primary_key_value_rejects_unit_components() {
        let err = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(1),
            PrimaryKeyComponent::Unit,
        ])
        .expect_err("unit is scalar-only and should reject in composite keys");

        assert!(matches!(
            err,
            CompositePrimaryKeyValueError::UnitComponent { index: 1 }
        ));
    }

    #[test]
    fn composite_primary_key_value_uses_lexicographic_component_order() {
        let left = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(1),
            PrimaryKeyComponent::Int(10),
        ])
        .expect("left key should construct");
        let right = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(2),
            PrimaryKeyComponent::Int(-10),
        ])
        .expect("right key should construct");

        assert!(left < right);
    }

    #[test]
    fn compact_composite_primary_key_roundtrips_components() {
        let value = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(7),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
        ])
        .expect("valid composite primary key should construct");

        let encoded = EncodedPrimaryKey::encode_composite(&value)
            .expect("composite primary key should encode");

        assert_eq!(
            encoded.kind().expect("encoded kind should decode"),
            PrimaryKeyKind::Composite,
        );
        assert_eq!(
            encoded
                .decode_composite()
                .expect("composite primary key should decode"),
            value,
        );
        assert_eq!(
            encoded
                .decode()
                .expect("composite primary key should decode"),
            PrimaryKeyValue::Composite(value),
        );
        assert!(matches!(
            encoded.decode_component(),
            Err(CompactPrimaryKeyDecodeError::CompositeNotScalar)
        ));
    }

    #[test]
    fn compact_primary_key_value_scalar_wrapper_uses_scalar_encoding() {
        let encoded =
            EncodedPrimaryKey::encode(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat(7)))
                .expect("scalar primary-key value should encode");

        assert_eq!(
            encoded.as_bytes(),
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(7))
                .expect("component should encode")
                .as_bytes(),
        );
        assert_eq!(
            encoded
                .decode()
                .expect("scalar primary-key value should decode"),
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat(7)),
        );
    }

    #[test]
    fn compact_primary_key_value_composite_wrapper_uses_composite_encoding() {
        let value = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(7),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(11)),
        ])
        .expect("composite primary-key value should construct");

        let encoded = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(value))
            .expect("composite primary-key value should encode");

        assert_eq!(
            encoded.kind().expect("kind should decode"),
            PrimaryKeyKind::Composite,
        );
        assert_eq!(
            encoded.decode().expect("primary-key value should decode"),
            PrimaryKeyValue::Composite(value),
        );
        assert!(matches!(
            encoded.decode_component(),
            Err(CompactPrimaryKeyDecodeError::CompositeNotScalar)
        ));
    }

    #[test]
    fn compact_composite_primary_key_rejects_invalid_counts() {
        let count_one = [
            PrimaryKeyKind::Composite.tag(),
            1,
            PrimaryKeyKind::Nat.tag(),
        ];
        let err = EncodedPrimaryKey {
            bytes: count_one.to_vec(),
        }
        .decode_composite()
        .expect_err("composite count one should reject");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidCompositeCount { count: 1, .. }
        ));

        let overwide = [PrimaryKeyKind::Composite.tag(), 5];
        let err = EncodedPrimaryKey {
            bytes: overwide.to_vec(),
        }
        .decode_composite()
        .expect_err("overwide composite count should reject");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidCompositeCount { count: 5, .. }
        ));
    }

    #[test]
    fn compact_composite_primary_key_rejects_unit_component_payload() {
        let bytes = [
            PrimaryKeyKind::Composite.tag(),
            2,
            PrimaryKeyKind::Nat.tag(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            PrimaryKeyKind::Unit.tag(),
        ];
        let err = EncodedPrimaryKey {
            bytes: bytes.to_vec(),
        }
        .decode_composite()
        .expect_err("unit component should reject");

        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::UnitCompositeComponent { index: 1 }
        ));
    }

    #[test]
    fn compact_composite_primary_key_byte_order_matches_component_order() {
        let left = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(1),
            PrimaryKeyComponent::Int(-1),
        ])
        .expect("left composite key should construct");
        let right = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat(1),
            PrimaryKeyComponent::Int(1),
        ])
        .expect("right composite key should construct");
        let left_encoded =
            EncodedPrimaryKey::encode_composite(&left).expect("left composite key should encode");
        let right_encoded =
            EncodedPrimaryKey::encode_composite(&right).expect("right composite key should encode");

        assert!(left < right);
        assert!(left_encoded < right_encoded);
    }

    #[test]
    fn compact_primary_key_roundtrip_per_key_type() {
        let values = [
            PrimaryKeyComponent::Nat(42),
            PrimaryKeyComponent::Int(-42),
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-42)),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(42)),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
            PrimaryKeyComponent::Account(account_fixture(7)),
            PrimaryKeyComponent::Unit,
        ];

        for value in values {
            roundtrip(value);
        }
    }

    #[test]
    fn compact_primary_key_rejects_malformed_kind_tag() {
        let err = EncodedPrimaryKey::try_from(&[0xFF][..])
            .expect_err("unknown primary-key kind tag should reject");

        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::UnknownKind { tag: 0xFF }
        ));
    }

    #[test]
    fn compact_primary_key_rejects_malformed_lengths() {
        let fixed_cases = [
            PrimaryKeyKind::Nat,
            PrimaryKeyKind::Int,
            PrimaryKeyKind::Timestamp,
            PrimaryKeyKind::Ulid,
            PrimaryKeyKind::Subaccount,
            PrimaryKeyKind::Account,
            PrimaryKeyKind::Unit,
        ];

        for kind in fixed_cases {
            let err = EncodedPrimaryKey::try_from(&[kind.tag(), 0xAA][..])
                .expect_err("fixed-width primary key should reject wrong length");
            assert!(matches!(
                err,
                CompactPrimaryKeyDecodeError::InvalidLength {
                    kind: err_kind,
                    ..
                } if err_kind == kind
            ));
        }
    }

    #[test]
    fn compact_primary_key_rejects_invalid_principal_length() {
        let too_long = [
            PrimaryKeyKind::Principal.tag(),
            u8::try_from(Principal::MAX_LENGTH_IN_BYTES)
                .expect("principal max length fits in one byte")
                + 1,
        ];
        let err = EncodedPrimaryKey::try_from(&too_long[..])
            .expect_err("oversized principal length should reject");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidPrincipalLength { len, max }
                if len == Principal::MAX_LENGTH_IN_BYTES as usize + 1
                    && max == Principal::MAX_LENGTH_IN_BYTES as usize
        ));

        let truncated = [PrimaryKeyKind::Principal.tag(), 3, 1, 2];
        let err = EncodedPrimaryKey::try_from(&truncated[..])
            .expect_err("truncated principal payload should reject");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidLength {
                kind: PrimaryKeyKind::Principal,
                ..
            }
        ));
    }

    #[test]
    fn compact_primary_key_accepts_principal_max_length_and_rejects_invalid_length() {
        let max = Principal::from_slice(&[0xAB; Principal::MAX_LENGTH_IN_BYTES as usize]);
        roundtrip(PrimaryKeyComponent::Principal(max));

        let missing_length = [PrimaryKeyKind::Principal.tag()];
        let err = EncodedPrimaryKey::try_from(&missing_length[..])
            .expect_err("principal payload must contain a length byte");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidLength {
                kind: PrimaryKeyKind::Principal,
                ..
            }
        ));
    }

    #[test]
    fn compact_primary_key_requires_subaccount_exact_length() {
        let short = [PrimaryKeyKind::Subaccount.tag(), 0x01];
        let err = EncodedPrimaryKey::try_from(&short[..])
            .expect_err("subaccount primary key must be exactly 32 bytes");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidLength {
                kind: PrimaryKeyKind::Subaccount,
                ..
            }
        ));

        roundtrip(PrimaryKeyComponent::Subaccount(Subaccount::from_array(
            [0xCC; 32],
        )));
    }

    #[test]
    fn compact_primary_key_validates_account_payload() {
        roundtrip(PrimaryKeyComponent::Account(account_fixture(9)));

        let mut invalid = vec![PrimaryKeyKind::Account.tag()];
        invalid.extend_from_slice(&[0u8; Account::STORED_SIZE as usize]);
        invalid[1] = u8::try_from(Principal::MAX_LENGTH_IN_BYTES)
            .expect("principal max length fits in one byte")
            + 1;

        let err = EncodedPrimaryKey::try_from(&invalid[..])
            .expect_err("invalid account payload should reject");
        assert!(matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidAccount { .. }
        ));
    }

    #[test]
    fn compact_primary_key_unit_is_kind_only_singleton() {
        let encoded = EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
            .expect("unit primary key should encode");

        assert_eq!(encoded.as_bytes(), &[PrimaryKeyKind::Unit.tag()]);
        assert_eq!(
            encoded
                .decode_component()
                .expect("unit primary key should decode"),
            PrimaryKeyComponent::Unit
        );
    }

    #[test]
    fn compact_primary_key_ordering_matches_logical_order_per_type() {
        let cases = [
            (PrimaryKeyComponent::Nat(1), PrimaryKeyComponent::Nat(2)),
            (PrimaryKeyComponent::Int(-2), PrimaryKeyComponent::Int(1)),
            (
                PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-1)),
                PrimaryKeyComponent::Timestamp(Timestamp::from_millis(1)),
            ),
            (
                PrimaryKeyComponent::Ulid(Ulid::from_u128(1)),
                PrimaryKeyComponent::Ulid(Ulid::from_u128(2)),
            ),
            (
                PrimaryKeyComponent::Principal(Principal::from_slice(&[9])),
                PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 0])),
            ),
            (
                PrimaryKeyComponent::Subaccount(Subaccount::from_array([1; 32])),
                PrimaryKeyComponent::Subaccount(Subaccount::from_array([2; 32])),
            ),
            (
                PrimaryKeyComponent::Account(account_fixture(1)),
                PrimaryKeyComponent::Account(account_fixture(2)),
            ),
        ];

        for (left, right) in cases {
            assert_eq!(left.cmp(&right), std::cmp::Ordering::Less);

            let left_encoded =
                EncodedPrimaryKey::encode(left).expect("left primary key should encode");
            let right_encoded =
                EncodedPrimaryKey::encode(right).expect("right primary key should encode");

            assert_eq!(left_encoded.cmp(&right_encoded), left.cmp(&right));
        }
    }

    #[test]
    fn compact_primary_key_timestamp_negative_ordering_is_biased() {
        let mut values = [
            Timestamp::from_millis(0),
            Timestamp::from_millis(i64::MIN),
            Timestamp::from_millis(-1),
            Timestamp::from_millis(1),
            Timestamp::from_millis(i64::MAX),
        ];
        values.sort();

        let mut encoded = values.map(|value| {
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Timestamp(value))
                .expect("timestamp primary key should encode")
        });
        encoded.sort();

        let decoded =
            encoded.map(
                |value| match value.decode_component().expect("timestamp should decode") {
                    PrimaryKeyComponent::Timestamp(value) => value.repr(),
                    other => panic!("expected timestamp primary key, got {other:?}"),
                },
            );
        let expected = values.map(|value| value.repr());

        assert_eq!(decoded, expected);
    }

    #[test]
    fn compact_primary_key_principal_length_first_ordering_fixture() {
        let short = PrimaryKeyComponent::Principal(Principal::from_slice(&[9]));
        let long = PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 0]));

        assert_eq!(short.cmp(&long), std::cmp::Ordering::Less);

        let short_encoded =
            EncodedPrimaryKey::encode(short).expect("short principal should encode");
        let long_encoded = EncodedPrimaryKey::encode(long).expect("long principal should encode");

        assert_eq!(short_encoded.cmp(&long_encoded), std::cmp::Ordering::Less);
        assert_eq!(short_encoded.payload().expect("payload"), &[1, 9]);
        assert_eq!(long_encoded.payload().expect("payload"), &[2, 1, 0]);
    }

    #[test]
    fn compact_primary_and_index_component_payload_ordering_match_for_overlapping_primitives() {
        let pairs = [
            (PrimaryKeyComponent::Nat(7), PrimaryKeyComponent::Nat(8)),
            (PrimaryKeyComponent::Int(-7), PrimaryKeyComponent::Int(8)),
            (
                PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-7)),
                PrimaryKeyComponent::Timestamp(Timestamp::from_millis(8)),
            ),
            (
                PrimaryKeyComponent::Ulid(Ulid::from_u128(7)),
                PrimaryKeyComponent::Ulid(Ulid::from_u128(8)),
            ),
            (
                PrimaryKeyComponent::Principal(Principal::from_slice(&[7])),
                PrimaryKeyComponent::Principal(Principal::from_slice(&[8])),
            ),
            (
                PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
                PrimaryKeyComponent::Subaccount(Subaccount::from_array([8; 32])),
            ),
            (
                PrimaryKeyComponent::Account(account_fixture(7)),
                PrimaryKeyComponent::Account(account_fixture(8)),
            ),
            (PrimaryKeyComponent::Unit, PrimaryKeyComponent::Unit),
        ];

        for (left, right) in pairs {
            let left_primary =
                EncodedPrimaryKey::encode(left).expect("left primary key should encode");
            let right_primary =
                EncodedPrimaryKey::encode(right).expect("right primary key should encode");
            let left_index = EncodedIndexComponent::encode_primary_overlap(left)
                .expect("left index component should encode");
            let right_index = EncodedIndexComponent::encode_primary_overlap(right)
                .expect("right index component should encode");

            assert_eq!(left_primary.as_bytes(), left_index.as_bytes());
            assert_eq!(
                left_primary.payload().expect("primary payload"),
                left_index.payload().expect("index payload")
            );
            assert_eq!(
                left_primary.cmp(&right_primary),
                left_index.cmp(&right_index)
            );
        }
    }

    #[test]
    fn compact_primary_key_storage_key_bridge_preserves_logical_values() {
        let storage_key = StorageKey::Timestamp(Timestamp::from_millis(-11));
        let primary_key = PrimaryKeyComponent::from(storage_key);

        assert_eq!(
            EncodedPrimaryKey::encode(primary_key)
                .expect("storage-key bridge should encode")
                .decode_component()
                .expect("storage-key bridge should decode"),
            primary_key
        );
    }

    #[test]
    fn key_taxonomy_wrappers_match_live_compact_data_key_cut() {
        let entity = EntityTag::new(0x159);
        let primary_key = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(5))
            .expect("primary key should encode");
        let data_key = DataStoreKey::new(entity, primary_key.clone());
        let raw_data: RawDataStoreKey = data_key.to_raw();
        let live_data_key = DecodedDataStoreKey::new(entity, StorageKey::Nat(5))
            .to_raw()
            .expect("live data key should encode");

        assert_eq!(raw_data.as_bytes().len(), size_of::<u64>() + 1 + 8);
        assert_eq!(
            live_data_key.as_bytes(),
            raw_data.as_bytes(),
            "live data-store keys should use the compact taxonomy wire shape"
        );
        assert_eq!(StorageKey::STORED_SIZE_USIZE, 64);
        assert_eq!(
            RawDataStoreKey::MAX_STORED_SIZE_BYTES,
            size_of::<u64>() as u64 + COMPOSITE_PRIMARY_KEY_MAX_SIZE as u64
        );

        let index_component =
            EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat(3))
                .expect("index component should encode");
        let index_key =
            IndexStoreKey::new(IndexId::new(entity, 1), vec![index_component], primary_key);
        let raw_index: RawIndexStoreKey = index_key.to_raw().expect("raw index key should encode");
        assert!(!raw_index.as_bytes().is_empty());

        let entry = IndexEntryValue::presence_only();
        assert_eq!(
            entry.as_bytes(),
            &[0],
            "taxonomy index entry values carry only the presence witness"
        );
    }

    #[test]
    fn raw_data_store_key_decodes_live_compact_shape() {
        let entity = EntityTag::new(0x1590);
        let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Int(-5))
            .expect("primary key should encode");
        let raw = DataStoreKey::new(entity, primary.clone()).to_raw();

        let decoded = raw.decode().expect("raw data key should decode");

        assert_eq!(decoded.entity_tag(), entity);
        assert_eq!(decoded.primary_key(), &primary);
        assert_eq!(
            RawDataStoreKey::from_bytes(raw.as_bytes())
                .expect("validated raw data key should be retained")
                .as_bytes(),
            raw.as_bytes()
        );
    }

    #[test]
    fn raw_data_store_key_accepts_composite_primary_key_suffix() {
        let entity = EntityTag::new(0x1620);
        let primary_value = composite_primary_key_fixture();
        let primary = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(primary_value))
            .expect("composite primary key should encode");
        let raw = DataStoreKey::new(entity, primary.clone()).to_raw();

        let decoded = raw.decode().expect("raw data key should decode");

        assert_eq!(decoded.entity_tag(), entity);
        assert_eq!(decoded.primary_key(), &primary);
        assert_eq!(
            decoded
                .primary_key()
                .decode()
                .expect("primary-key value should decode"),
            PrimaryKeyValue::Composite(primary_value),
        );
    }

    #[test]
    fn raw_data_store_key_rejects_malformed_live_shape() {
        let short = [0u8; size_of::<u64>()];
        let err = RawDataStoreKey::from_bytes(&short[..])
            .expect_err("raw data key without primary suffix should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::DataStoreKeyTooShort { .. }
        ));

        let mut invalid_primary = vec![0u8; size_of::<u64>()];
        invalid_primary.push(0xFF);
        let err = RawDataStoreKey::from_bytes(&invalid_primary[..])
            .expect_err("raw data key with invalid primary suffix should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::InvalidPrimaryKey(
                CompactPrimaryKeyDecodeError::UnknownKind { tag: 0xFF }
            )
        ));
    }

    #[test]
    fn raw_data_store_key_entity_prefix_range_avoids_primary_key_sentinels() {
        let entity = EntityTag::new(0x1593);
        let range = RawDataStoreKeyRange::entity_prefix(entity);

        assert_eq!(range.lower_inclusive(), &entity.value().to_be_bytes());
        assert_eq!(
            range.upper_exclusive().expect("ordinary entity has upper"),
            &(entity.value() + 1).to_be_bytes()
        );

        let first = DataStoreKey::new(
            entity,
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(0))
                .expect("first primary key should encode"),
        )
        .to_raw();
        let last = DataStoreKey::new(
            entity,
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
                .expect("unit primary key should encode"),
        )
        .to_raw();
        let previous = DataStoreKey::new(
            EntityTag::new(entity.value() - 1),
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
                .expect("unit primary key should encode"),
        )
        .to_raw();
        let next = DataStoreKey::new(
            EntityTag::new(entity.value() + 1),
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(0))
                .expect("next primary key should encode"),
        )
        .to_raw();

        assert!(range.contains(&first));
        assert!(range.contains(&last));
        assert!(!range.contains(&previous));
        assert!(!range.contains(&next));
    }

    #[test]
    fn raw_data_store_key_entity_prefix_range_handles_max_entity_tag() {
        let entity = EntityTag::new(u64::MAX);
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        let key = DataStoreKey::new(
            entity,
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(1))
                .expect("primary key should encode"),
        )
        .to_raw();

        assert_eq!(range.lower_inclusive(), &u64::MAX.to_be_bytes());
        assert_eq!(range.upper_exclusive(), None);
        assert!(range.contains(&key));
    }

    #[test]
    fn raw_index_store_key_decodes_live_compact_shape() {
        let entity = EntityTag::new(0x1591);
        let index_id = IndexId::new(entity, 7);
        let component = EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat(99))
            .expect("index component should encode");
        let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Ulid(Ulid::from_u128(11)))
            .expect("primary key should encode");
        let raw = IndexStoreKey::new(index_id, vec![component.clone()], primary.clone())
            .to_raw()
            .expect("raw index key should encode");

        let decoded = raw.decode().expect("raw index key should decode");

        assert_eq!(decoded.key_kind(), IndexStoreKeyKind::User);
        assert_eq!(decoded.index_id(), index_id);
        assert_eq!(decoded.components(), &[component]);
        assert_eq!(decoded.primary_key(), &primary);
        assert_eq!(
            RawIndexStoreKey::from_bytes(raw.as_bytes())
                .expect("validated raw index key should be retained")
                .as_bytes(),
            raw.as_bytes()
        );
    }

    #[test]
    fn raw_index_store_key_accepts_composite_primary_key_suffix() {
        let entity = EntityTag::new(0x1621);
        let index_id = IndexId::new(entity, 7);
        let component = EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat(99))
            .expect("index component should encode");
        let primary_value = composite_primary_key_fixture();
        let primary = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(primary_value))
            .expect("composite primary key should encode");
        let raw = IndexStoreKey::new(index_id, vec![component.clone()], primary.clone())
            .to_raw()
            .expect("raw index key should encode");

        let decoded = raw.decode().expect("raw index key should decode");

        assert_eq!(decoded.key_kind(), IndexStoreKeyKind::User);
        assert_eq!(decoded.index_id(), index_id);
        assert_eq!(decoded.components(), &[component]);
        assert_eq!(decoded.primary_key(), &primary);
        assert_eq!(
            decoded
                .primary_key()
                .decode()
                .expect("primary-key value should decode"),
            PrimaryKeyValue::Composite(primary_value),
        );
    }

    #[test]
    fn raw_index_store_key_rejects_malformed_live_shape() {
        let err = RawIndexStoreKey::from_bytes(&[])
            .expect_err("empty raw index key should reject before handle open");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::TruncatedIndexSegment {
                segment: "key kind"
            }
        ));

        let wrong_kind = [0xFF];
        let err = RawIndexStoreKey::from_bytes(&wrong_kind[..])
            .expect_err("unknown raw index key kind should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::UnknownIndexKeyKind { .. }
        ));

        let entity = EntityTag::new(0x1592);
        let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(1))
            .expect("primary key should encode");
        let mut truncated = IndexStoreKey::new(IndexId::new(entity, 1), Vec::new(), primary)
            .to_raw()
            .expect("raw index key should encode")
            .as_bytes()
            .to_vec();
        let _ = truncated.pop();
        let err = RawIndexStoreKey::from_bytes(&truncated[..])
            .expect_err("truncated primary-key suffix should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::TruncatedIndexSegment {
                segment: "primary key suffix"
            }
        ));
    }

    #[test]
    fn raw_index_store_key_rejects_empty_component_and_primary_segments() {
        let entity = EntityTag::new(0x1594);
        let index_id = IndexId::new(entity, 3);

        let mut empty_component = Vec::new();
        empty_component.push(IndexStoreKeyKind::User.tag());
        empty_component.extend_from_slice(&index_id.to_bytes());
        empty_component.push(1);
        empty_component.extend_from_slice(&0u16.to_be_bytes());
        let err = RawIndexStoreKey::from_bytes(&empty_component)
            .expect_err("empty component segment should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::EmptyIndexSegment {
                segment: "index component"
            }
        ));

        let mut empty_primary = Vec::new();
        empty_primary.push(IndexStoreKeyKind::User.tag());
        empty_primary.extend_from_slice(&index_id.to_bytes());
        empty_primary.push(0);
        empty_primary.extend_from_slice(&0u16.to_be_bytes());
        let err = RawIndexStoreKey::from_bytes(&empty_primary)
            .expect_err("empty primary-key suffix should reject");
        assert!(matches!(
            err,
            CompactStoreKeyDecodeError::EmptyIndexSegment {
                segment: "primary key suffix"
            }
        ));
    }

    #[test]
    fn raw_index_store_key_taxonomy_matches_live_user_and_system_codecs() {
        let entity = EntityTag::new(0x1595);
        let index_id = IndexId::new(entity, 9);
        let component = EncodedIndexComponent::from_canonical_bytes(vec![0x20, 0xAA, 0xBB]);
        let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat(77))
            .expect("primary key should encode");

        let cases = [
            (IndexStoreKeyKind::User, IndexKeyKind::User),
            (IndexStoreKeyKind::System, IndexKeyKind::System),
        ];
        for (taxonomy_kind, live_kind) in cases {
            let taxonomy_raw = IndexStoreKey::new_with_kind(
                taxonomy_kind,
                index_id,
                vec![component.clone()],
                primary.clone(),
            )
            .to_raw()
            .expect("taxonomy raw index key should encode");
            let live_raw = IndexKey::new_from_components_with_kind(
                &index_id,
                live_kind,
                &[component.as_bytes()],
                StorageKey::Nat(77),
            )
            .to_raw();

            assert_eq!(
                taxonomy_raw.as_bytes(),
                live_raw.as_bytes(),
                "taxonomy store-key wrapper must match the live index codec for {taxonomy_kind:?}"
            );
        }
    }
}
