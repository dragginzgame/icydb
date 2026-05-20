//! Module: db::key_taxonomy
//! Responsibility: 0.159 compact key vocabulary and canonical primary-key
//! encoder proof.
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
            _ => None,
        }
    }

    const fn fixed_payload_len(self) -> Option<usize> {
        match self {
            Self::Nat => Some(NAT_SIZE),
            Self::Int => Some(INT_SIZE),
            Self::Timestamp => Some(TIMESTAMP_SIZE),
            Self::Ulid => Some(ULID_SIZE),
            Self::Principal => None,
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
        }
    }
}

//
// PrimaryKeyValue
//

/// Logical admitted primary-key value before compact canonical encoding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) enum PrimaryKeyValue {
    Nat(u64),
    Int(i64),
    Timestamp(Timestamp),
    Ulid(Ulid),
    Principal(Principal),
    Subaccount(Subaccount),
    Account(Account),
    Unit,
}

impl PrimaryKeyValue {
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

impl From<StorageKey> for PrimaryKeyValue {
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

impl From<PrimaryKeyValue> for StorageKey {
    fn from(value: PrimaryKeyValue) -> Self {
        match value {
            PrimaryKeyValue::Nat(value) => Self::Nat(value),
            PrimaryKeyValue::Int(value) => Self::Int(value),
            PrimaryKeyValue::Timestamp(value) => Self::Timestamp(value),
            PrimaryKeyValue::Ulid(value) => Self::Ulid(value),
            PrimaryKeyValue::Principal(value) => Self::Principal(value),
            PrimaryKeyValue::Subaccount(value) => Self::Subaccount(value),
            PrimaryKeyValue::Account(value) => Self::Account(value),
            PrimaryKeyValue::Unit => Self::Unit,
        }
    }
}

impl Ord for PrimaryKeyValue {
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

impl PartialOrd for PrimaryKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//
// Errors
//

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
        value: PrimaryKeyValue,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(max_encoded_primary_key_len(value.kind()));
        encode_primary_key_value(value, &mut bytes)?;
        Ok(Self { bytes })
    }

    pub(in crate::db) fn decode(&self) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        decode_primary_key_value(&self.bytes)
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
        value: PrimaryKeyValue,
    ) -> Result<Self, CompactPrimaryKeyEncodeError> {
        let mut bytes = Vec::with_capacity(max_encoded_primary_key_len(value.kind()));
        encode_primary_key_value(value, &mut bytes)?;
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
pub(in crate::db) struct RawDataStoreKey {
    bytes: Vec<u8>,
}

impl RawDataStoreKey {
    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = DataStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    pub(in crate::db) fn decode(&self) -> Result<DataStoreKey, CompactStoreKeyDecodeError> {
        DataStoreKey::try_from_raw_bytes(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
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
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct RawIndexStoreKey {
    bytes: Vec<u8>,
}

impl RawIndexStoreKey {
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
}

/// Secondary-index value. Primary-key membership belongs to the key, so this
/// value carries only a storage-owned presence/existence witness.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct IndexEntryValue {
    bytes: Vec<u8>,
}

impl IndexEntryValue {
    #[must_use]
    pub(in crate::db) fn presence_only() -> Self {
        Self { bytes: vec![0] }
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn encode_primary_key_value(
    value: PrimaryKeyValue,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    out.push(value.kind().tag());
    encode_primary_key_payload(value, out)
}

fn encode_primary_key_payload(
    value: PrimaryKeyValue,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    match value {
        PrimaryKeyValue::Nat(value) => out.extend_from_slice(&value.to_be_bytes()),
        PrimaryKeyValue::Int(value) => out.extend_from_slice(&encode_ordered_i64(value)),
        PrimaryKeyValue::Timestamp(value) => {
            out.extend_from_slice(&encode_ordered_i64(value.repr()));
        }
        PrimaryKeyValue::Ulid(value) => out.extend_from_slice(&value.to_bytes()),
        PrimaryKeyValue::Principal(value) => encode_principal(value, out)?,
        PrimaryKeyValue::Subaccount(value) => out.extend_from_slice(value.as_slice()),
        PrimaryKeyValue::Account(value) => {
            let bytes = value.to_stored_bytes().map_err(|_| {
                CompactPrimaryKeyEncodeError::InvalidAccount {
                    reason: "account payload failed fixed stored encoding",
                }
            })?;
            out.extend_from_slice(&bytes);
        }
        PrimaryKeyValue::Unit => {}
    }

    Ok(())
}

fn decode_primary_key_value(bytes: &[u8]) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
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
            Ok(PrimaryKeyValue::Nat(u64::from_be_bytes(buf)))
        }
        PrimaryKeyKind::Int => {
            let mut buf = [0u8; INT_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyValue::Int(decode_ordered_i64(buf)))
        }
        PrimaryKeyKind::Timestamp => {
            let mut buf = [0u8; TIMESTAMP_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyValue::Timestamp(Timestamp::from_repr(
                decode_ordered_i64(buf),
            )))
        }
        PrimaryKeyKind::Ulid => {
            let mut buf = [0u8; ULID_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyValue::Ulid(Ulid::from_bytes(buf)))
        }
        PrimaryKeyKind::Principal => decode_principal(payload),
        PrimaryKeyKind::Subaccount => {
            let mut buf = [0u8; SUBACCOUNT_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyValue::Subaccount(Subaccount::from_array(buf)))
        }
        PrimaryKeyKind::Account => Ok(PrimaryKeyValue::Account(
            Account::try_from_bytes(payload)
                .map_err(|reason| CompactPrimaryKeyDecodeError::InvalidAccount { reason })?,
        )),
        PrimaryKeyKind::Unit => Ok(PrimaryKeyValue::Unit),
    }
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

fn decode_principal(payload: &[u8]) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
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

    Ok(PrimaryKeyValue::Principal(Principal::from_slice(bytes)))
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

const fn max_encoded_primary_key_len(kind: PrimaryKeyKind) -> usize {
    TAG_SIZE
        + match kind {
            PrimaryKeyKind::Principal => TAG_SIZE + Principal::MAX_LENGTH_IN_BYTES as usize,
            PrimaryKeyKind::Nat | PrimaryKeyKind::Int | PrimaryKeyKind::Timestamp => NAT_SIZE,
            PrimaryKeyKind::Ulid => ULID_SIZE,
            PrimaryKeyKind::Subaccount => SUBACCOUNT_SIZE,
            PrimaryKeyKind::Account => ACCOUNT_SIZE,
            PrimaryKeyKind::Unit => 0,
        }
}

#[cfg(test)]
mod tests {
    use super::{
        CompactPrimaryKeyDecodeError, CompactStoreKeyDecodeError, DataStoreKey,
        EncodedIndexComponent, EncodedPrimaryKey, IndexEntryValue, IndexStoreKey,
        IndexStoreKeyKind, PrimaryKeyKind, PrimaryKeyValue, RawDataStoreKey, RawDataStoreKeyRange,
        RawIndexStoreKey,
    };
    use crate::{
        db::{
            data::DataKey,
            index::{IndexId, IndexKey, IndexKeyKind},
        },
        traits::Repr,
        types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
        value::StorageKey,
    };

    fn roundtrip(value: PrimaryKeyValue) {
        let encoded = EncodedPrimaryKey::encode(value).expect("primary key should encode");
        assert_eq!(encoded.kind().expect("kind should decode"), value.kind());
        assert_eq!(encoded.decode().expect("primary key should decode"), value);
    }

    #[test]
    fn compact_primary_key_roundtrip_per_key_type() {
        let values = [
            PrimaryKeyValue::Nat(42),
            PrimaryKeyValue::Int(-42),
            PrimaryKeyValue::Timestamp(Timestamp::from_millis(-42)),
            PrimaryKeyValue::Ulid(Ulid::from_u128(42)),
            PrimaryKeyValue::Principal(Principal::from_slice(&[1, 2, 3])),
            PrimaryKeyValue::Subaccount(Subaccount::from_array([7; 32])),
            PrimaryKeyValue::Account(Account::dummy(7)),
            PrimaryKeyValue::Unit,
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
        roundtrip(PrimaryKeyValue::Principal(max));

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

        roundtrip(PrimaryKeyValue::Subaccount(Subaccount::from_array(
            [0xCC; 32],
        )));
    }

    #[test]
    fn compact_primary_key_validates_account_payload() {
        roundtrip(PrimaryKeyValue::Account(Account::dummy(9)));

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
        let encoded = EncodedPrimaryKey::encode(PrimaryKeyValue::Unit)
            .expect("unit primary key should encode");

        assert_eq!(encoded.as_bytes(), &[PrimaryKeyKind::Unit.tag()]);
        assert_eq!(
            encoded.decode().expect("unit primary key should decode"),
            PrimaryKeyValue::Unit
        );
    }

    #[test]
    fn compact_primary_key_ordering_matches_logical_order_per_type() {
        let cases = [
            (PrimaryKeyValue::Nat(1), PrimaryKeyValue::Nat(2)),
            (PrimaryKeyValue::Int(-2), PrimaryKeyValue::Int(1)),
            (
                PrimaryKeyValue::Timestamp(Timestamp::from_millis(-1)),
                PrimaryKeyValue::Timestamp(Timestamp::from_millis(1)),
            ),
            (
                PrimaryKeyValue::Ulid(Ulid::from_u128(1)),
                PrimaryKeyValue::Ulid(Ulid::from_u128(2)),
            ),
            (
                PrimaryKeyValue::Principal(Principal::from_slice(&[9])),
                PrimaryKeyValue::Principal(Principal::from_slice(&[1, 0])),
            ),
            (
                PrimaryKeyValue::Subaccount(Subaccount::from_array([1; 32])),
                PrimaryKeyValue::Subaccount(Subaccount::from_array([2; 32])),
            ),
            (
                PrimaryKeyValue::Account(Account::dummy(1)),
                PrimaryKeyValue::Account(Account::dummy(2)),
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
            EncodedPrimaryKey::encode(PrimaryKeyValue::Timestamp(value))
                .expect("timestamp primary key should encode")
        });
        encoded.sort();

        let decoded = encoded.map(
            |value| match value.decode().expect("timestamp should decode") {
                PrimaryKeyValue::Timestamp(value) => value.repr(),
                other => panic!("expected timestamp primary key, got {other:?}"),
            },
        );
        let expected = values.map(|value| value.repr());

        assert_eq!(decoded, expected);
    }

    #[test]
    fn compact_primary_key_principal_length_first_ordering_fixture() {
        let short = PrimaryKeyValue::Principal(Principal::from_slice(&[9]));
        let long = PrimaryKeyValue::Principal(Principal::from_slice(&[1, 0]));

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
            (PrimaryKeyValue::Nat(7), PrimaryKeyValue::Nat(8)),
            (PrimaryKeyValue::Int(-7), PrimaryKeyValue::Int(8)),
            (
                PrimaryKeyValue::Timestamp(Timestamp::from_millis(-7)),
                PrimaryKeyValue::Timestamp(Timestamp::from_millis(8)),
            ),
            (
                PrimaryKeyValue::Ulid(Ulid::from_u128(7)),
                PrimaryKeyValue::Ulid(Ulid::from_u128(8)),
            ),
            (
                PrimaryKeyValue::Principal(Principal::from_slice(&[7])),
                PrimaryKeyValue::Principal(Principal::from_slice(&[8])),
            ),
            (
                PrimaryKeyValue::Subaccount(Subaccount::from_array([7; 32])),
                PrimaryKeyValue::Subaccount(Subaccount::from_array([8; 32])),
            ),
            (
                PrimaryKeyValue::Account(Account::dummy(7)),
                PrimaryKeyValue::Account(Account::dummy(8)),
            ),
            (PrimaryKeyValue::Unit, PrimaryKeyValue::Unit),
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
        let primary_key = PrimaryKeyValue::from(storage_key);

        assert_eq!(
            EncodedPrimaryKey::encode(primary_key)
                .expect("storage-key bridge should encode")
                .decode()
                .expect("storage-key bridge should decode"),
            primary_key
        );
    }

    #[test]
    fn key_taxonomy_wrappers_match_live_compact_data_key_cut() {
        let entity = EntityTag::new(0x159);
        let primary_key =
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(5)).expect("primary key should encode");
        let data_key = DataStoreKey::new(entity, primary_key.clone());
        let raw_data: RawDataStoreKey = data_key.to_raw();
        let live_data_key = DataKey::new(entity, StorageKey::Nat(5))
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
            DataKey::STORED_SIZE_BYTES,
            size_of::<u64>() as u64 + 1 + u64::from(Account::STORED_SIZE)
        );

        let index_component =
            EncodedIndexComponent::encode_primary_overlap(PrimaryKeyValue::Nat(3))
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
        let primary =
            EncodedPrimaryKey::encode(PrimaryKeyValue::Int(-5)).expect("primary key should encode");
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
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(0))
                .expect("first primary key should encode"),
        )
        .to_raw();
        let last = DataStoreKey::new(
            entity,
            EncodedPrimaryKey::encode(PrimaryKeyValue::Unit)
                .expect("unit primary key should encode"),
        )
        .to_raw();
        let previous = DataStoreKey::new(
            EntityTag::new(entity.value() - 1),
            EncodedPrimaryKey::encode(PrimaryKeyValue::Unit)
                .expect("unit primary key should encode"),
        )
        .to_raw();
        let next = DataStoreKey::new(
            EntityTag::new(entity.value() + 1),
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(0))
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
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(1)).expect("primary key should encode"),
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
        let component = EncodedIndexComponent::encode_primary_overlap(PrimaryKeyValue::Nat(99))
            .expect("index component should encode");
        let primary = EncodedPrimaryKey::encode(PrimaryKeyValue::Ulid(Ulid::from_u128(11)))
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
        let primary =
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(1)).expect("primary key should encode");
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
        let primary =
            EncodedPrimaryKey::encode(PrimaryKeyValue::Nat(77)).expect("primary key should encode");

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
