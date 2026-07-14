//! Module: db::key_taxonomy
//! Responsibility: compact key vocabulary and canonical primary-key encoder
//! proof, including the scalar-or-composite primary-key value model.
//! Does not own: index-entry value ownership or cursor semantics.
//! Boundary: storage-format layers consume these wrappers as the only row
//! identity vocabulary.
//!
//! Invariant:
//! One accepted entity primary-key namespace has exactly one logical
//! `PrimaryKeyKind`. Heterogeneous primary-key kinds inside one entity remain
//! unsupported; the persisted kind tag exists for validation, diagnostics,
//! cursor/index suffix decoding, and corruption handling.

mod contracts;

use crate::{
    MAX_INDEX_FIELDS,
    db::index::IndexId,
    traits::Repr,
    types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
    value::Value,
};
use std::{cmp::Ordering, fmt};

pub use contracts::{
    EntityKey, EntityKeyBytes, EntityKeyBytesError, KeyValueCodec, PrimaryKeyDecode,
    PrimaryKeyEncode, PrimaryKeyEncodeError, ScalarRelationTargetKey,
    ScalarRelationTargetKeyMatchesDeclaredPrimitive, validate_entity_key_bytes_buffer,
};

const TAG_SIZE: usize = 1;
const NAT64_SIZE: usize = 8;
const INT64_SIZE: usize = 8;
const NAT128_SIZE: usize = 16;
const INT128_SIZE: usize = 16;
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
    Nat64 = 0x01,
    Int64 = 0x02,
    Timestamp = 0x03,
    Ulid = 0x04,
    Principal = 0x05,
    Subaccount = 0x06,
    Account = 0x07,
    Unit = 0x08,
    Composite = 0x09,
    Int128 = 0x0A,
    Nat128 = 0x0B,
}

impl PrimaryKeyKind {
    #[must_use]
    pub(in crate::db) const fn tag(self) -> u8 {
        self as u8
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Nat64),
            0x02 => Some(Self::Int64),
            0x03 => Some(Self::Timestamp),
            0x04 => Some(Self::Ulid),
            0x05 => Some(Self::Principal),
            0x06 => Some(Self::Subaccount),
            0x07 => Some(Self::Account),
            0x08 => Some(Self::Unit),
            0x09 => Some(Self::Composite),
            0x0A => Some(Self::Int128),
            0x0B => Some(Self::Nat128),
            _ => None,
        }
    }

    const fn fixed_payload_len(self) -> Option<usize> {
        match self {
            Self::Nat64 => Some(NAT64_SIZE),
            Self::Int64 => Some(INT64_SIZE),
            Self::Nat128 => Some(NAT128_SIZE),
            Self::Int128 => Some(INT128_SIZE),
            Self::Timestamp => Some(TIMESTAMP_SIZE),
            Self::Ulid => Some(ULID_SIZE),
            Self::Principal | Self::Composite => None,
            Self::Subaccount => Some(SUBACCOUNT_SIZE),
            Self::Account => Some(ACCOUNT_SIZE),
            Self::Unit => Some(0),
        }
    }
}

//
// PrimaryKeyComponent
//

/// One admitted scalar primary-key component before compact canonical encoding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PrimaryKeyComponent {
    Nat64(u64),
    Int64(i64),
    Nat128(u128),
    Int128(i128),
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
            Self::Nat64(_) => PrimaryKeyKind::Nat64,
            Self::Int64(_) => PrimaryKeyKind::Int64,
            Self::Nat128(_) => PrimaryKeyKind::Nat128,
            Self::Int128(_) => PrimaryKeyKind::Int128,
            Self::Timestamp(_) => PrimaryKeyKind::Timestamp,
            Self::Ulid(_) => PrimaryKeyKind::Ulid,
            Self::Principal(_) => PrimaryKeyKind::Principal,
            Self::Subaccount(_) => PrimaryKeyKind::Subaccount,
            Self::Account(_) => PrimaryKeyKind::Account,
            Self::Unit => PrimaryKeyKind::Unit,
        }
    }

    #[must_use]
    pub(in crate::db) const fn as_runtime_value(self) -> Value {
        match self {
            Self::Nat64(value) => Value::Nat64(value),
            Self::Int64(value) => Value::Int64(value),
            Self::Nat128(value) => Value::Nat128(value),
            Self::Int128(value) => Value::Int128(value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Ulid(value) => Value::Ulid(value),
            Self::Principal(value) => Value::Principal(value),
            Self::Subaccount(value) => Value::Subaccount(value),
            Self::Account(value) => Value::Account(value),
            Self::Unit => Value::Unit,
        }
    }

    /// Convert one runtime scalar value into an admitted compact primary-key
    /// component without routing through any legacy scalar-key bridge.
    #[must_use]
    pub(in crate::db) const fn from_runtime_value(value: &Value) -> Option<Self> {
        match value {
            Value::Account(value) => Some(Self::Account(*value)),
            Value::Int64(value) => Some(Self::Int64(*value)),
            Value::Nat64(value) => Some(Self::Nat64(*value)),
            Value::Int128(value) => Some(Self::Int128(*value)),
            Value::Nat128(value) => Some(Self::Nat128(*value)),
            Value::Principal(value) => Some(Self::Principal(*value)),
            Value::Subaccount(value) => Some(Self::Subaccount(*value)),
            Value::Timestamp(value) => Some(Self::Timestamp(*value)),
            Value::Ulid(value) => Some(Self::Ulid(*value)),
            Value::Unit => Some(Self::Unit),
            _ => None,
        }
    }
}

impl Ord for PrimaryKeyComponent {
    fn cmp(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Nat64(a), Self::Nat64(b)) => a.cmp(&b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(&b),
            (Self::Nat128(a), Self::Nat128(b)) => a.cmp(&b),
            (Self::Int128(a), Self::Int128(b)) => a.cmp(&b),
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
    /// Build one admitted composite primary-key value from scalar components.
    ///
    /// Enforces the compact key component-count bounds and rejects `Unit`
    /// components, which are valid only for scalar primary keys.
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
        let Ok(len) = u8::try_from(components.len()) else {
            return Err(CompositePrimaryKeyValueError::TooManyComponents {
                count: components.len(),
                max: MAX_PRIMARY_KEY_FIELDS,
            });
        };

        Ok(Self {
            len,
            components: stored,
        })
    }

    /// Return the number of populated primary-key components.
    #[must_use]
    pub const fn len(self) -> usize {
        self.len as usize
    }

    /// Return whether this value contains no populated components.
    ///
    /// Constructed values are never empty; this is available for collection-like
    /// call sites that operate on borrowed instances.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Return the populated primary-key components in canonical order.
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
/// separately so scalar-only paths cannot silently erase composite identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "primary-key values stay Copy and allocation-free on hot encode/decode paths"
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

    #[must_use]
    pub(in crate::db) fn as_runtime_value(&self) -> Value {
        match self {
            Self::Scalar(component) => component.as_runtime_value(),
            Self::Composite(composite) => Value::List(
                composite
                    .components()
                    .iter()
                    .copied()
                    .map(PrimaryKeyComponent::as_runtime_value)
                    .collect(),
            ),
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn component_runtime_value(&self, component_index: usize) -> Option<Value> {
        match self {
            Self::Scalar(component) => (component_index == 0).then(|| component.as_runtime_value()),
            Self::Composite(composite) => composite
                .components()
                .get(component_index)
                .copied()
                .map(PrimaryKeyComponent::as_runtime_value),
        }
    }
}

impl From<PrimaryKeyComponent> for PrimaryKeyValue {
    fn from(value: PrimaryKeyComponent) -> Self {
        Self::Scalar(value)
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

/// Admission failure for a logical composite primary-key value.
///
/// Owned by the compact key taxonomy layer before any bytes are emitted.
#[derive(Debug)]
pub enum CompositePrimaryKeyValueError {
    /// Fewer than the minimum required composite key components were provided.
    TooFewComponents { count: usize, min: usize },

    /// More than the maximum supported composite key components were provided.
    TooManyComponents { count: usize, max: usize },

    /// A `Unit` component appeared inside a composite primary key.
    UnitComponent { index: usize },
}

#[derive(Debug)]
pub(in crate::db) enum CompactPrimaryKeyEncodeError {
    InvalidAccount,

    PrincipalTooLarge,
}

#[derive(Debug)]
pub(in crate::db) enum CompactPrimaryKeyDecodeError {
    Empty,

    UnknownKind { tag: u8 },

    InvalidLength { kind: PrimaryKeyKind },

    InvalidPrincipalLength,

    InvalidAccount,

    InvalidCompositeCount { count: usize },

    UnitCompositeComponent { index: usize },

    NestedComposite,

    CompositeNotScalar,

    TrailingCompositeBytes,
}

#[derive(Debug)]
pub(in crate::db) enum CompactStoreKeyEncodeError {
    TooManyIndexComponents,

    IndexSegmentTooLarge,
}

#[derive(Debug)]
pub(in crate::db) enum CompactStoreKeyDecodeError {
    DataStoreKeyTooShort,

    TruncatedIndexSegment,

    UnknownIndexKeyKind,

    EmptyIndexSegment,

    InvalidIndexId,

    TrailingIndexBytes,

    TooManyIndexComponents,

    IndexSegmentTooLarge,

    InvalidPrimaryKey(CompactPrimaryKeyDecodeError),
}

impl fmt::Display for CompactPrimaryKeyDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("empty compact primary key"),
            Self::UnknownKind { tag } => {
                write!(f, "unknown compact primary-key kind tag {tag:#04x}")
            }
            Self::InvalidLength { kind } => {
                write!(f, "invalid compact primary-key length for kind {kind:?}")
            }
            Self::InvalidPrincipalLength => {
                f.write_str("invalid compact primary-key principal length")
            }
            Self::InvalidAccount => f.write_str("invalid compact primary-key account payload"),
            Self::InvalidCompositeCount { count } => {
                write!(
                    f,
                    "invalid compact composite primary-key component count {count}"
                )
            }
            Self::UnitCompositeComponent { index } => {
                write!(f, "unit primary-key component at composite index {index}")
            }
            Self::NestedComposite => f.write_str("nested compact composite primary key"),
            Self::CompositeNotScalar => f.write_str("compact composite component is not scalar"),
            Self::TrailingCompositeBytes => {
                f.write_str("trailing bytes after compact composite primary key")
            }
        }
    }
}

impl fmt::Display for CompactStoreKeyDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataStoreKeyTooShort => f.write_str("data-store key is too short"),
            Self::TruncatedIndexSegment => f.write_str("truncated index-store key segment"),
            Self::UnknownIndexKeyKind => f.write_str("unknown index-store key kind"),
            Self::EmptyIndexSegment => f.write_str("empty index-store key segment"),
            Self::InvalidIndexId => f.write_str("invalid index-store key id"),
            Self::TrailingIndexBytes => f.write_str("trailing index-store key bytes"),
            Self::TooManyIndexComponents => f.write_str("too many index-store key components"),
            Self::IndexSegmentTooLarge => f.write_str("index-store key segment is too large"),
            Self::InvalidPrimaryKey(err) => write!(f, "invalid primary key: {err}"),
        }
    }
}

impl From<CompactPrimaryKeyDecodeError> for CompactStoreKeyDecodeError {
    fn from(err: CompactPrimaryKeyDecodeError) -> Self {
        Self::InvalidPrimaryKey(err)
    }
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
        Self::decode_bytes(self.as_bytes())
    }

    pub(in crate::db) fn decode_bytes(
        bytes: &[u8],
    ) -> Result<PrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        match primary_key_kind_from_bytes(bytes)? {
            PrimaryKeyKind::Composite => {
                decode_composite_primary_key_value(bytes).map(PrimaryKeyValue::Composite)
            }
            _ => decode_primary_key_component(bytes).map(PrimaryKeyValue::Scalar),
        }
    }

    #[cfg(test)]
    pub(in crate::db) fn decode_component(
        &self,
    ) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
        decode_primary_key_component(&self.bytes)
    }

    #[cfg(test)]
    pub(in crate::db) fn decode_composite(
        &self,
    ) -> Result<CompositePrimaryKeyValue, CompactPrimaryKeyDecodeError> {
        decode_composite_primary_key_value(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[cfg(test)]
    pub(in crate::db) fn kind(&self) -> Result<PrimaryKeyKind, CompactPrimaryKeyDecodeError> {
        primary_key_kind_from_bytes(self.as_bytes())
    }

    #[cfg(test)]
    pub(in crate::db) fn payload(&self) -> Result<&[u8], CompactPrimaryKeyDecodeError> {
        let _ = self.kind()?;
        Ok(&self.bytes[TAG_SIZE..])
    }
}

impl TryFrom<&[u8]> for EncodedPrimaryKey {
    type Error = CompactPrimaryKeyDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let _ = Self::decode_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }
}

fn primary_key_kind_from_bytes(
    bytes: &[u8],
) -> Result<PrimaryKeyKind, CompactPrimaryKeyDecodeError> {
    let Some(&tag) = bytes.first() else {
        return Err(CompactPrimaryKeyDecodeError::Empty);
    };

    PrimaryKeyKind::from_tag(tag).ok_or(CompactPrimaryKeyDecodeError::UnknownKind { tag })
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
    #[cfg(test)]
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

    #[cfg(test)]
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
            return Err(CompactStoreKeyDecodeError::DataStoreKeyTooShort);
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
    #[must_use]
    pub(in crate::db) fn from_entity_and_primary_key_bytes(
        entity_tag: EntityTag,
        primary_key: &[u8],
    ) -> Self {
        let mut bytes = Vec::with_capacity(size_of::<u64>() + primary_key.len());
        bytes.extend_from_slice(&entity_tag.value().to_be_bytes());
        bytes.extend_from_slice(primary_key);

        Self { bytes }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(in crate::db) fn decode(&self) -> Result<DataStoreKey, CompactStoreKeyDecodeError> {
        DataStoreKey::try_from_raw_bytes(&self.bytes)
    }

    #[must_use]
    pub(in crate::db) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub(in crate::db) fn entity_tag_prefix(&self) -> Option<EntityTag> {
        const ENTITY_TAG_SIZE: usize = size_of::<u64>();
        let prefix = self.bytes.get(..ENTITY_TAG_SIZE)?;
        let mut bytes = [0u8; ENTITY_TAG_SIZE];
        bytes.copy_from_slice(prefix);

        Some(EntityTag::new(u64::from_be_bytes(bytes)))
    }

    #[must_use]
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
        let component_count = u8::try_from(self.components.len())
            .map_err(|_| CompactStoreKeyEncodeError::TooManyIndexComponents)?;
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
            push_len_prefixed(component.as_bytes(), &mut bytes)?;
        }
        push_len_prefixed(self.primary_key.as_bytes(), &mut bytes)?;

        Ok(RawIndexStoreKey { bytes })
    }

    #[cfg(test)]
    pub(in crate::db) fn try_from_raw_bytes(
        bytes: &[u8],
    ) -> Result<Self, CompactStoreKeyDecodeError> {
        let mut input = bytes;

        let key_kind = take_exact(&mut input, TAG_SIZE)?[0];
        let key_kind = IndexStoreKeyKind::from_tag(key_kind)
            .ok_or(CompactStoreKeyDecodeError::UnknownIndexKeyKind)?;

        let index_id = IndexId::from_bytes(take_exact(&mut input, IndexId::STORED_SIZE_USIZE)?)
            .ok_or(CompactStoreKeyDecodeError::InvalidIndexId)?;

        let component_count = usize::from(take_exact(&mut input, TAG_SIZE)?[0]);
        let mut components = Vec::with_capacity(component_count);
        for _ in 0..component_count {
            let component_bytes = take_len_prefixed(&mut input)?;
            components.push(EncodedIndexComponent::try_from(component_bytes)?);
        }

        let primary_key = EncodedPrimaryKey::try_from(take_len_prefixed(&mut input)?)?;
        if !input.is_empty() {
            return Err(CompactStoreKeyDecodeError::TrailingIndexBytes);
        }

        Ok(Self::new_with_kind(
            key_kind,
            index_id,
            components,
            primary_key,
        ))
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn key_kind(&self) -> IndexStoreKeyKind {
        self.key_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn components(&self) -> &[EncodedIndexComponent] {
        &self.components
    }

    #[must_use]
    #[cfg(test)]
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

    #[cfg(test)]
    pub(in crate::db) fn from_bytes(bytes: &[u8]) -> Result<Self, CompactStoreKeyDecodeError> {
        let _ = IndexStoreKey::try_from_raw_bytes(bytes)?;
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    #[cfg(test)]
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
    #[cfg(test)]
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
        PrimaryKeyComponent::Nat64(value) => out.extend_from_slice(&value.to_be_bytes()),
        PrimaryKeyComponent::Int64(value) => out.extend_from_slice(&encode_ordered_i64(value)),
        PrimaryKeyComponent::Nat128(value) => out.extend_from_slice(&value.to_be_bytes()),
        PrimaryKeyComponent::Int128(value) => out.extend_from_slice(&encode_ordered_i128(value)),
        PrimaryKeyComponent::Timestamp(value) => {
            out.extend_from_slice(&encode_ordered_i64(value.repr()));
        }
        PrimaryKeyComponent::Ulid(value) => out.extend_from_slice(&value.to_bytes()),
        PrimaryKeyComponent::Principal(value) => encode_principal(value, out)?,
        PrimaryKeyComponent::Subaccount(value) => out.extend_from_slice(value.as_slice()),
        PrimaryKeyComponent::Account(value) => {
            let bytes = value
                .to_stored_bytes()
                .map_err(|_| CompactPrimaryKeyEncodeError::InvalidAccount)?;
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
        return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });
    }

    match kind {
        PrimaryKeyKind::Nat64 => {
            let mut buf = [0u8; NAT64_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Nat64(u64::from_be_bytes(buf)))
        }
        PrimaryKeyKind::Int64 => {
            let mut buf = [0u8; INT64_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Int64(decode_ordered_i64(buf)))
        }
        PrimaryKeyKind::Nat128 => {
            let mut buf = [0u8; NAT128_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Nat128(u128::from_be_bytes(buf)))
        }
        PrimaryKeyKind::Int128 => {
            let mut buf = [0u8; INT128_SIZE];
            buf.copy_from_slice(payload);
            Ok(PrimaryKeyComponent::Int128(decode_ordered_i128(buf)))
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
                .map_err(|_| CompactPrimaryKeyDecodeError::InvalidAccount)?,
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
        return Err(CompactPrimaryKeyDecodeError::InvalidCompositeCount { count: 0 });
    }

    let Some((&count, rest)) = payload.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });
    };
    let count = usize::from(count);
    if !(2..=MAX_PRIMARY_KEY_FIELDS).contains(&count) {
        return Err(CompactPrimaryKeyDecodeError::InvalidCompositeCount { count });
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
        return Err(CompactPrimaryKeyDecodeError::TrailingCompositeBytes);
    }

    CompositePrimaryKeyValue::try_from_components(&components[..count]).map_err(|err| match err {
        CompositePrimaryKeyValueError::TooFewComponents { count, .. }
        | CompositePrimaryKeyValueError::TooManyComponents { count, .. } => {
            CompactPrimaryKeyDecodeError::InvalidCompositeCount { count }
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
                return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });
            };
            let len = usize::from(len);
            if len > Principal::MAX_LENGTH_IN_BYTES as usize {
                return Err(CompactPrimaryKeyDecodeError::InvalidPrincipalLength);
            }
            TAG_SIZE + TAG_SIZE + len
        }
        PrimaryKeyKind::Composite => return Err(CompactPrimaryKeyDecodeError::NestedComposite),
        _ => {
            let Some(payload_len) = kind.fixed_payload_len() else {
                return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });
            };
            TAG_SIZE + payload_len
        }
    };
    if input.len() < total_len {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength { kind });
    }

    let (component, rest) = input.split_at(total_len);
    *input = rest;

    Ok(component)
}

fn encode_principal(
    value: Principal,
    out: &mut Vec<u8>,
) -> Result<(), CompactPrimaryKeyEncodeError> {
    let bytes = value
        .stored_bytes()
        .map_err(|_| CompactPrimaryKeyEncodeError::PrincipalTooLarge)?;
    let len =
        u8::try_from(bytes.len()).map_err(|_| CompactPrimaryKeyEncodeError::PrincipalTooLarge)?;

    out.push(len);
    out.extend_from_slice(bytes);

    Ok(())
}

fn decode_principal(payload: &[u8]) -> Result<PrimaryKeyComponent, CompactPrimaryKeyDecodeError> {
    let Some((&len, bytes)) = payload.split_first() else {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
        });
    };

    let len = usize::from(len);
    if len > Principal::MAX_LENGTH_IN_BYTES as usize {
        return Err(CompactPrimaryKeyDecodeError::InvalidPrincipalLength);
    }

    if bytes.len() != len {
        return Err(CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
        });
    }

    Ok(PrimaryKeyComponent::Principal(Principal::from_slice(bytes)))
}

#[must_use]
const fn encode_ordered_i64(value: i64) -> [u8; INT64_SIZE] {
    (value.cast_unsigned() ^ (1u64 << 63)).to_be_bytes()
}

#[must_use]
const fn decode_ordered_i64(bytes: [u8; INT64_SIZE]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1u64 << 63)).cast_signed()
}

#[must_use]
const fn encode_ordered_i128(value: i128) -> [u8; INT128_SIZE] {
    (value.cast_unsigned() ^ (1u128 << 127)).to_be_bytes()
}

#[must_use]
const fn decode_ordered_i128(bytes: [u8; INT128_SIZE]) -> i128 {
    (u128::from_be_bytes(bytes) ^ (1u128 << 127)).cast_signed()
}

fn push_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CompactStoreKeyEncodeError> {
    let len =
        u16::try_from(bytes.len()).map_err(|_| CompactStoreKeyEncodeError::IndexSegmentTooLarge)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);

    Ok(())
}

const fn take_exact<'a>(
    input: &mut &'a [u8],
    len: usize,
) -> Result<&'a [u8], CompactStoreKeyDecodeError> {
    if input.len() < len {
        return Err(CompactStoreKeyDecodeError::TruncatedIndexSegment);
    }

    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_len_prefixed<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], CompactStoreKeyDecodeError> {
    let len_bytes = take_exact(input, LENGTH_PREFIX_SIZE)?;
    let mut len = [0u8; LENGTH_PREFIX_SIZE];
    len.copy_from_slice(len_bytes);
    let len = usize::from(u16::from_be_bytes(len));
    if len == 0 {
        return Err(CompactStoreKeyDecodeError::EmptyIndexSegment);
    }
    take_exact(input, len)
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

    let key_kind = take_exact(&mut input, TAG_SIZE)?[0];
    let key_kind = IndexStoreKeyKind::from_tag(key_kind)
        .ok_or(CompactStoreKeyDecodeError::UnknownIndexKeyKind)?;

    let index_id = IndexId::from_bytes(take_exact(&mut input, IndexId::STORED_SIZE_USIZE)?)
        .ok_or(CompactStoreKeyDecodeError::InvalidIndexId)?;

    let component_count = usize::from(take_exact(&mut input, TAG_SIZE)?[0]);
    if component_count > MAX_INDEX_FIELDS {
        return Err(CompactStoreKeyDecodeError::TooManyIndexComponents);
    }

    let mut components = Vec::with_capacity(component_count);
    for _ in 0..component_count {
        let component = take_len_prefixed(&mut input)?;
        if component.len() > INDEX_COMPONENT_MAX_SIZE {
            return Err(CompactStoreKeyDecodeError::IndexSegmentTooLarge);
        }
        components.push(component);
    }

    let primary_key = take_len_prefixed(&mut input)?;
    if primary_key.len() > INDEX_PRIMARY_KEY_MAX_SIZE {
        return Err(CompactStoreKeyDecodeError::IndexSegmentTooLarge);
    }
    if !input.is_empty() {
        return Err(CompactStoreKeyDecodeError::TrailingIndexBytes);
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
            PrimaryKeyKind::Nat64 | PrimaryKeyKind::Int64 | PrimaryKeyKind::Timestamp => NAT64_SIZE,
            PrimaryKeyKind::Nat128 | PrimaryKeyKind::Int128 => NAT128_SIZE,
            PrimaryKeyKind::Ulid => ULID_SIZE,
            PrimaryKeyKind::Subaccount => SUBACCOUNT_SIZE,
            PrimaryKeyKind::Account => ACCOUNT_SIZE,
            PrimaryKeyKind::Unit => 0,
            PrimaryKeyKind::Composite => COMPOSITE_PRIMARY_KEY_MAX_SIZE - TAG_SIZE,
        }
}

#[cfg(test)]
mod tests;
