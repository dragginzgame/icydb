//! Module: db::key_taxonomy::contracts
//! Responsibility: typed entity-key admission, projection, and conversion
//! contracts over the canonical primary-key taxonomy.
//! Does not own: entity schema declarations, cursor semantics, or persisted
//! primary-key byte layout.
//! Boundary: typed keys enter runtime values, row identity, and indexes only
//! through these contracts.

use super::{CompositePrimaryKeyValueError, PrimaryKeyComponent, PrimaryKeyValue};
use crate::{
    error::InternalError,
    value::{RuntimeValueDecode, RuntimeValueEncode, Value},
};
use std::fmt::Debug;

/// Associates an entity with the primitive or composite type used as its
/// primary key.
///
/// Keys are public identifiers rather than authority-bearing capabilities.
/// Typed identity is provided by `Id<Self>`, not by the key itself.
pub trait EntityKey {
    type Key: Copy
        + Debug
        + Eq
        + Ord
        + KeyValueCodec
        + PrimaryKeyEncode
        + PrimaryKeyDecode
        + EntityKeyBytes
        + 'static;
}

/// Typed failures from fixed-width canonical entity-key encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityKeyBytesError {
    /// The caller-provided output buffer has the wrong size.
    BufferLength { expected: usize, actual: usize },

    /// The key value exceeds the maintained fixed-width representation.
    ValueTooLong { len: usize, max: usize },
}

/// Validate that one caller-provided key buffer has the exact required size.
///
/// # Errors
///
/// Returns [`EntityKeyBytesError::BufferLength`] when `out` does not have the
/// required `expected` length.
#[doc(hidden)]
pub const fn validate_entity_key_bytes_buffer(
    out: &[u8],
    expected: usize,
) -> Result<(), EntityKeyBytesError> {
    let actual = out.len();
    if actual != expected {
        return Err(EntityKeyBytesError::BufferLength { expected, actual });
    }

    Ok(())
}

/// Fixed-width canonical byte encoding for entity primary keys.
pub trait EntityKeyBytes {
    /// Exact number of bytes produced.
    const BYTE_LEN: usize;

    /// Write bytes into an exact-length caller-provided buffer.
    ///
    /// # Errors
    ///
    /// Returns [`EntityKeyBytesError`] when the buffer length or key value
    /// cannot satisfy the fixed-width key representation.
    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError>;
}

macro_rules! impl_entity_key_bytes_numeric {
    ($($ty:ty),* $(,)?) => {
        $(
            impl EntityKeyBytes for $ty {
                const BYTE_LEN: usize = ::core::mem::size_of::<Self>();

                fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
                    validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
                    out.copy_from_slice(&self.to_be_bytes());

                    Ok(())
                }
            }
        )*
    };
}

impl_entity_key_bytes_numeric!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

impl EntityKeyBytes for () {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)
    }
}

/// Marker for scalar entity key types that relation fields may target.
///
/// Composite generated key structs deliberately do not implement this marker.
pub trait ScalarRelationTargetKey {}

macro_rules! maintained_scalar_key_types {
    ($consumer:ident) => {
        $consumer!(
            i8,
            i16,
            i32,
            i64,
            i128,
            u8,
            u16,
            u32,
            u64,
            u128,
            crate::types::Account,
            crate::types::Principal,
            crate::types::Subaccount,
            crate::types::Timestamp,
            crate::types::Ulid,
            crate::types::Unit,
            (),
        );
    };
}

macro_rules! impl_scalar_relation_target_key {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ScalarRelationTargetKey for $ty {}
        )*
    };
}

maintained_scalar_key_types!(impl_scalar_relation_target_key);

/// Proves that a scalar relation target key matches the field's declared
/// primitive key type.
pub trait ScalarRelationTargetKeyMatchesDeclaredPrimitive<Declared> {}

impl<T> ScalarRelationTargetKeyMatchesDeclaredPrimitive<T> for T where T: ScalarRelationTargetKey {}

/// Narrow runtime [`Value`] codec for typed primary keys and key-only access
/// surfaces.
///
/// This keeps cursor, access, and key-routing contracts off the wider
/// structured-value conversion surface used by persisted-field codecs and
/// planner queryability metadata.
pub trait KeyValueCodec {
    fn to_key_value(&self) -> Value;

    #[must_use]
    fn from_key_value(value: &Value) -> Option<Self>
    where
        Self: Sized;
}

/// Typed primary-key admission errors.
///
/// This is deliberately separate from compact row-key encoding so composite
/// keys do not inherit scalar-only compatibility lanes.
#[derive(Debug)]
pub enum PrimaryKeyEncodeError {
    UnsupportedComponentKind { kind: &'static str },

    TooFewComponents { count: usize, min: usize },

    TooManyComponents { count: usize, max: usize },

    UnitComponent { index: usize },
}

impl From<CompositePrimaryKeyValueError> for PrimaryKeyEncodeError {
    fn from(err: CompositePrimaryKeyValueError) -> Self {
        match err {
            CompositePrimaryKeyValueError::TooFewComponents { count, min } => {
                Self::TooFewComponents { count, min }
            }
            CompositePrimaryKeyValueError::TooManyComponents { count, max } => {
                Self::TooManyComponents { count, max }
            }
            CompositePrimaryKeyValueError::UnitComponent { index } => Self::UnitComponent { index },
        }
    }
}

impl From<PrimaryKeyEncodeError> for InternalError {
    fn from(_err: PrimaryKeyEncodeError) -> Self {
        Self::serialize_unsupported()
    }
}

/// Narrow typed primary-key encode contract for persistence and indexing
/// admission.
///
/// This keeps typed key ownership off the runtime [`Value`] bridge so
/// persisted identity boundaries can encode directly into the internal
/// decoded primary-key value.
pub trait PrimaryKeyEncode {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError>;
}

/// Narrow typed primary-key decode contract for persistence and indexing
/// boundaries.
///
/// This keeps typed key recovery off the runtime [`Value`] bridge so persisted
/// identity boundaries can decode directly from the internal decoded
/// primary-key value.
pub trait PrimaryKeyDecode: Sized {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError>;
}

fn primary_key_variant_decode_failed(
    _type_name: &'static str,
    _key: &PrimaryKeyValue,
    _expected: &'static str,
) -> InternalError {
    InternalError::store_corruption()
}

fn primary_key_range_decode_failed(
    _type_name: &'static str,
    _key: &PrimaryKeyValue,
) -> InternalError {
    InternalError::store_corruption()
}

macro_rules! impl_primary_key_encode_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PrimaryKeyEncode for $ty {
                fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
                    Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Int64(i64::from(*self))))
                }
            }
        )*
    };
}

macro_rules! impl_primary_key_encode_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PrimaryKeyEncode for $ty {
                fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
                    Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(u64::from(*self))))
                }
            }
        )*
    };
}

macro_rules! impl_key_value_codec {
    ($($ty:ty),* $(,)?) => {
        $(
            impl KeyValueCodec for $ty {
                fn to_key_value(&self) -> Value {
                    RuntimeValueEncode::to_value(self)
                }

                fn from_key_value(value: &Value) -> Option<Self> {
                    RuntimeValueDecode::from_value(value)
                }
            }
        )*
    };
}

maintained_scalar_key_types!(impl_key_value_codec);

// Planner access paths use `Value` only after a typed key has been lowered.
// Keep that canonical carrier explicit and separate from typed-key eligibility.
impl KeyValueCodec for Value {
    fn to_key_value(&self) -> Value {
        self.clone()
    }

    fn from_key_value(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

impl_primary_key_encode_signed!(i8, i16, i32, i64);
impl_primary_key_encode_unsigned!(u8, u16, u32, u64);

impl PrimaryKeyEncode for i128 {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Int128(*self)))
    }
}

impl PrimaryKeyEncode for u128 {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat128(*self)))
    }
}

macro_rules! impl_primary_key_decode_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PrimaryKeyDecode for $ty {
                fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
                    let PrimaryKeyValue::Scalar(PrimaryKeyComponent::Int64(value)) = *key else {
                        return Err(primary_key_variant_decode_failed(
                            ::std::any::type_name::<Self>(),
                            key,
                            "PrimaryKeyComponent::Int64",
                        ));
                    };

                    Self::try_from(value).map_err(|_| {
                        primary_key_range_decode_failed(::std::any::type_name::<Self>(), key)
                    })
                }
            }
        )*
    };
}

macro_rules! impl_primary_key_decode_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PrimaryKeyDecode for $ty {
                fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
                    let PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)) = *key else {
                        return Err(primary_key_variant_decode_failed(
                            ::std::any::type_name::<Self>(),
                            key,
                            "PrimaryKeyComponent::Nat64",
                        ));
                    };

                    Self::try_from(value).map_err(|_| {
                        primary_key_range_decode_failed(::std::any::type_name::<Self>(), key)
                    })
                }
            }
        )*
    };
}

impl_primary_key_decode_signed!(i8, i16, i32, i64);
impl_primary_key_decode_unsigned!(u8, u16, u32, u64);

impl PrimaryKeyDecode for i128 {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Int128(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Int128",
            )),
        }
    }
}

impl PrimaryKeyDecode for u128 {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat128(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Nat128",
            )),
        }
    }
}

impl PrimaryKeyEncode for crate::types::Principal {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Principal(
            *self,
        )))
    }
}

impl PrimaryKeyDecode for crate::types::Principal {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Principal(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Principal",
            )),
        }
    }
}

impl PrimaryKeyEncode for crate::types::Subaccount {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Subaccount(
            *self,
        )))
    }
}

impl PrimaryKeyDecode for crate::types::Subaccount {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Subaccount(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Subaccount",
            )),
        }
    }
}

impl PrimaryKeyEncode for crate::types::Account {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Account(*self)))
    }
}

impl PrimaryKeyDecode for crate::types::Account {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Account(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Account",
            )),
        }
    }
}

impl PrimaryKeyEncode for crate::types::Timestamp {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Timestamp(
            *self,
        )))
    }
}

impl PrimaryKeyDecode for crate::types::Timestamp {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Timestamp(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Timestamp",
            )),
        }
    }
}

impl PrimaryKeyEncode for crate::types::Ulid {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Ulid(*self)))
    }
}

impl PrimaryKeyDecode for crate::types::Ulid {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Ulid(value)) => Ok(value),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Ulid",
            )),
        }
    }
}

impl PrimaryKeyEncode for () {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Unit))
    }
}

impl PrimaryKeyDecode for () {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Unit) => Ok(()),
            _ => Err(primary_key_variant_decode_failed(
                ::std::any::type_name::<Self>(),
                key,
                "PrimaryKeyComponent::Unit",
            )),
        }
    }
}
