//! Module: db::data::persisted_row::codec
//! Defines the persisted scalar row-slot encoding and borrowed decoding helpers
//! used by runtime row access.

use crate::{
    db::data::{
        decode_account, decode_decimal, decode_int, decode_int128, decode_list_item,
        decode_map_entry, decode_nat, decode_nat128, decode_storage_key_binary_value_bytes,
        decode_structural_field_by_kind_bytes, decode_structural_value_storage_blob_bytes,
        decode_structural_value_storage_bool_bytes, decode_structural_value_storage_bytes,
        decode_structural_value_storage_date_bytes, decode_structural_value_storage_duration_bytes,
        decode_structural_value_storage_float32_bytes,
        decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
        decode_structural_value_storage_principal_bytes,
        decode_structural_value_storage_subaccount_bytes,
        decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
        decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
        decode_text, encode_account, encode_decimal, encode_int, encode_int128, encode_list_item,
        encode_map_entry, encode_nat, encode_nat128, encode_storage_key_binary_value_bytes,
        encode_structural_field_by_kind_bytes, encode_structural_value_storage_blob_bytes,
        encode_structural_value_storage_bool_bytes, encode_structural_value_storage_bytes,
        encode_structural_value_storage_date_bytes, encode_structural_value_storage_duration_bytes,
        encode_structural_value_storage_float32_bytes,
        encode_structural_value_storage_float64_bytes, encode_structural_value_storage_i64_bytes,
        encode_structural_value_storage_null_bytes,
        encode_structural_value_storage_principal_bytes,
        encode_structural_value_storage_subaccount_bytes,
        encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
        encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
        encode_text, structural_value_storage_bytes_are_null, supports_storage_key_binary_kind,
    },
    error::InternalError,
    model::field::{FieldKind, ScalarCodec},
    traits::{FieldTypeMeta, PersistedStructuredFieldCodec, ValueCodec},
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid, Unit,
    },
    value::Value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::str;

const SCALAR_SLOT_PREFIX: u8 = 0xFF;
const SCALAR_SLOT_TAG_NULL: u8 = 0;
const SCALAR_SLOT_TAG_VALUE: u8 = 1;

const SCALAR_BOOL_PAYLOAD_LEN: usize = 1;
const SCALAR_WORD32_PAYLOAD_LEN: usize = 4;
const SCALAR_WORD64_PAYLOAD_LEN: usize = 8;
const SCALAR_ULID_PAYLOAD_LEN: usize = 16;
const SCALAR_SUBACCOUNT_PAYLOAD_LEN: usize = 32;

const SCALAR_BOOL_FALSE_TAG: u8 = 0;
const SCALAR_BOOL_TRUE_TAG: u8 = 1;

///
/// ScalarValueRef
///
/// ScalarValueRef is the borrowed-or-copy scalar payload view returned by the
/// slot-reader fast path.
/// It preserves cheap references for text/blob payloads while keeping fixed
/// width scalar wrappers as copy values.
///

#[derive(Clone, Copy, Debug)]
pub enum ScalarValueRef<'a> {
    Blob(&'a [u8]),
    Bool(bool),
    Date(Date),
    Duration(Duration),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Principal(Principal),
    Subaccount(Subaccount),
    Text(&'a str),
    Timestamp(Timestamp),
    Uint(u64),
    Ulid(Ulid),
    Unit,
}

impl ScalarValueRef<'_> {
    /// Materialize this scalar view into the runtime `Value` enum.
    #[must_use]
    pub fn into_value(self) -> Value {
        match self {
            Self::Blob(value) => Value::Blob(value.to_vec()),
            Self::Bool(value) => Value::Bool(value),
            Self::Date(value) => Value::Date(value),
            Self::Duration(value) => Value::Duration(value),
            Self::Float32(value) => Value::Float32(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Int(value) => Value::Int(value),
            Self::Principal(value) => Value::Principal(value),
            Self::Subaccount(value) => Value::Subaccount(value),
            Self::Text(value) => Value::Text(value.to_owned()),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Uint(value) => Value::Uint(value),
            Self::Ulid(value) => Value::Ulid(value),
            Self::Unit => Value::Unit,
        }
    }
}

///
/// ScalarSlotValueRef
///
/// ScalarSlotValueRef preserves the distinction between a missing slot and an
/// explicitly persisted `NULL` scalar payload.
/// The outer `Option` from `SlotReader::get_scalar` therefore still means
/// "slot absent".
///

#[derive(Clone, Copy, Debug)]
pub enum ScalarSlotValueRef<'a> {
    Null,
    Value(ScalarValueRef<'a>),
}

///
/// PersistedScalar
///
/// PersistedScalar defines the canonical binary payload codec for one scalar
/// leaf type.
/// Derive-generated persisted-row materializers and writers use this trait to
/// avoid routing scalar fields back through a generic structural envelope.
///

pub trait PersistedScalar: Sized {
    /// Canonical scalar codec identifier used by schema/runtime metadata.
    const CODEC: ScalarCodec;

    /// Encode this scalar value into its codec-specific payload bytes.
    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError>;

    /// Decode this scalar value from its codec-specific payload bytes.
    fn decode_scalar_payload(bytes: &[u8], field_name: &'static str)
    -> Result<Self, InternalError>;
}

/// Encode one persisted slot payload using the stricter schema-owned `ByKind`
/// storage contract.
pub fn encode_persisted_slot_payload_by_kind<T>(
    value: &T,
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: ValueCodec,
{
    if supports_storage_key_binary_kind(kind) {
        return encode_storage_key_binary_value_bytes(kind, &value.to_value(), field_name)?
            .ok_or_else(|| {
                InternalError::persisted_row_field_encode_failed(
                    field_name,
                    "storage-key binary lane rejected a supported field kind",
                )
            });
    }

    encode_structural_field_by_kind_bytes(kind, &value.to_value(), field_name)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

/// Encode one persisted scalar slot payload using the canonical scalar envelope.
pub fn encode_persisted_scalar_slot_payload<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedScalar,
{
    let payload = value.encode_scalar_payload()?;
    let mut encoded = Vec::with_capacity(payload.len() + 2);
    encoded.push(SCALAR_SLOT_PREFIX);
    encoded.push(SCALAR_SLOT_TAG_VALUE);
    encoded.extend_from_slice(&payload);

    if encoded.len() < 2 {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "scalar payload envelope underflow",
        ));
    }

    Ok(encoded)
}

/// Encode one optional persisted scalar slot payload preserving explicit `NULL`.
pub fn encode_persisted_option_scalar_slot_payload<T>(
    value: &Option<T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedScalar,
{
    match value {
        Some(value) => encode_persisted_scalar_slot_payload(value, field_name),
        None => Ok(vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_NULL]),
    }
}

// Decode one `ByKind` structural persisted payload, preserving the explicit
// null sentinel instead of forcing each wrapper to repeat the same branch.
fn decode_persisted_structural_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: ValueCodec,
{
    let value = if supports_storage_key_binary_kind(kind) {
        decode_storage_key_binary_value_bytes(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
            .ok_or_else(|| {
                InternalError::persisted_row_field_decode_failed(
                    field_name,
                    "storage-key binary lane rejected a supported field kind",
                )
            })?
    } else {
        decode_structural_field_by_kind_bytes(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
    };

    if matches!(value, Value::Null) {
        return Ok(None);
    }

    T::from_value(&value).map(Some).ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!(
                "value payload does not match {}",
                std::any::type_name::<T>()
            ),
        )
    })
}

/// Decode one persisted slot payload using the stricter schema-owned `ByKind`
/// storage contract.
pub fn decode_persisted_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: ValueCodec,
{
    let value = if supports_storage_key_binary_kind(kind) {
        decode_storage_key_binary_value_bytes(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
            .ok_or_else(|| {
                InternalError::persisted_row_field_decode_failed(
                    field_name,
                    "storage-key binary lane rejected a supported field kind",
                )
            })?
    } else {
        decode_structural_field_by_kind_bytes(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
    };

    T::from_value(&value).ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!(
                "value payload does not match {}",
                std::any::type_name::<T>()
            ),
        )
    })
}

/// Decode one non-null persisted slot payload through the stricter schema-owned
/// `ByKind` storage contract.
pub fn decode_persisted_non_null_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: ValueCodec,
{
    decode_persisted_structural_slot_payload_by_kind(bytes, kind, field_name)?.ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            "unexpected null for non-nullable field",
        )
    })
}

/// Decode one optional persisted slot payload preserving the explicit null
/// sentinel under the stricter schema-owned `ByKind` storage contract.
pub fn decode_persisted_option_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: ValueCodec,
{
    decode_persisted_structural_slot_payload_by_kind(bytes, kind, field_name)
}

/// Decode one persisted slot payload using the field type's own runtime field
/// metadata.
pub fn decode_persisted_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: FieldTypeMeta + PersistedStructuredFieldCodec + ValueCodec,
{
    match T::STORAGE_DECODE {
        crate::model::field::FieldStorageDecode::ByKind => {
            decode_persisted_non_null_slot_payload_by_kind(bytes, T::KIND, field_name)
        }
        crate::model::field::FieldStorageDecode::Value => {
            decode_persisted_custom_slot_payload(bytes, field_name)
        }
    }
}

/// Decode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn decode_persisted_option_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: FieldTypeMeta + PersistedStructuredFieldCodec + ValueCodec,
{
    match T::STORAGE_DECODE {
        crate::model::field::FieldStorageDecode::ByKind => {
            decode_persisted_option_slot_payload_by_kind(bytes, T::KIND, field_name)
        }
        crate::model::field::FieldStorageDecode::Value => {
            decode_persisted_custom_slot_payload(bytes, field_name)
        }
    }
}

macro_rules! impl_persisted_structured_signed_scalar_codec {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedStructuredFieldCodec for $ty {
                fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(encode_structural_value_storage_i64_bytes(i64::from(*self)))
                }

                fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
                    let value = decode_structural_value_storage_i64_bytes(bytes)
                        .map_err(InternalError::persisted_row_decode_failed)?;

                    <$ty>::try_from(value).map_err(|_| {
                        InternalError::persisted_row_decode_failed(format!(
                            "value payload does not match {}",
                            std::any::type_name::<$ty>()
                        ))
                    })
                }
            }
        )*
    };
}

macro_rules! impl_persisted_structured_unsigned_scalar_codec {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedStructuredFieldCodec for $ty {
                fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(encode_structural_value_storage_u64_bytes(u64::from(*self)))
                }

                fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
                    let value = decode_structural_value_storage_u64_bytes(bytes)
                        .map_err(InternalError::persisted_row_decode_failed)?;

                    <$ty>::try_from(value).map_err(|_| {
                        InternalError::persisted_row_decode_failed(format!(
                            "value payload does not match {}",
                            std::any::type_name::<$ty>()
                        ))
                    })
                }
            }
        )*
    };
}

impl PersistedStructuredFieldCodec for bool {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_bool_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_bool_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for String {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_text(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_text(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Blob {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_blob_bytes(self.as_slice()))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_blob_bytes(bytes)
            .map(Self::from)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Account {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        encode_account(*self)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_account(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Decimal {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_decimal(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_decimal(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Float32 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_float32_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_float32_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Float64 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_float64_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_float64_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Principal {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        encode_structural_value_storage_principal_bytes(*self)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_principal_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Subaccount {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_subaccount_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_subaccount_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Timestamp {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_timestamp_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_timestamp_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Date {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_date_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_date_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Duration {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_duration_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_duration_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Ulid {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_ulid_bytes(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_ulid_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Int128 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_int128(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_int128(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Nat128 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_nat128(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_nat128(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Int {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_int(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_int(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Nat {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_nat(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_nat(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Unit {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_structural_value_storage_unit_bytes())
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_unit_bytes(bytes)
            .map(|()| Self)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Value {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        encode_structural_value_storage_bytes(self)
            .map_err(InternalError::persisted_row_encode_failed)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structural_value_storage_bytes(bytes)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl_persisted_structured_signed_scalar_codec!(i8, i16, i32, i64);
impl_persisted_structured_unsigned_scalar_codec!(u8, u16, u32, u64);

impl<T> PersistedStructuredFieldCodec for Vec<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        let item_payloads = self
            .iter()
            .map(PersistedStructuredFieldCodec::encode_persisted_structured_payload)
            .collect::<Result<Vec<_>, _>>()?;
        let item_slices = item_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();

        Ok(encode_list_item(item_slices.as_slice()))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        let item_bytes =
            decode_list_item(bytes).map_err(InternalError::persisted_row_decode_failed)?;

        item_bytes
            .into_iter()
            .map(T::decode_persisted_structured_payload)
            .collect()
    }
}

impl<T> PersistedStructuredFieldCodec for Option<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        match self {
            Some(value) => value.encode_persisted_structured_payload(),
            None => Ok(encode_structural_value_storage_null_bytes()),
        }
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        if structural_value_storage_bytes_are_null(bytes)
            .map_err(InternalError::persisted_row_decode_failed)?
        {
            return Ok(None);
        }

        T::decode_persisted_structured_payload(bytes).map(Some)
    }
}

impl<T> PersistedStructuredFieldCodec for Box<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        self.as_ref().encode_persisted_structured_payload()
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        T::decode_persisted_structured_payload(bytes).map(Self::new)
    }
}

impl<T> PersistedStructuredFieldCodec for BTreeSet<T>
where
    T: Ord + PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        let item_payloads = self
            .iter()
            .map(PersistedStructuredFieldCodec::encode_persisted_structured_payload)
            .collect::<Result<Vec<_>, _>>()?;
        let item_slices = item_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();

        Ok(encode_list_item(item_slices.as_slice()))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        let item_bytes =
            decode_list_item(bytes).map_err(InternalError::persisted_row_decode_failed)?;
        let mut out = Self::new();
        for item_bytes in item_bytes {
            let item = T::decode_persisted_structured_payload(item_bytes)?;
            if !out.insert(item) {
                return Err(InternalError::persisted_row_decode_failed(format!(
                    "value payload does not match BTreeSet<{}>",
                    std::any::type_name::<T>()
                )));
            }
        }

        Ok(out)
    }
}

impl<K, V> PersistedStructuredFieldCodec for BTreeMap<K, V>
where
    K: Ord + PersistedStructuredFieldCodec,
    V: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        let entry_payloads = self
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.encode_persisted_structured_payload()?,
                    value.encode_persisted_structured_payload()?,
                ))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;
        let entry_slices = entry_payloads
            .iter()
            .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
            .collect::<Vec<_>>();

        Ok(encode_map_entry(entry_slices.as_slice()))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        let entry_bytes =
            decode_map_entry(bytes).map_err(InternalError::persisted_row_decode_failed)?;
        let mut out = Self::new();
        for (key_bytes, value_bytes) in entry_bytes {
            let key = K::decode_persisted_structured_payload(key_bytes)?;
            let value = V::decode_persisted_structured_payload(value_bytes)?;

            if let Some((previous_key, _)) = out.last_key_value()
                && key <= *previous_key
            {
                return Err(InternalError::persisted_row_decode_failed(format!(
                    "value payload does not match BTreeMap<{}, {}>",
                    std::any::type_name::<K>(),
                    std::any::type_name::<V>()
                )));
            }
            if out.insert(key, value).is_some() {
                return Err(InternalError::persisted_row_decode_failed(format!(
                    "value payload does not match BTreeMap<{}, {}>",
                    std::any::type_name::<K>(),
                    std::any::type_name::<V>()
                )));
            }
        }

        Ok(out)
    }
}

/// Decode one persisted custom-schema payload through the direct structured
/// field codec owner.
pub fn decode_persisted_custom_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    T::decode_persisted_structured_payload(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

/// Decode one persisted repeated custom-schema payload through the `Vec<T>`
/// structured codec owner.
pub fn decode_persisted_custom_many_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Vec<T>, InternalError>
where
    Vec<T>: PersistedStructuredFieldCodec,
{
    <Vec<T>>::decode_persisted_structured_payload(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

/// Encode one custom-schema field payload through the direct structured field
/// codec owner.
pub fn encode_persisted_custom_slot_payload<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    value
        .encode_persisted_structured_payload()
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

/// Encode one repeated custom-schema payload through the `Vec<T>` structured
/// codec owner.
pub fn encode_persisted_custom_many_slot_payload<T>(
    values: &[T],
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: Clone,
    Vec<T>: PersistedStructuredFieldCodec,
{
    values
        .to_vec()
        .encode_persisted_structured_payload()
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

/// Encode one persisted slot payload using the field type's own runtime field
/// metadata.
pub fn encode_persisted_slot_payload_by_meta<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: FieldTypeMeta + PersistedStructuredFieldCodec + ValueCodec,
{
    match T::STORAGE_DECODE {
        crate::model::field::FieldStorageDecode::ByKind => {
            encode_persisted_slot_payload_by_kind(value, T::KIND, field_name)
        }
        crate::model::field::FieldStorageDecode::Value => {
            encode_persisted_custom_slot_payload(value, field_name)
        }
    }
}

/// Encode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn encode_persisted_option_slot_payload_by_meta<T>(
    value: &Option<T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: FieldTypeMeta + PersistedStructuredFieldCodec + ValueCodec,
{
    match T::STORAGE_DECODE {
        crate::model::field::FieldStorageDecode::ByKind => {
            encode_persisted_slot_payload_by_kind(value, T::KIND, field_name)
        }
        crate::model::field::FieldStorageDecode::Value => {
            encode_persisted_custom_slot_payload(value, field_name)
        }
    }
}

/// Decode one persisted scalar slot payload using the canonical scalar envelope.
pub fn decode_persisted_scalar_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedScalar,
{
    let payload = decode_scalar_slot_payload_body(bytes, field_name)?.ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            "unexpected null for non-nullable scalar field",
        )
    })?;

    T::decode_scalar_payload(payload, field_name)
}

/// Decode one optional persisted scalar slot payload preserving explicit `NULL`.
pub fn decode_persisted_option_scalar_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedScalar,
{
    let Some(payload) = decode_scalar_slot_payload_body(bytes, field_name)? else {
        return Ok(None);
    };

    T::decode_scalar_payload(payload, field_name).map(Some)
}

// Encode one scalar slot value into the canonical prefixed scalar envelope.
pub(super) fn encode_scalar_slot_value(value: ScalarSlotValueRef<'_>) -> Vec<u8> {
    match value {
        ScalarSlotValueRef::Null => vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_NULL],
        ScalarSlotValueRef::Value(value) => {
            let mut encoded = Vec::new();
            encoded.push(SCALAR_SLOT_PREFIX);
            encoded.push(SCALAR_SLOT_TAG_VALUE);

            match value {
                ScalarValueRef::Blob(bytes) => encoded.extend_from_slice(bytes),
                ScalarValueRef::Bool(value) => encoded.push(u8::from(value)),
                ScalarValueRef::Date(value) => {
                    encoded.extend_from_slice(&value.as_days_since_epoch().to_le_bytes());
                }
                ScalarValueRef::Duration(value) => {
                    encoded.extend_from_slice(&value.as_millis().to_le_bytes());
                }
                ScalarValueRef::Float32(value) => {
                    encoded.extend_from_slice(&value.get().to_bits().to_le_bytes());
                }
                ScalarValueRef::Float64(value) => {
                    encoded.extend_from_slice(&value.get().to_bits().to_le_bytes());
                }
                ScalarValueRef::Int(value) => encoded.extend_from_slice(&value.to_le_bytes()),
                ScalarValueRef::Principal(value) => encoded.extend_from_slice(value.as_slice()),
                ScalarValueRef::Subaccount(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Text(value) => encoded.extend_from_slice(value.as_bytes()),
                ScalarValueRef::Timestamp(value) => {
                    encoded.extend_from_slice(&value.as_millis().to_le_bytes());
                }
                ScalarValueRef::Uint(value) => encoded.extend_from_slice(&value.to_le_bytes()),
                ScalarValueRef::Ulid(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Unit => {}
            }

            encoded
        }
    }
}

// Split one scalar slot envelope into `NULL` vs payload bytes.
fn decode_scalar_slot_payload_body<'a>(
    bytes: &'a [u8],
    field_name: &'static str,
) -> Result<Option<&'a [u8]>, InternalError> {
    let Some((&prefix, rest)) = bytes.split_first() else {
        return Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            "empty scalar payload",
        ));
    };
    if prefix != SCALAR_SLOT_PREFIX {
        return Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            format!(
                "scalar payload prefix mismatch: expected slot envelope prefix byte 0x{SCALAR_SLOT_PREFIX:02X}, found 0x{prefix:02X}",
            ),
        ));
    }
    let Some((&tag, payload)) = rest.split_first() else {
        return Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            "truncated scalar payload tag",
        ));
    };

    match tag {
        SCALAR_SLOT_TAG_NULL => {
            if !payload.is_empty() {
                return Err(InternalError::persisted_row_field_decode_failed(
                    field_name,
                    "null scalar payload has trailing bytes",
                ));
            }

            Ok(None)
        }
        SCALAR_SLOT_TAG_VALUE => Ok(Some(payload)),
        _ => Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("invalid scalar payload tag {tag}"),
        )),
    }
}

// Decode one scalar slot view using the field-declared scalar codec.
#[expect(clippy::too_many_lines)]
pub(super) fn decode_scalar_slot_value<'a>(
    bytes: &'a [u8],
    codec: ScalarCodec,
    field_name: &'static str,
) -> Result<ScalarSlotValueRef<'a>, InternalError> {
    let Some(payload) = decode_scalar_slot_payload_body(bytes, field_name)? else {
        return Ok(ScalarSlotValueRef::Null);
    };

    let value = match codec {
        ScalarCodec::Blob => ScalarValueRef::Blob(payload),
        ScalarCodec::Bool => {
            let [value] = payload else {
                return Err(
                    InternalError::persisted_row_field_payload_exact_len_required(
                        field_name,
                        "bool",
                        SCALAR_BOOL_PAYLOAD_LEN,
                    ),
                );
            };
            match *value {
                SCALAR_BOOL_FALSE_TAG => ScalarValueRef::Bool(false),
                SCALAR_BOOL_TRUE_TAG => ScalarValueRef::Bool(true),
                _ => {
                    return Err(InternalError::persisted_row_field_payload_invalid_byte(
                        field_name, "bool", *value,
                    ));
                }
            }
        }
        ScalarCodec::Date => {
            let bytes: [u8; SCALAR_WORD32_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "date",
                    SCALAR_WORD32_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Date(Date::from_days_since_epoch(i32::from_le_bytes(bytes)))
        }
        ScalarCodec::Duration => {
            let bytes: [u8; SCALAR_WORD64_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "duration",
                    SCALAR_WORD64_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Duration(Duration::from_millis(u64::from_le_bytes(bytes)))
        }
        ScalarCodec::Float32 => {
            let bytes: [u8; SCALAR_WORD32_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "float32",
                    SCALAR_WORD32_PAYLOAD_LEN,
                )
            })?;
            let value = f32::from_bits(u32::from_le_bytes(bytes));
            let value = Float32::try_new(value).ok_or_else(|| {
                InternalError::persisted_row_field_payload_non_finite(field_name, "float32")
            })?;
            ScalarValueRef::Float32(value)
        }
        ScalarCodec::Float64 => {
            let bytes: [u8; SCALAR_WORD64_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "float64",
                    SCALAR_WORD64_PAYLOAD_LEN,
                )
            })?;
            let value = f64::from_bits(u64::from_le_bytes(bytes));
            let value = Float64::try_new(value).ok_or_else(|| {
                InternalError::persisted_row_field_payload_non_finite(field_name, "float64")
            })?;
            ScalarValueRef::Float64(value)
        }
        ScalarCodec::Int64 => {
            let bytes: [u8; SCALAR_WORD64_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "int",
                    SCALAR_WORD64_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Int(i64::from_le_bytes(bytes))
        }
        ScalarCodec::Principal => ScalarValueRef::Principal(
            Principal::try_from_bytes(payload)
                .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?,
        ),
        ScalarCodec::Subaccount => {
            let bytes: [u8; SCALAR_SUBACCOUNT_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "subaccount",
                    SCALAR_SUBACCOUNT_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Subaccount(Subaccount::from_array(bytes))
        }
        ScalarCodec::Text => {
            let value = str::from_utf8(payload).map_err(|err| {
                InternalError::persisted_row_field_text_payload_invalid_utf8(field_name, err)
            })?;
            ScalarValueRef::Text(value)
        }
        ScalarCodec::Timestamp => {
            let bytes: [u8; SCALAR_WORD64_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "timestamp",
                    SCALAR_WORD64_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Timestamp(Timestamp::from_millis(i64::from_le_bytes(bytes)))
        }
        ScalarCodec::Uint64 => {
            let bytes: [u8; SCALAR_WORD64_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "uint",
                    SCALAR_WORD64_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Uint(u64::from_le_bytes(bytes))
        }
        ScalarCodec::Ulid => {
            let bytes: [u8; SCALAR_ULID_PAYLOAD_LEN] = payload.try_into().map_err(|_| {
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "ulid",
                    SCALAR_ULID_PAYLOAD_LEN,
                )
            })?;
            ScalarValueRef::Ulid(Ulid::from_bytes(bytes))
        }
        ScalarCodec::Unit => {
            if !payload.is_empty() {
                return Err(InternalError::persisted_row_field_payload_must_be_empty(
                    field_name, "unit",
                ));
            }
            ScalarValueRef::Unit
        }
    };

    Ok(ScalarSlotValueRef::Value(value))
}

macro_rules! impl_persisted_scalar_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedScalar for $ty {
                const CODEC: ScalarCodec = ScalarCodec::Int64;

                fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(i64::from(*self).to_le_bytes().to_vec())
                }

                fn decode_scalar_payload(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    let raw: [u8; SCALAR_WORD64_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
                        InternalError::persisted_row_field_payload_exact_len_required(
                            field_name,
                            "int",
                            SCALAR_WORD64_PAYLOAD_LEN,
                        )
                    })?;
                    <$ty>::try_from(i64::from_le_bytes(raw)).map_err(|_| {
                        InternalError::persisted_row_field_payload_out_of_range(
                            field_name,
                            "integer",
                        )
                    })
                }
            }
        )*
    };
}

macro_rules! impl_persisted_scalar_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedScalar for $ty {
                const CODEC: ScalarCodec = ScalarCodec::Uint64;

                fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(u64::from(*self).to_le_bytes().to_vec())
                }

                fn decode_scalar_payload(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    let raw: [u8; SCALAR_WORD64_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
                        InternalError::persisted_row_field_payload_exact_len_required(
                            field_name,
                            "uint",
                            SCALAR_WORD64_PAYLOAD_LEN,
                        )
                    })?;
                    <$ty>::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                        InternalError::persisted_row_field_payload_out_of_range(
                            field_name,
                            "unsigned",
                        )
                    })
                }
            }
        )*
    };
}

impl_persisted_scalar_signed!(i8, i16, i32, i64);
impl_persisted_scalar_unsigned!(u8, u16, u32, u64);

impl PersistedScalar for bool {
    const CODEC: ScalarCodec = ScalarCodec::Bool;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(vec![u8::from(*self)])
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let [value] = bytes else {
            return Err(
                InternalError::persisted_row_field_payload_exact_len_required(
                    field_name,
                    "bool",
                    SCALAR_BOOL_PAYLOAD_LEN,
                ),
            );
        };

        match *value {
            SCALAR_BOOL_FALSE_TAG => Ok(false),
            SCALAR_BOOL_TRUE_TAG => Ok(true),
            _ => Err(InternalError::persisted_row_field_payload_invalid_byte(
                field_name, "bool", *value,
            )),
        }
    }
}

impl PersistedScalar for String {
    const CODEC: ScalarCodec = ScalarCodec::Text;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.as_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        str::from_utf8(bytes).map(str::to_owned).map_err(|err| {
            InternalError::persisted_row_field_text_payload_invalid_utf8(field_name, err)
        })
    }
}

impl PersistedScalar for Vec<u8> {
    const CODEC: ScalarCodec = ScalarCodec::Blob;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.clone())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        _field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Ok(bytes.to_vec())
    }
}

impl PersistedScalar for Blob {
    const CODEC: ScalarCodec = ScalarCodec::Blob;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        _field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Ok(Self::from(bytes))
    }
}

impl PersistedScalar for Ulid {
    const CODEC: ScalarCodec = ScalarCodec::Ulid;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.to_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Self::try_from_bytes(bytes)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
    }
}

impl PersistedScalar for Timestamp {
    const CODEC: ScalarCodec = ScalarCodec::Timestamp;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.as_millis().to_le_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_WORD64_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "timestamp",
                SCALAR_WORD64_PAYLOAD_LEN,
            )
        })?;

        Ok(Self::from_millis(i64::from_le_bytes(raw)))
    }
}

impl PersistedScalar for Date {
    const CODEC: ScalarCodec = ScalarCodec::Date;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.as_days_since_epoch().to_le_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_WORD32_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "date",
                SCALAR_WORD32_PAYLOAD_LEN,
            )
        })?;

        Ok(Self::from_days_since_epoch(i32::from_le_bytes(raw)))
    }
}

impl PersistedScalar for Duration {
    const CODEC: ScalarCodec = ScalarCodec::Duration;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.as_millis().to_le_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_WORD64_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "duration",
                SCALAR_WORD64_PAYLOAD_LEN,
            )
        })?;

        Ok(Self::from_millis(u64::from_le_bytes(raw)))
    }
}

impl PersistedScalar for Float32 {
    const CODEC: ScalarCodec = ScalarCodec::Float32;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.get().to_bits().to_le_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_WORD32_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "float32",
                SCALAR_WORD32_PAYLOAD_LEN,
            )
        })?;
        let value = f32::from_bits(u32::from_le_bytes(raw));

        Self::try_new(value).ok_or_else(|| {
            InternalError::persisted_row_field_payload_non_finite(field_name, "float32")
        })
    }
}

impl PersistedScalar for Float64 {
    const CODEC: ScalarCodec = ScalarCodec::Float64;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.get().to_bits().to_le_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_WORD64_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "float64",
                SCALAR_WORD64_PAYLOAD_LEN,
            )
        })?;
        let value = f64::from_bits(u64::from_le_bytes(raw));

        Self::try_new(value).ok_or_else(|| {
            InternalError::persisted_row_field_payload_non_finite(field_name, "float64")
        })
    }
}

impl PersistedScalar for Principal {
    const CODEC: ScalarCodec = ScalarCodec::Principal;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        self.to_bytes()
            .map_err(|err| InternalError::persisted_row_field_encode_failed("principal", err))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Self::try_from_bytes(bytes)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
    }
}

impl PersistedScalar for Subaccount {
    const CODEC: ScalarCodec = ScalarCodec::Subaccount;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(self.to_bytes().to_vec())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let raw: [u8; SCALAR_SUBACCOUNT_PAYLOAD_LEN] = bytes.try_into().map_err(|_| {
            InternalError::persisted_row_field_payload_exact_len_required(
                field_name,
                "subaccount",
                SCALAR_SUBACCOUNT_PAYLOAD_LEN,
            )
        })?;

        Ok(Self::from_array(raw))
    }
}

impl PersistedScalar for () {
    const CODEC: ScalarCodec = ScalarCodec::Unit;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(Vec::new())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        if !bytes.is_empty() {
            return Err(InternalError::persisted_row_field_payload_must_be_empty(
                field_name, "unit",
            ));
        }

        Ok(())
    }
}

impl PersistedScalar for Unit {
    const CODEC: ScalarCodec = ScalarCodec::Unit;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(Vec::new())
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        if !bytes.is_empty() {
            return Err(InternalError::persisted_row_field_payload_must_be_empty(
                field_name, "unit",
            ));
        }

        Ok(Self)
    }
}
