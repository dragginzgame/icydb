//! Module: db::data::persisted_row::codec
//! Defines the persisted scalar row-slot encoding and borrowed decoding helpers
//! used by runtime row access.

use crate::{
    db::data::{
        decode_account, decode_blob_field_by_kind_bytes, decode_bool_field_by_kind_bytes,
        decode_date_field_by_kind_bytes, decode_decimal, decode_decimal_field_by_kind_bytes,
        decode_duration_field_by_kind_bytes, decode_float32_field_by_kind_bytes,
        decode_float64_field_by_kind_bytes, decode_int, decode_int_big_field_by_kind_bytes,
        decode_int128, decode_int128_field_by_kind_bytes, decode_list_field_items,
        decode_list_item, decode_map_entry, decode_map_field_entries, decode_nat, decode_nat128,
        decode_nat128_field_by_kind_bytes, decode_optional_storage_key_field_bytes,
        decode_storage_key_binary_value_bytes, decode_structural_field_by_kind_bytes,
        decode_structural_value_storage_blob_bytes, decode_structural_value_storage_bool_bytes,
        decode_structural_value_storage_bytes, decode_structural_value_storage_date_bytes,
        decode_structural_value_storage_duration_bytes,
        decode_structural_value_storage_float32_bytes,
        decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
        decode_structural_value_storage_principal_bytes,
        decode_structural_value_storage_subaccount_bytes,
        decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
        decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
        decode_text, decode_text_field_by_kind_bytes, decode_uint_big_field_by_kind_bytes,
        encode_account, encode_blob_field_by_kind_bytes, encode_bool_field_by_kind_bytes,
        encode_date_field_by_kind_bytes, encode_decimal, encode_decimal_field_by_kind_bytes,
        encode_duration_field_by_kind_bytes, encode_float32_field_by_kind_bytes,
        encode_float64_field_by_kind_bytes, encode_int, encode_int_big_field_by_kind_bytes,
        encode_int128, encode_int128_field_by_kind_bytes, encode_list_field_items,
        encode_list_item, encode_map_entry, encode_map_field_entries, encode_nat, encode_nat128,
        encode_nat128_field_by_kind_bytes, encode_storage_key_binary_value_bytes,
        encode_storage_key_field_bytes, encode_structural_field_by_kind_bytes,
        encode_structural_value_storage_blob_bytes, encode_structural_value_storage_bool_bytes,
        encode_structural_value_storage_bytes, encode_structural_value_storage_date_bytes,
        encode_structural_value_storage_duration_bytes,
        encode_structural_value_storage_float32_bytes,
        encode_structural_value_storage_float64_bytes, encode_structural_value_storage_i64_bytes,
        encode_structural_value_storage_null_bytes,
        encode_structural_value_storage_principal_bytes,
        encode_structural_value_storage_subaccount_bytes,
        encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
        encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
        encode_text, encode_text_field_by_kind_bytes, encode_uint_big_field_by_kind_bytes,
        structural_value_storage_bytes_are_null, supports_storage_key_binary_kind,
    },
    error::InternalError,
    model::field::{FieldKind, ScalarCodec},
    traits::{
        Collection, PersistedByKindCodec, PersistedFieldMetaCodec, PersistedStructuredFieldCodec,
    },
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid, Unit,
    },
    value::{StorageKey, Value},
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
    T: PersistedByKindCodec,
{
    value.encode_persisted_slot_payload_by_kind(kind, field_name)
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
    T: PersistedByKindCodec,
{
    T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
}

/// Decode one persisted slot payload using the stricter schema-owned `ByKind`
/// storage contract.
pub fn decode_persisted_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedByKindCodec,
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
    T: PersistedByKindCodec,
{
    decode_persisted_structural_slot_payload_by_kind(bytes, kind, field_name)
}

macro_rules! impl_persisted_by_kind_direct_leaf {
    ($($ty:ty => { encode: $encode:expr, decode: $decode:expr }),* $(,)?) => {
        $(
            impl PersistedByKindCodec for $ty {
                fn encode_persisted_slot_payload_by_kind(
                    &self,
                    kind: FieldKind,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    ($encode)(self, kind, field_name)
                }

                fn decode_persisted_option_slot_payload_by_kind(
                    bytes: &[u8],
                    kind: FieldKind,
                    field_name: &'static str,
                ) -> Result<Option<Self>, InternalError> {
                    ($decode)(bytes, kind, field_name)
                }
            }
        )*
    };
}

macro_rules! impl_persisted_by_kind_scalar_leaf {
    ($($ty:ty => { encode: $encode:ident, decode: $decode:ident }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(*value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_ref_leaf {
    ($($ty:ty => { encode: $encode:ident, decode: $decode:ident }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_value_leaf {
    ($($ty:ty => { encode: $encode:ident, decode: $decode:ident }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(*value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_int_leaf {
    ($($ty:ty),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        encode_storage_key_field_bytes(StorageKey::Int(i64::from(*value)), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        let Some(key) = decode_optional_storage_key_field_bytes(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
                        else {
                            return Ok(None);
                        };

                        let StorageKey::Int(value) = key else {
                            return Err(InternalError::persisted_row_field_decode_failed(
                                field_name,
                                format!("field kind {kind:?} did not decode as storage int"),
                            ));
                        };

                        <$ty>::try_from(value).map(Some).map_err(|_| {
                            InternalError::persisted_row_field_decode_failed(
                                field_name,
                                format!("field kind {kind:?} did not decode as storage int"),
                            )
                        })
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_uint_leaf {
    ($($ty:ty),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        encode_storage_key_field_bytes(StorageKey::Uint(u64::from(*value)), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        let Some(key) = decode_optional_storage_key_field_bytes(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
                        else {
                            return Ok(None);
                        };

                        let StorageKey::Uint(value) = key else {
                            return Err(InternalError::persisted_row_field_decode_failed(
                                field_name,
                                format!("field kind {kind:?} did not decode as storage uint"),
                            ));
                        };

                        <$ty>::try_from(value).map(Some).map_err(|_| {
                            InternalError::persisted_row_field_decode_failed(
                                field_name,
                                format!("field kind {kind:?} did not decode as storage uint"),
                            )
                        })
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_leaf {
    ($($ty:ty => { variant: $variant:ident, label: $label:literal }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        encode_storage_key_field_bytes(StorageKey::$variant(*value), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        let Some(key) = decode_optional_storage_key_field_bytes(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
                        else {
                            return Ok(None);
                        };

                        match key {
                            StorageKey::$variant(value) => Ok(Some(value)),
                            _ => Err(InternalError::persisted_row_field_decode_failed(
                                field_name,
                                format!("field kind {kind:?} did not decode as {}", $label),
                            )),
                        }
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_unit_leaf {
    ($ty:ty) => {
        impl_persisted_by_kind_direct_leaf!(
            $ty => {
                encode: |_: &$ty, kind: FieldKind, field_name: &'static str| {
                    encode_storage_key_field_bytes(StorageKey::Unit, kind, field_name)
                },
                decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                    let Some(key) = decode_optional_storage_key_field_bytes(bytes, kind)
                        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
                    else {
                        return Ok(None);
                    };

                    match key {
                        StorageKey::Unit => Ok(Some(Unit)),
                        _ => Err(InternalError::persisted_row_field_decode_failed(
                            field_name,
                            format!("field kind {kind:?} did not decode as storage unit"),
                        )),
                    }
                }
            }
        );
    };
}

impl_persisted_by_kind_scalar_leaf!(
    bool => { encode: encode_bool_field_by_kind_bytes, decode: decode_bool_field_by_kind_bytes },
    Float32 => { encode: encode_float32_field_by_kind_bytes, decode: decode_float32_field_by_kind_bytes },
    Float64 => { encode: encode_float64_field_by_kind_bytes, decode: decode_float64_field_by_kind_bytes },
    Int128 => { encode: encode_int128_field_by_kind_bytes, decode: decode_int128_field_by_kind_bytes },
    Nat128 => { encode: encode_nat128_field_by_kind_bytes, decode: decode_nat128_field_by_kind_bytes }
);

impl_persisted_by_kind_ref_leaf!(
    String => { encode: encode_text_field_by_kind_bytes, decode: decode_text_field_by_kind_bytes },
    Blob => { encode: encode_blob_field_by_kind_bytes, decode: decode_blob_field_by_kind_bytes },
    Int => { encode: encode_int_big_field_by_kind_bytes, decode: decode_int_big_field_by_kind_bytes },
    Nat => { encode: encode_uint_big_field_by_kind_bytes, decode: decode_uint_big_field_by_kind_bytes }
);

impl_persisted_by_kind_value_leaf!(
    Date => { encode: encode_date_field_by_kind_bytes, decode: decode_date_field_by_kind_bytes },
    Decimal => { encode: encode_decimal_field_by_kind_bytes, decode: decode_decimal_field_by_kind_bytes },
    Duration => { encode: encode_duration_field_by_kind_bytes, decode: decode_duration_field_by_kind_bytes }
);

impl_persisted_by_kind_storage_int_leaf!(i8, i16, i32, i64);
impl_persisted_by_kind_storage_uint_leaf!(u8, u16, u32, u64);
impl_persisted_by_kind_storage_leaf!(
    Account => { variant: Account, label: "storage account" },
    Timestamp => { variant: Timestamp, label: "storage timestamp" },
    Principal => { variant: Principal, label: "storage principal" },
    Subaccount => { variant: Subaccount, label: "storage subaccount" },
    Ulid => { variant: Ulid, label: "storage ulid" }
);
impl_persisted_by_kind_storage_unit_leaf!(Unit);

// Encode one explicit by-kind owner through the current field-kind structural
// contract.
fn encode_explicit_by_kind_value(
    kind: FieldKind,
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError> {
    if supports_storage_key_binary_kind(kind) {
        return encode_storage_key_binary_value_bytes(kind, value, field_name)?.ok_or_else(|| {
            InternalError::persisted_row_field_encode_failed(
                field_name,
                "storage-key binary lane rejected a supported field kind",
            )
        });
    }

    encode_structural_field_by_kind_bytes(kind, value, field_name)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Decode one explicit by-kind owner through the current field-kind structural
// contract.
fn decode_explicit_by_kind_value(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<Value>, InternalError> {
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

    Ok(Some(value))
}

impl<T> PersistedByKindCodec for Box<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        self.as_ref()
            .encode_persisted_slot_payload_by_kind(kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
            .map(|value| value.map(Self::new))
    }
}

impl<T> PersistedByKindCodec for Option<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        match self {
            Some(value) => value.encode_persisted_slot_payload_by_kind(kind, field_name),
            None => encode_explicit_by_kind_value(kind, &Value::Null, field_name),
        }
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        match decode_explicit_by_kind_value(bytes, kind, field_name)? {
            None => Ok(Some(None)),
            Some(_) => T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
                .map(|value| value.map(Some)),
        }
    }
}

// Encode one explicit by-kind wrapper owner through its value-surface
// Decode one nested by-kind payload and require that it materializes a real
// value rather than an incompatible null for this owner type.
fn decode_required_nested_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
    label: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedByKindCodec,
{
    T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)?.ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!(
                "{label} payload did not decode as {}",
                std::any::type_name::<T>()
            ),
        )
    })
}

// Encode one collection wrapper through recursive by-kind item ownership
// instead of re-entering the generic runtime `Value` bridge.
fn encode_direct_by_kind_collection<C, T>(
    values: &C,
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    C: Collection<Item = T>,
    T: PersistedByKindCodec,
{
    let (FieldKind::List(inner) | FieldKind::Set(inner)) = kind else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept collection payloads"),
        ));
    };

    let item_bytes = values
        .iter()
        .map(|item| item.encode_persisted_slot_payload_by_kind(*inner, field_name))
        .collect::<Result<Vec<_>, _>>()?;
    let item_slices = item_bytes.iter().map(Vec::as_slice).collect::<Vec<_>>();

    encode_list_field_items(item_slices.as_slice(), kind, field_name)
}

// Decode one collection wrapper through recursive by-kind item ownership
// instead of re-entering the generic runtime `Value` bridge.
fn decode_direct_by_kind_collection<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Vec<T>, InternalError>
where
    T: PersistedByKindCodec,
{
    let (FieldKind::List(inner) | FieldKind::Set(inner)) = kind else {
        return Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("field kind {kind:?} does not accept collection payloads"),
        ));
    };

    let item_bytes = decode_list_field_items(bytes, kind)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?;

    item_bytes
        .iter()
        .map(|item| decode_required_nested_by_kind(item.as_slice(), *inner, field_name, "item"))
        .collect()
}

// Encode one map wrapper through recursive by-kind key/value ownership instead
// of re-entering the generic runtime `Value` bridge.
fn encode_direct_by_kind_map<K, V>(
    entries: &BTreeMap<K, V>,
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    K: Ord + PersistedByKindCodec,
    V: PersistedByKindCodec,
{
    let FieldKind::Map {
        key,
        value: value_kind,
    } = kind
    else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept map payloads"),
        ));
    };

    let entry_bytes = entries
        .iter()
        .map(|(entry_key, entry_value)| {
            let key_bytes = entry_key.encode_persisted_slot_payload_by_kind(*key, field_name)?;
            let value_bytes =
                entry_value.encode_persisted_slot_payload_by_kind(*value_kind, field_name)?;

            Ok((key_bytes, value_bytes))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let entry_slices = entry_bytes
        .iter()
        .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
        .collect::<Vec<_>>();

    encode_map_field_entries(entry_slices.as_slice(), kind, field_name)
}

// Decode one map wrapper through recursive by-kind key/value ownership instead
// of re-entering the generic runtime `Value` bridge.
fn decode_direct_by_kind_map<K, V>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<BTreeMap<K, V>, InternalError>
where
    K: Ord + PersistedByKindCodec,
    V: PersistedByKindCodec,
{
    let FieldKind::Map {
        key,
        value: value_kind,
    } = kind
    else {
        return Err(InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("field kind {kind:?} does not accept map payloads"),
        ));
    };

    let entry_bytes = decode_map_field_entries(bytes, kind)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?;
    let mut decoded = BTreeMap::new();
    for (key_bytes, value_bytes) in entry_bytes {
        let decoded_key =
            decode_required_nested_by_kind(key_bytes.as_slice(), *key, field_name, "map key")?;
        let decoded_value = decode_required_nested_by_kind(
            value_bytes.as_slice(),
            *value_kind,
            field_name,
            "map value",
        )?;
        decoded.insert(decoded_key, decoded_value);
    }

    Ok(decoded)
}

impl<T> PersistedByKindCodec for Vec<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_direct_by_kind_collection(self, kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_direct_by_kind_collection(bytes, kind, field_name).map(Some)
    }
}

impl<T> PersistedByKindCodec for BTreeSet<T>
where
    T: Ord + PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_direct_by_kind_collection(self, kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_direct_by_kind_collection::<T>(bytes, kind, field_name)
            .map(|values| Some(values.into_iter().collect()))
    }
}

impl<K, V> PersistedByKindCodec for BTreeMap<K, V>
where
    K: Ord + PersistedByKindCodec,
    V: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_direct_by_kind_map(self, kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_direct_by_kind_map(bytes, kind, field_name).map(Some)
    }
}

/// Decode one persisted slot payload using the field type's own runtime field
/// metadata.
pub fn decode_persisted_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::decode_persisted_slot_payload_by_meta(bytes, field_name)
}

/// Decode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn decode_persisted_option_slot_payload_by_meta<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
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

// `Value` remains an explicit runtime/dynamic escape hatch for callers that
// intentionally want to persist one already-materialized runtime union.
// This is not a generic fallback: normal typed persistence should use
// `PersistedStructuredFieldCodec` or `PersistedByKindCodec` on the concrete
// field type instead of routing through `Value`.
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

impl PersistedFieldMetaCodec for Value {
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(self, field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(value, field_name)
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
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

impl<T> PersistedFieldMetaCodec for Vec<T>
where
    T: PersistedFieldMetaCodec + PersistedStructuredFieldCodec,
{
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(self, field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(value, field_name)
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
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

impl<T> PersistedFieldMetaCodec for Box<T>
where
    T: PersistedFieldMetaCodec,
{
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        self.as_ref()
            .encode_persisted_slot_payload_by_meta(field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        T::decode_persisted_slot_payload_by_meta(bytes, field_name).map(Self::new)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        match value {
            Some(value) => value
                .as_ref()
                .encode_persisted_slot_payload_by_meta(field_name),
            None => T::encode_persisted_option_slot_payload_by_meta(&None, field_name),
        }
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        T::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
            .map(|value| value.map(Self::new))
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

impl<T> PersistedFieldMetaCodec for BTreeSet<T>
where
    T: Ord + PersistedFieldMetaCodec + PersistedStructuredFieldCodec,
{
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(self, field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(value, field_name)
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
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

impl<K, V> PersistedFieldMetaCodec for BTreeMap<K, V>
where
    K: Ord + PersistedFieldMetaCodec + PersistedStructuredFieldCodec,
    V: PersistedFieldMetaCodec + PersistedStructuredFieldCodec,
{
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(self, field_name)
    }

    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
    }

    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_custom_slot_payload(value, field_name)
    }

    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_custom_slot_payload(bytes, field_name)
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
    T: PersistedFieldMetaCodec,
{
    value.encode_persisted_slot_payload_by_meta(field_name)
}

/// Encode one optional persisted slot payload using the inner field type's own
/// runtime field metadata.
pub fn encode_persisted_option_slot_payload_by_meta<T>(
    value: &Option<T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedFieldMetaCodec,
{
    T::encode_persisted_option_slot_payload_by_meta(value, field_name)
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
