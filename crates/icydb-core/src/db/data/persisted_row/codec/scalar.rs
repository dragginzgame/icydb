use crate::{
    db::data::persisted_row::codec::{
        decode_required_with_strategy, decode_with_strategy, encode_with_strategy,
        strategy::StorageStrategy,
    },
    error::InternalError,
    model::field::ScalarCodec,
    types::{Blob, Date, Duration, Float32, Float64, Principal, Subaccount, Timestamp, Ulid, Unit},
    value::Value,
};
use std::str;

const SCALAR_SLOT_PREFIX: u8 = 0xFF;
const SCALAR_SLOT_TAG_NULL: u8 = 0;
const SCALAR_SLOT_TAG_VALUE: u8 = 1;

const SCALAR_BOOL_PAYLOAD_LEN: usize = 1;

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
    Nat(u64),
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
            Self::Nat(value) => Value::Nat(value),
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

// Encode a typed scalar through the strategy-owned scalar envelope. Both
// nullable and required scalar entrypoints share this adapter.
fn encode_scalar_value<T>(
    _strategy: StorageStrategy,
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedScalar,
{
    let payload = value.encode_scalar_payload()?;

    encode_scalar_payload_envelope(payload.as_slice(), field_name)
}

/// Encode one persisted scalar slot payload using the canonical scalar envelope.
pub fn encode_persisted_scalar_slot_payload<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedScalar,
{
    encode_with_strategy(
        StorageStrategy::Scalar,
        Some(value),
        field_name,
        encode_scalar_value,
    )
}

/// Encode one optional persisted scalar slot payload preserving explicit `NULL`.
pub fn encode_persisted_option_scalar_slot_payload<T>(
    value: &Option<T>,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedScalar,
{
    encode_with_strategy(
        StorageStrategy::Scalar,
        value.as_ref(),
        field_name,
        encode_scalar_value,
    )
}

// Decode a typed scalar from the strategy-owned scalar envelope. The shared
// decode gateway owns required/null policy around this adapter.
fn decode_scalar_value<T>(
    _strategy: StorageStrategy,
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedScalar,
{
    decode_scalar_slot_payload_body(bytes, field_name)?
        .map(|payload| T::decode_scalar_payload(payload, field_name))
        .transpose()
}

/// Decode one persisted scalar slot payload using the canonical scalar envelope.
pub fn decode_persisted_scalar_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedScalar,
{
    decode_required_with_strategy(
        StorageStrategy::Scalar,
        bytes,
        field_name,
        "unexpected null for non-nullable scalar field",
        decode_scalar_value,
    )
}

/// Decode one optional persisted scalar slot payload preserving explicit `NULL`.
pub fn decode_persisted_option_scalar_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedScalar,
{
    decode_with_strategy(
        StorageStrategy::Scalar,
        bytes,
        field_name,
        decode_scalar_value,
    )
}

// Copy a fixed-width scalar payload into an array while preserving the exact
// field/codec-specific length error used by each scalar owner.
fn decode_fixed<const N: usize>(
    bytes: &[u8],
    field_name: &str,
    label: &'static str,
) -> Result<[u8; N], InternalError> {
    bytes.try_into().map_err(|_| {
        InternalError::persisted_row_field_payload_exact_len_required(field_name, label, N)
    })
}

// Encode fixed-width scalar primitives into owned payload bytes. This is only a
// named version of the repeated `to_le_bytes().to_vec()` shape.
fn encode_fixed<const N: usize>(bytes: [u8; N]) -> Vec<u8> {
    bytes.to_vec()
}

// Decode the one-byte boolean scalar payload shared by raw scalar slots and
// generated scalar-field owners.
fn decode_bool_scalar_payload(bytes: &[u8], field_name: &str) -> Result<bool, InternalError> {
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

// Decode the empty unit scalar payload shared by `()` and the public `Unit`
// wrapper without giving either owner its own copy of the same guard.
fn decode_unit_scalar_payload(bytes: &[u8], field_name: &str) -> Result<(), InternalError> {
    if !bytes.is_empty() {
        return Err(InternalError::persisted_row_field_payload_must_be_empty(
            field_name, "unit",
        ));
    }

    Ok(())
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i32_payload(
    bytes: &[u8],
    field_name: &str,
    label: &'static str,
) -> Result<i32, InternalError> {
    Ok(i32::from_le_bytes(decode_fixed(bytes, field_name, label)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i64_payload(
    bytes: &[u8],
    field_name: &str,
    label: &'static str,
) -> Result<i64, InternalError> {
    Ok(i64::from_le_bytes(decode_fixed(bytes, field_name, label)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u32_payload(
    bytes: &[u8],
    field_name: &str,
    label: &'static str,
) -> Result<u32, InternalError> {
    Ok(u32::from_le_bytes(decode_fixed(bytes, field_name, label)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u64_payload(
    bytes: &[u8],
    field_name: &str,
    label: &'static str,
) -> Result<u64, InternalError> {
    Ok(u64::from_le_bytes(decode_fixed(bytes, field_name, label)?))
}

// Write the two-byte scalar slot envelope prefix shared by generic scalar
// encoding and the hot direct scalar slot writer.
fn write_scalar_envelope_prefix(out: &mut Vec<u8>, is_null: bool) {
    out.push(SCALAR_SLOT_PREFIX);
    out.push(if is_null {
        SCALAR_SLOT_TAG_NULL
    } else {
        SCALAR_SLOT_TAG_VALUE
    });
}

// Encode the scalar-lane null sentinel while keeping the scalar envelope bytes
// owned by the scalar subsystem instead of the strategy root.
pub(in crate::db::data::persisted_row::codec) fn encode_null_slot_payload() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(2);
    write_scalar_envelope_prefix(&mut encoded, true);

    encoded
}

// Wrap one typed scalar payload in the persisted scalar slot envelope. This is
// shared by generated scalar field writes and manual scalar payload writes.
fn encode_scalar_payload_envelope(
    payload: &[u8],
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::with_capacity(payload.len() + 2);
    write_scalar_envelope_prefix(&mut encoded, false);
    encoded.extend_from_slice(payload);

    if encoded.len() < 2 {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "scalar payload envelope underflow",
        ));
    }

    Ok(encoded)
}

// Compute the encoded scalar payload size before writing the slot envelope so
// the hot scalar writer can reserve exactly once for fixed-width values.
fn scalar_value_payload_len(value: ScalarValueRef<'_>) -> usize {
    match value {
        ScalarValueRef::Blob(bytes) => bytes.len(),
        ScalarValueRef::Bool(_) => 1,
        ScalarValueRef::Date(_) | ScalarValueRef::Float32(_) => 4,
        ScalarValueRef::Duration(_)
        | ScalarValueRef::Float64(_)
        | ScalarValueRef::Int(_)
        | ScalarValueRef::Timestamp(_)
        | ScalarValueRef::Nat(_) => 8,
        ScalarValueRef::Principal(value) => value.as_slice().len(),
        ScalarValueRef::Subaccount(_) => 32,
        ScalarValueRef::Text(value) => value.len(),
        ScalarValueRef::Ulid(_) => 16,
        ScalarValueRef::Unit => 0,
    }
}

// Encode one scalar slot value into the canonical prefixed scalar envelope.
pub(in crate::db::data::persisted_row) fn encode_scalar_slot_value(
    value: ScalarSlotValueRef<'_>,
) -> Vec<u8> {
    match value {
        ScalarSlotValueRef::Null => encode_null_slot_payload(),
        ScalarSlotValueRef::Value(value) => {
            let mut encoded = Vec::with_capacity(2 + scalar_value_payload_len(value));
            write_scalar_envelope_prefix(&mut encoded, false);

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
                ScalarValueRef::Nat(value) => encoded.extend_from_slice(&value.to_le_bytes()),
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
    field_name: &str,
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
pub(in crate::db::data::persisted_row) fn decode_scalar_slot_value<'a>(
    bytes: &'a [u8],
    codec: ScalarCodec,
    field_name: &str,
) -> Result<ScalarSlotValueRef<'a>, InternalError> {
    let Some(payload) = decode_scalar_slot_payload_body(bytes, field_name)? else {
        return Ok(ScalarSlotValueRef::Null);
    };

    let value = match codec {
        ScalarCodec::Blob => ScalarValueRef::Blob(payload),
        ScalarCodec::Bool => ScalarValueRef::Bool(decode_bool_scalar_payload(payload, field_name)?),
        ScalarCodec::Date => {
            let days = decode_i32_payload(payload, field_name, "date")?;
            ScalarValueRef::Date(Date::from_days_since_epoch(days))
        }
        ScalarCodec::Duration => {
            let millis = decode_u64_payload(payload, field_name, "duration")?;
            ScalarValueRef::Duration(Duration::from_millis(millis))
        }
        ScalarCodec::Float32 => {
            let value = f32::from_bits(decode_u32_payload(payload, field_name, "float32")?);
            let value = Float32::try_new(value).ok_or_else(|| {
                InternalError::persisted_row_field_payload_non_finite(field_name, "float32")
            })?;
            ScalarValueRef::Float32(value)
        }
        ScalarCodec::Float64 => {
            let value = f64::from_bits(decode_u64_payload(payload, field_name, "float64")?);
            let value = Float64::try_new(value).ok_or_else(|| {
                InternalError::persisted_row_field_payload_non_finite(field_name, "float64")
            })?;
            ScalarValueRef::Float64(value)
        }
        ScalarCodec::Int64 => ScalarValueRef::Int(decode_i64_payload(payload, field_name, "int")?),
        ScalarCodec::Principal => ScalarValueRef::Principal(
            Principal::try_from_bytes(payload)
                .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?,
        ),
        ScalarCodec::Subaccount => {
            let bytes = decode_fixed(payload, field_name, "subaccount")?;
            ScalarValueRef::Subaccount(Subaccount::from_array(bytes))
        }
        ScalarCodec::Text => {
            let value = str::from_utf8(payload).map_err(|err| {
                InternalError::persisted_row_field_text_payload_invalid_utf8(field_name, err)
            })?;
            ScalarValueRef::Text(value)
        }
        ScalarCodec::Timestamp => {
            let millis = decode_i64_payload(payload, field_name, "timestamp")?;
            ScalarValueRef::Timestamp(Timestamp::from_millis(millis))
        }
        ScalarCodec::Nat64 => ScalarValueRef::Nat(decode_u64_payload(payload, field_name, "nat")?),
        ScalarCodec::Ulid => {
            let bytes = decode_fixed(payload, field_name, "ulid")?;
            ScalarValueRef::Ulid(Ulid::from_bytes(bytes))
        }
        ScalarCodec::Unit => {
            decode_unit_scalar_payload(payload, field_name)?;
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
                    Ok(encode_fixed(i64::from(*self).to_le_bytes()))
                }

                fn decode_scalar_payload(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    <$ty>::try_from(decode_i64_payload(bytes, field_name, "int")?).map_err(|_| {
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
                const CODEC: ScalarCodec = ScalarCodec::Nat64;

                fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(encode_fixed(u64::from(*self).to_le_bytes()))
                }

                fn decode_scalar_payload(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    <$ty>::try_from(decode_u64_payload(bytes, field_name, "nat")?).map_err(|_| {
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
        decode_bool_scalar_payload(bytes, field_name)
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
        Ok(encode_fixed(self.as_millis().to_le_bytes()))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Ok(Self::from_millis(decode_i64_payload(
            bytes,
            field_name,
            "timestamp",
        )?))
    }
}

impl PersistedScalar for Date {
    const CODEC: ScalarCodec = ScalarCodec::Date;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_fixed(self.as_days_since_epoch().to_le_bytes()))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Ok(Self::from_days_since_epoch(decode_i32_payload(
            bytes, field_name, "date",
        )?))
    }
}

impl PersistedScalar for Duration {
    const CODEC: ScalarCodec = ScalarCodec::Duration;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_fixed(self.as_millis().to_le_bytes()))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Ok(Self::from_millis(decode_u64_payload(
            bytes, field_name, "duration",
        )?))
    }
}

impl PersistedScalar for Float32 {
    const CODEC: ScalarCodec = ScalarCodec::Float32;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_fixed(self.get().to_bits().to_le_bytes()))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let value = f32::from_bits(decode_u32_payload(bytes, field_name, "float32")?);

        Self::try_new(value).ok_or_else(|| {
            InternalError::persisted_row_field_payload_non_finite(field_name, "float32")
        })
    }
}

impl PersistedScalar for Float64 {
    const CODEC: ScalarCodec = ScalarCodec::Float64;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(encode_fixed(self.get().to_bits().to_le_bytes()))
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        let value = f64::from_bits(decode_u64_payload(bytes, field_name, "float64")?);

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
        Ok(Self::from_array(decode_fixed(
            bytes,
            field_name,
            "subaccount",
        )?))
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
        decode_unit_scalar_payload(bytes, field_name)?;

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
        decode_unit_scalar_payload(bytes, field_name)?;

        Ok(Self)
    }
}
