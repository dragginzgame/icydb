//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: row envelope versions, typed entity materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.

use crate::{
    db::data::{
        DataKey, RawRow, StructuralRowDecodeError, StructuralRowFieldBytes,
        decode_structural_field_bytes,
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_primary_key_slot},
        field::{FieldModel, LeafCodec, ScalarCodec},
    },
    serialize::{deserialize, serialize},
    traits::EntityKind,
    types::{Blob, Date, Duration, Float32, Float64, Principal, Subaccount, Timestamp, Ulid},
    value::{StorageKey, Value},
};
use std::str;

const SCALAR_SLOT_PREFIX: u8 = 0xFF;
const SCALAR_SLOT_TAG_NULL: u8 = 0;
const SCALAR_SLOT_TAG_VALUE: u8 = 1;

///
/// SlotReader
///
/// SlotReader exposes one persisted row as stable slot-addressable fields.
/// Callers may inspect field presence, borrow raw field bytes, or decode one
/// field value on demand.
///

pub trait SlotReader {
    /// Return the structural model that owns this slot mapping.
    fn model(&self) -> &'static EntityModel;

    /// Return whether the given slot is present in the persisted row.
    fn has(&self, slot: usize) -> bool;

    /// Borrow the raw persisted payload for one slot when present.
    fn get_bytes(&self, slot: usize) -> Option<&[u8]>;

    /// Decode one slot as a scalar leaf when the field model declares a scalar codec.
    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError>;

    /// Decode one slot value on demand using the field contract declared by the model.
    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError>;
}

///
/// SlotWriter
///
/// SlotWriter is the canonical row-container output seam used by persisted-row
/// writers.
///

pub trait SlotWriter {
    /// Record one slot payload for the current row.
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError>;

    /// Record one scalar slot payload using the canonical scalar leaf envelope.
    fn write_scalar(
        &mut self,
        slot: usize,
        value: ScalarSlotValueRef<'_>,
    ) -> Result<(), InternalError> {
        let payload = encode_scalar_slot_value(value)?;

        self.write_slot(slot, Some(payload.as_slice()))
    }
}

///
/// PersistedRow
///
/// PersistedRow is the derive-owned bridge between typed entities and
/// slot-addressable persisted rows.
/// It owns entity-specific materialization/default semantics while runtime
/// paths stay structural at the row boundary.
///

pub trait PersistedRow: EntityKind + Sized {
    /// Materialize one typed entity from one slot reader.
    fn materialize_from_slots(slots: &mut dyn SlotReader) -> Result<Self, InternalError>;

    /// Write one typed entity into one slot writer.
    fn write_slots(&self, out: &mut dyn SlotWriter) -> Result<(), InternalError>;

    /// Decode one slot value needed by structural planner/projection consumers.
    fn project_slot(
        slots: &mut dyn SlotReader,
        slot: usize,
    ) -> Result<Option<Value>, InternalError>;
}

/// Decode one slot value through the declared field contract without routing
/// through `SlotReader::get_value`.
pub(in crate::db) fn decode_slot_value_by_contract(
    slots: &dyn SlotReader,
    slot: usize,
) -> Result<Option<Value>, InternalError> {
    let field = slots.model().fields().get(slot).ok_or_else(|| {
        InternalError::index_invariant(format!(
            "slot lookup outside model bounds during structural row access: model='{}' slot={slot}",
            slots.model().path(),
        ))
    })?;

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(value) = slots.get_scalar(slot)?
    {
        return Ok(Some(match value {
            ScalarSlotValueRef::Null => Value::Null,
            ScalarSlotValueRef::Value(value) => value.into_value(),
        }));
    }

    match slots.get_bytes(slot) {
        Some(raw_value) => {
            decode_structural_field_bytes(raw_value, field.kind(), field.storage_decode())
                .map(Some)
                .map_err(|err| {
                    InternalError::serialize_corruption(format!(
                        "row decode failed for field '{}' kind={:?}: {err}",
                        field.name(),
                        field.kind(),
                    ))
                })
        }
        None => Ok(None),
    }
}

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
/// avoid routing scalar fields back through CBOR.
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

/// Encode one persisted slot payload using the shared leaf codec boundary.
pub fn encode_persisted_slot_payload<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: serde::Serialize,
{
    serialize(value).map_err(|err| {
        InternalError::serialize_internal(format!(
            "row encode failed for field '{field_name}': {err}",
        ))
    })
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
        return Err(InternalError::serialize_internal(format!(
            "row encode failed for field '{field_name}': scalar payload envelope underflow",
        )));
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

/// Decode one persisted slot payload using the shared leaf codec boundary.
pub fn decode_persisted_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: serde::de::DeserializeOwned,
{
    deserialize(bytes).map_err(|err| {
        InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': {err}",
        ))
    })
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
        InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': unexpected null for non-nullable scalar field",
        ))
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

/// Build the canonical missing-field error for persisted-row materialization.
pub fn missing_persisted_slot_error(field_name: &'static str) -> InternalError {
    InternalError::serialize_corruption(format!(
        "row decode failed: missing required field '{field_name}'",
    ))
}

///
/// SlotBufferWriter
///
/// SlotBufferWriter captures one row worth of slot payloads before they are
/// encoded into the canonical slot container.
///

pub(in crate::db) struct SlotBufferWriter {
    slots: Vec<Option<Vec<u8>>>,
}

impl SlotBufferWriter {
    /// Build one empty slot buffer for one entity model.
    pub(in crate::db) fn for_model(model: &'static EntityModel) -> Self {
        Self {
            slots: vec![None; model.fields().len()],
        }
    }

    /// Encode the buffered slots into the canonical row payload.
    pub(in crate::db) fn finish(self) -> Result<Vec<u8>, InternalError> {
        let field_count = u16::try_from(self.slots.len()).map_err(|_| {
            InternalError::serialize_internal(format!(
                "row encode failed: field count {} exceeds u16 slot table capacity",
                self.slots.len(),
            ))
        })?;
        let mut payload_bytes = Vec::new();
        let mut slot_table = Vec::with_capacity(self.slots.len());

        // Phase 1: assign each present slot one payload span inside the data section.
        for slot_payload in self.slots {
            match slot_payload {
                Some(bytes) => {
                    let start = u32::try_from(payload_bytes.len()).map_err(|_| {
                        InternalError::serialize_internal(
                            "row encode failed: slot payload start exceeds u32 range",
                        )
                    })?;
                    let len = u32::try_from(bytes.len()).map_err(|_| {
                        InternalError::serialize_internal(
                            "row encode failed: slot payload length exceeds u32 range",
                        )
                    })?;
                    payload_bytes.extend_from_slice(&bytes);
                    slot_table.push((start, len));
                }
                None => slot_table.push((0, 0)),
            }
        }

        // Phase 2: write the fixed-width slot header followed by concatenated payloads.
        let mut encoded = Vec::with_capacity(
            usize::from(field_count) * (u32::BITS as usize / 4) + 2 + payload_bytes.len(),
        );
        encoded.extend_from_slice(&field_count.to_be_bytes());
        for (start, len) in slot_table {
            encoded.extend_from_slice(&start.to_be_bytes());
            encoded.extend_from_slice(&len.to_be_bytes());
        }
        encoded.extend_from_slice(&payload_bytes);

        Ok(encoded)
    }
}

impl SlotWriter for SlotBufferWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = self.slots.get_mut(slot).ok_or_else(|| {
            InternalError::serialize_internal(format!(
                "row encode failed: slot {slot} is outside the row layout",
            ))
        })?;
        *entry = payload.map(<[u8]>::to_vec);

        Ok(())
    }
}

/// Encode one entity into the canonical slot-container payload.
pub(in crate::db) fn encode_persisted_row<E>(entity: &E) -> Result<Vec<u8>, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SlotBufferWriter::for_model(E::MODEL);
    entity.write_slots(&mut writer)?;
    writer.finish()
}

///
/// StructuralSlotReader
///
/// StructuralSlotReader adapts the current persisted-row bytes into the
/// canonical slot-reader seam.
/// It caches decoded field values lazily so repeated index/predicate reads do
/// not re-run the same field decoder within one row planning pass.
///

pub(in crate::db) struct StructuralSlotReader<'a> {
    model: &'static EntityModel,
    field_bytes: StructuralRowFieldBytes<'a>,
    cached_values: Vec<CachedSlotValue>,
}

impl<'a> StructuralSlotReader<'a> {
    /// Build one slot reader over one persisted row using the current structural row scanner.
    pub(in crate::db) fn from_raw_row(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        let field_bytes =
            StructuralRowFieldBytes::from_raw_row(raw_row, model).map_err(|err| match err {
                StructuralRowDecodeError::Deserialize(source) => source,
            })?;
        let cached_values = std::iter::repeat_with(|| CachedSlotValue::Pending)
            .take(model.fields().len())
            .collect();

        Ok(Self {
            model,
            field_bytes,
            cached_values,
        })
    }

    /// Validate the decoded primary-key slot against the authoritative row key.
    pub(in crate::db) fn validate_storage_key_for_entity<E: EntityKind>(
        &mut self,
        data_key: &DataKey,
    ) -> Result<(), InternalError> {
        let Some(primary_key_slot) = resolve_primary_key_slot(E::MODEL) else {
            return Err(InternalError::index_invariant(format!(
                "entity primary key field missing during structural row validation: {} field={}",
                E::PATH,
                E::PRIMARY_KEY
            )));
        };
        let field = self.field_model(primary_key_slot)?;
        let primary_key_value = match self.get_scalar(primary_key_slot)? {
            Some(ScalarSlotValueRef::Null) => Some(Value::Null),
            Some(ScalarSlotValueRef::Value(value)) => Some(value.into_value()),
            None => match self.field_bytes.field(primary_key_slot) {
                Some(raw_value) => Some(
                    decode_structural_field_bytes(raw_value, field.kind(), field.storage_decode())
                        .map_err(|err| {
                            InternalError::serialize_corruption(format!(
                                "row decode failed for primary-key field '{}' kind={:?}: {err}",
                                field.name(),
                                field.kind(),
                            ))
                        })?,
                ),
                None => None,
            },
        };
        let Some(primary_key_value) = primary_key_value else {
            return Err(InternalError::serialize_corruption(format!(
                "row decode failed: missing primary-key slot while validating {data_key}",
            )));
        };
        let decoded_key = StorageKey::try_from_value(&primary_key_value).map_err(|err| {
            InternalError::serialize_corruption(format!(
                "row decode failed: primary-key value is not storage-key encodable: {data_key} ({err})",
            ))
        })?;
        let expected_key = data_key.storage_key();

        if decoded_key != expected_key {
            return Err(InternalError::store_corruption(format!(
                "row key mismatch: expected {expected_key}, found {decoded_key}",
            )));
        }

        Ok(())
    }

    // Resolve one field model entry by stable slot index.
    fn field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        self.model.fields().get(slot).ok_or_else(|| {
            InternalError::index_invariant(format!(
                "slot lookup outside model bounds during structural row access: model='{}' slot={slot}",
                self.model.path(),
            ))
        })
    }
}

impl SlotReader for StructuralSlotReader<'_> {
    fn model(&self) -> &'static EntityModel {
        self.model
    }

    fn has(&self, slot: usize) -> bool {
        self.field_bytes.field(slot).is_some()
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.field_bytes.field(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let field = self.field_model(slot)?;
        let Some(raw_value) = self.field_bytes.field(slot) else {
            return Ok(None);
        };

        match field.leaf_codec() {
            LeafCodec::Scalar(codec) => {
                decode_scalar_slot_value(raw_value, codec, field.name()).map(Some)
            }
            LeafCodec::CborFallback => Ok(None),
        }
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        let cached = self.cached_values.get(slot).ok_or_else(|| {
            InternalError::index_invariant(format!(
                "slot cache lookup outside model bounds during structural row access: model='{}' slot={slot}",
                self.model.path(),
            ))
        })?;
        if let CachedSlotValue::Decoded(value) = cached {
            return Ok(value.clone());
        }

        let field = self.field_model(slot)?;
        let value = match self.get_scalar(slot)? {
            Some(ScalarSlotValueRef::Null) => Some(Value::Null),
            Some(ScalarSlotValueRef::Value(value)) => Some(value.into_value()),
            None => match self.field_bytes.field(slot) {
                Some(raw_value) => Some(
                    decode_structural_field_bytes(raw_value, field.kind(), field.storage_decode())
                        .map_err(|err| {
                            InternalError::serialize_corruption(format!(
                                "row decode failed for field '{}' kind={:?}: {err}",
                                field.name(),
                                field.kind(),
                            ))
                        })?,
                ),
                None => None,
            },
        };
        self.cached_values[slot] = CachedSlotValue::Decoded(value.clone());

        Ok(value)
    }
}

///
/// CachedSlotValue
///
/// CachedSlotValue tracks whether one slot has already been decoded during the
/// current structural row access pass.
///

enum CachedSlotValue {
    Pending,
    Decoded(Option<Value>),
}

// Encode one scalar slot value into the canonical prefixed scalar envelope.
fn encode_scalar_slot_value(value: ScalarSlotValueRef<'_>) -> Result<Vec<u8>, InternalError> {
    match value {
        ScalarSlotValueRef::Null => Ok(vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_NULL]),
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

            Ok(encoded)
        }
    }
}

// Split one scalar slot envelope into `NULL` vs payload bytes.
fn decode_scalar_slot_payload_body<'a>(
    bytes: &'a [u8],
    field_name: &'static str,
) -> Result<Option<&'a [u8]>, InternalError> {
    let Some((&prefix, rest)) = bytes.split_first() else {
        return Err(InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': empty scalar payload",
        )));
    };
    if prefix != SCALAR_SLOT_PREFIX {
        return Err(InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': scalar payload prefix mismatch",
        )));
    }
    let Some((&tag, payload)) = rest.split_first() else {
        return Err(InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': truncated scalar payload tag",
        )));
    };

    match tag {
        SCALAR_SLOT_TAG_NULL => {
            if !payload.is_empty() {
                return Err(InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': null scalar payload has trailing bytes",
                )));
            }

            Ok(None)
        }
        SCALAR_SLOT_TAG_VALUE => Ok(Some(payload)),
        _ => Err(InternalError::serialize_corruption(format!(
            "row decode failed for field '{field_name}': invalid scalar payload tag {tag}",
        ))),
    }
}

// Decode one scalar slot view using the field-declared scalar codec.
fn decode_scalar_slot_value<'a>(
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
                return Err(InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': bool payload must be exactly 1 byte",
                )));
            };
            match value {
                0 => ScalarValueRef::Bool(false),
                1 => ScalarValueRef::Bool(true),
                _ => {
                    return Err(InternalError::serialize_corruption(format!(
                        "row decode failed for field '{field_name}': invalid bool payload byte {value}",
                    )));
                }
            }
        }
        ScalarCodec::Date => {
            let bytes: [u8; 4] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': date payload must be exactly 4 bytes",
                ))
            })?;
            ScalarValueRef::Date(Date::from_days_since_epoch(i32::from_le_bytes(bytes)))
        }
        ScalarCodec::Duration => {
            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': duration payload must be exactly 8 bytes",
                ))
            })?;
            ScalarValueRef::Duration(Duration::from_millis(u64::from_le_bytes(bytes)))
        }
        ScalarCodec::Float32 => {
            let bytes: [u8; 4] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': float32 payload must be exactly 4 bytes",
                ))
            })?;
            let value = f32::from_bits(u32::from_le_bytes(bytes));
            let value = Float32::try_new(value).ok_or_else(|| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': float32 payload is non-finite",
                ))
            })?;
            ScalarValueRef::Float32(value)
        }
        ScalarCodec::Float64 => {
            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': float64 payload must be exactly 8 bytes",
                ))
            })?;
            let value = f64::from_bits(u64::from_le_bytes(bytes));
            let value = Float64::try_new(value).ok_or_else(|| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': float64 payload is non-finite",
                ))
            })?;
            ScalarValueRef::Float64(value)
        }
        ScalarCodec::Int64 => {
            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': int payload must be exactly 8 bytes",
                ))
            })?;
            ScalarValueRef::Int(i64::from_le_bytes(bytes))
        }
        ScalarCodec::Principal => {
            ScalarValueRef::Principal(Principal::try_from_bytes(payload).map_err(|err| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': {err}",
                ))
            })?)
        }
        ScalarCodec::Subaccount => {
            let bytes: [u8; 32] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': subaccount payload must be exactly 32 bytes",
                ))
            })?;
            ScalarValueRef::Subaccount(Subaccount::from_array(bytes))
        }
        ScalarCodec::Text => {
            let value = str::from_utf8(payload).map_err(|err| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': invalid UTF-8 text payload ({err})",
                ))
            })?;
            ScalarValueRef::Text(value)
        }
        ScalarCodec::Timestamp => {
            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': timestamp payload must be exactly 8 bytes",
                ))
            })?;
            ScalarValueRef::Timestamp(Timestamp::from_millis(i64::from_le_bytes(bytes)))
        }
        ScalarCodec::Uint64 => {
            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': uint payload must be exactly 8 bytes",
                ))
            })?;
            ScalarValueRef::Uint(u64::from_le_bytes(bytes))
        }
        ScalarCodec::Ulid => {
            let bytes: [u8; 16] = payload.try_into().map_err(|_| {
                InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': ulid payload must be exactly 16 bytes",
                ))
            })?;
            ScalarValueRef::Ulid(Ulid::from_bytes(bytes))
        }
        ScalarCodec::Unit => {
            if !payload.is_empty() {
                return Err(InternalError::serialize_corruption(format!(
                    "row decode failed for field '{field_name}': unit payload must be empty",
                )));
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
                    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                        InternalError::serialize_corruption(format!(
                            "row decode failed for field '{field_name}': int payload must be exactly 8 bytes",
                        ))
                    })?;
                    <$ty>::try_from(i64::from_le_bytes(raw)).map_err(|_| {
                        InternalError::serialize_corruption(format!(
                            "row decode failed for field '{field_name}': integer payload out of range for target type",
                        ))
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
                    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                        InternalError::serialize_corruption(format!(
                            "row decode failed for field '{field_name}': uint payload must be exactly 8 bytes",
                        ))
                    })?;
                    <$ty>::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                        InternalError::serialize_corruption(format!(
                            "row decode failed for field '{field_name}': unsigned payload out of range for target type",
                        ))
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
            return Err(InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': bool payload must be exactly 1 byte",
            )));
        };

        match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': invalid bool payload byte {value}",
            ))),
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
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': invalid UTF-8 text payload ({err})",
            ))
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
        Ulid::try_from_bytes(bytes).map_err(|err| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': {err}",
            ))
        })
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
        let raw: [u8; 8] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': timestamp payload must be exactly 8 bytes",
            ))
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
        let raw: [u8; 4] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': date payload must be exactly 4 bytes",
            ))
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
        let raw: [u8; 8] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': duration payload must be exactly 8 bytes",
            ))
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
        let raw: [u8; 4] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': float32 payload must be exactly 4 bytes",
            ))
        })?;
        let value = f32::from_bits(u32::from_le_bytes(raw));

        Self::try_new(value).ok_or_else(|| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': float32 payload is non-finite",
            ))
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
        let raw: [u8; 8] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': float64 payload must be exactly 8 bytes",
            ))
        })?;
        let value = f64::from_bits(u64::from_le_bytes(raw));

        Self::try_new(value).ok_or_else(|| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': float64 payload is non-finite",
            ))
        })
    }
}

impl PersistedScalar for Principal {
    const CODEC: ScalarCodec = ScalarCodec::Principal;

    fn encode_scalar_payload(&self) -> Result<Vec<u8>, InternalError> {
        self.to_bytes().map_err(|err| {
            InternalError::serialize_internal(format!(
                "row encode failed for principal field: {err}",
            ))
        })
    }

    fn decode_scalar_payload(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        Self::try_from_bytes(bytes).map_err(|err| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': {err}",
            ))
        })
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
        let raw: [u8; 32] = bytes.try_into().map_err(|_| {
            InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': subaccount payload must be exactly 32 bytes",
            ))
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
            return Err(InternalError::serialize_corruption(format!(
                "row decode failed for field '{field_name}': unit payload must be empty",
            )));
        }

        Ok(())
    }
}
