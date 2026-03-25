//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: row envelope versions, typed entity materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.

use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            DataKey, RawRow, StructuralRowDecodeError, StructuralRowFieldBytes,
            decode_storage_key_field_bytes, decode_structural_field_by_kind_bytes,
            decode_structural_value_storage_bytes,
        },
        scalar_expr::compile_scalar_literal_expr_value,
        schema::{field_type_from_model_kind, literal_matches_type},
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_field_slot, resolve_primary_key_slot},
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
    },
    serialize::{deserialize, serialize},
    traits::EntityKind,
    types::{Blob, Date, Duration, Float32, Float64, Principal, Subaccount, Timestamp, Ulid},
    value::{StorageKey, Value, ValueEnum},
};
use serde_cbor::{Value as CborValue, value::to_value as to_cbor_value};
use std::{cmp::Ordering, collections::BTreeMap, str};

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
/// FieldSlot
///
/// FieldSlot
///
/// FieldSlot is the structural stable slot reference used by the `0.64`
/// patching path.
/// It intentionally carries only the model-local slot index so field-level
/// mutation stays structural instead of reintroducing typed entity helpers.
///

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldSlot {
    index: usize,
}

#[allow(dead_code)]
impl FieldSlot {
    /// Resolve one stable field slot by runtime field name.
    #[must_use]
    pub(in crate::db) fn resolve(model: &'static EntityModel, field_name: &str) -> Option<Self> {
        resolve_field_slot(model, field_name).map(|index| Self { index })
    }

    /// Build one stable field slot from an already validated index.
    pub(in crate::db) fn from_index(
        model: &'static EntityModel,
        index: usize,
    ) -> Result<Self, InternalError> {
        field_model_for_slot(model, index)?;

        Ok(Self { index })
    }

    /// Return the stable slot index inside `EntityModel::fields`.
    #[must_use]
    pub(in crate::db) const fn index(self) -> usize {
        self.index
    }
}

///
/// FieldUpdate
///
/// FieldUpdate
///
/// FieldUpdate carries one ordered field-level mutation over the structural
/// persisted-row boundary.
/// `UpdatePatch` applies these entries in order and last write wins for the
/// same slot.
///

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldUpdate {
    slot: FieldSlot,
    value: Value,
}

#[allow(dead_code)]
impl FieldUpdate {
    /// Build one field-level structural update.
    #[must_use]
    pub(in crate::db) const fn new(slot: FieldSlot, value: Value) -> Self {
        Self { slot, value }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Return the runtime value payload for this update.
    #[must_use]
    pub(in crate::db) const fn value(&self) -> &Value {
        &self.value
    }
}

///
/// UpdatePatch
///
/// UpdatePatch
///
/// UpdatePatch is the ordered structural mutation program applied to one
/// persisted row.
/// This is the phase-1 `0.64` patch container: it updates slot values
/// structurally and then re-encodes the full row.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdatePatch {
    entries: Vec<FieldUpdate>,
}

impl UpdatePatch {
    /// Build one empty patch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append one structural field update in declaration order.
    #[must_use]
    pub(in crate::db) fn set(mut self, slot: FieldSlot, value: Value) -> Self {
        self.entries.push(FieldUpdate::new(slot, value));
        self
    }

    /// Resolve one field name and append its structural update.
    pub fn set_field(
        self,
        model: &'static EntityModel,
        field_name: &str,
        value: Value,
    ) -> Result<Self, InternalError> {
        let Some(slot) = FieldSlot::resolve(model, field_name) else {
            return Err(InternalError::mutation_structural_field_unknown(
                model.path(),
                field_name,
            ));
        };

        Ok(self.set(slot, value))
    }

    /// Borrow the ordered field updates carried by this patch.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[FieldUpdate] {
        self.entries.as_slice()
    }

    /// Return whether this patch carries no field updates.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

///
/// SerializedFieldUpdate
///
/// SerializedFieldUpdate
///
/// SerializedFieldUpdate carries one ordered field-level mutation after the
/// owning persisted-row field codec has already lowered the runtime `Value`
/// into canonical slot payload bytes.
/// This lets later patch-application stages consume one mechanical slot-patch
/// artifact instead of rebuilding per-field encode dispatch.
///

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SerializedFieldUpdate {
    slot: FieldSlot,
    payload: Option<Vec<u8>>,
}

#[allow(dead_code)]
impl SerializedFieldUpdate {
    /// Build one serialized structural field update.
    #[must_use]
    pub(in crate::db) const fn new(slot: FieldSlot, payload: Option<Vec<u8>>) -> Self {
        Self { slot, payload }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Borrow the canonical slot payload bytes for this update when present.
    #[must_use]
    pub(in crate::db) fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}

///
/// SerializedUpdatePatch
///
/// SerializedUpdatePatch
///
/// SerializedUpdatePatch is the canonical serialized form of `UpdatePatch`
/// over persisted-row slot payload bytes.
/// This is the structural patch artifact later write-path stages can stage or
/// replay without re-entering field-contract encode logic.
///

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SerializedUpdatePatch {
    entries: Vec<SerializedFieldUpdate>,
}

#[allow(dead_code)]
impl SerializedUpdatePatch {
    /// Build one serialized patch from already encoded slot payloads.
    #[must_use]
    pub(in crate::db) const fn new(entries: Vec<SerializedFieldUpdate>) -> Self {
        Self { entries }
    }

    /// Borrow the ordered serialized field updates carried by this patch.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[SerializedFieldUpdate] {
        self.entries.as_slice()
    }

    /// Return whether this serialized patch carries no field updates.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

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
        let payload = encode_scalar_slot_value(value);

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
    fn project_slot(slots: &mut dyn SlotReader, slot: usize) -> Result<Option<Value>, InternalError>
    where
        Self: crate::traits::FieldProjection,
    {
        let entity = Self::materialize_from_slots(slots)?;

        Ok(<Self as crate::traits::FieldProjection>::get_value_by_index(&entity, slot))
    }
}

/// Decode one slot value through the declared field contract without routing
/// through `SlotReader::get_value`.
pub(in crate::db) fn decode_slot_value_by_contract(
    slots: &dyn SlotReader,
    slot: usize,
) -> Result<Option<Value>, InternalError> {
    let Some(raw_value) = slots.get_bytes(slot) else {
        return Ok(None);
    };

    decode_slot_value_from_bytes(slots.model(), slot, raw_value).map(Some)
}

/// Decode one structural slot payload using the owning model field contract.
///
/// This is the canonical field-level decode boundary for persisted-row bytes.
/// Higher-level row readers may still cache decoded values, but they should not
/// rebuild scalar-vs-CBOR field dispatch themselves.
pub(in crate::db) fn decode_slot_value_from_bytes(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let field = field_model_for_slot(model, slot)?;

    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => match decode_scalar_slot_value(raw_value, codec, field.name())?
        {
            ScalarSlotValueRef::Null => Ok(Value::Null),
            ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
        },
        LeafCodec::CborFallback => decode_non_scalar_slot_value(raw_value, field),
    }
}

/// Encode one structural slot value using the owning model field contract.
///
/// This is the initial `0.64` write-side field-codec boundary. It currently
/// covers:
/// - scalar leaf slots
/// - `FieldStorageDecode::Value` slots
///
/// Composite `ByKind` field encoding remains a follow-up slice so the runtime
/// can add one structural encoder owner instead of quietly rebuilding typed
/// per-field branches.
#[allow(dead_code)]
pub(in crate::db) fn encode_slot_value_from_value(
    model: &'static EntityModel,
    slot: usize,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let field = field_model_for_slot(model, slot)?;
    ensure_slot_value_matches_field_contract(field, value)?;

    match field.storage_decode() {
        FieldStorageDecode::Value => serialize(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err)),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(_) => {
                let scalar = compile_scalar_literal_expr_value(value).ok_or_else(|| {
                    InternalError::persisted_row_field_encode_failed(
                        field.name(),
                        format!(
                            "field kind {:?} requires a scalar runtime value, found {value:?}",
                            field.kind()
                        ),
                    )
                })?;

                Ok(encode_scalar_slot_value(scalar.as_slot_value_ref()))
            }
            LeafCodec::CborFallback => {
                encode_structural_field_bytes_by_kind(field.kind(), value, field.name())
            }
        },
    }
}

/// Apply one ordered structural patch to one raw row using the current
/// persisted-row field codec authority.
#[allow(dead_code)]
pub(in crate::db) fn apply_update_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &UpdatePatch,
) -> Result<RawRow, InternalError> {
    let serialized_patch = serialize_update_patch_fields(model, patch)?;

    apply_serialized_update_patch_to_raw_row(model, raw_row, &serialized_patch)
}

/// Serialize one ordered structural patch into canonical slot payload bytes.
///
/// This is the phase-1 partial-serialization seam for `0.64`: later mutation
/// stages can stage or replay one field patch without rebuilding the runtime
/// value-to-bytes contract per consumer.
#[allow(dead_code)]
pub(in crate::db) fn serialize_update_patch_fields(
    model: &'static EntityModel,
    patch: &UpdatePatch,
) -> Result<SerializedUpdatePatch, InternalError> {
    if patch.is_empty() {
        return Ok(SerializedUpdatePatch::default());
    }

    let mut entries = Vec::with_capacity(patch.entries().len());

    // Phase 1: validate and encode each ordered field update through the
    // canonical slot codec owner.
    for entry in patch.entries() {
        let slot = entry.slot();
        let payload = encode_slot_value_from_value(model, slot.index(), entry.value())?;
        entries.push(SerializedFieldUpdate::new(slot, Some(payload)));
    }

    Ok(SerializedUpdatePatch::new(entries))
}

/// Serialize one full typed entity image into the canonical serialized patch
/// artifact used by row-boundary patch replay.
///
/// This keeps typed save/update APIs on the existing surface while moving the
/// actual after-image staging onto the structural slot-patch boundary.
#[allow(dead_code)]
pub(in crate::db) fn serialize_entity_slots_as_update_patch<E>(
    entity: &E,
) -> Result<SerializedUpdatePatch, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SerializedPatchWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned persisted-row writer emit the complete
    // structural slot image for this entity.
    entity.write_slots(&mut writer)?;

    // Phase 2: require a dense slot image so save/update replay remains
    // equivalent to the existing full-row write semantics.
    writer.finish_complete()
}

/// Apply one serialized structural patch to one raw row.
///
/// This mechanical replay step no longer owns any `Value -> bytes` dispatch.
/// It only replays already encoded slot payloads over the current row layout.
#[allow(dead_code)]
pub(in crate::db) fn apply_serialized_update_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &SerializedUpdatePatch,
) -> Result<RawRow, InternalError> {
    if patch.is_empty() {
        return Ok(raw_row.clone());
    }

    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;
    let mut writer = SlotBufferWriter::for_model(model);

    // Phase 1: replay the current row layout slot-by-slot, overriding only the
    // patched fields with their already serialized payload bytes.
    for (slot, patch_payload) in patch_payloads.iter().enumerate() {
        match patch_payload {
            Some(SerializedSlotPatchRef::Set(payload)) => {
                writer.write_slot(slot, Some(payload))?;
            }
            Some(SerializedSlotPatchRef::Clear) => {
                writer.write_slot(slot, None)?;
            }
            None => {
                writer.write_slot(slot, field_bytes.field(slot))?;
            }
        }
    }

    // Phase 2: wrap the new slot payload bytes back into the canonical row
    // envelope.
    let payload = writer.finish()?;
    let encoded = serialize_row_payload(payload)?;

    RawRow::try_new(encoded).map_err(InternalError::from)
}

// Decode one non-scalar slot through the exact persisted contract declared by
// the field model.
fn decode_non_scalar_slot_value(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<Value, InternalError> {
    let decoded = match field.storage_decode() {
        crate::model::field::FieldStorageDecode::ByKind => {
            decode_structural_field_by_kind_bytes(raw_value, field.kind())
        }
        crate::model::field::FieldStorageDecode::Value => {
            decode_structural_value_storage_bytes(raw_value)
        }
    };

    decoded.map_err(|err| {
        InternalError::persisted_row_field_kind_decode_failed(field.name(), field.kind(), err)
    })
}

// Validate one runtime value against the persisted field contract before field-
// level structural encoding writes bytes into a row slot.
#[allow(dead_code)]
fn ensure_slot_value_matches_field_contract(
    field: &FieldModel,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    if matches!(field.kind(), FieldKind::Structured { queryable: false })
        && matches!(field.storage_decode(), FieldStorageDecode::Value)
    {
        return ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value);
    }

    let field_type = field_type_from_model_kind(&field.kind());
    if !literal_matches_type(value, &field_type) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            format!(
                "field kind {:?} does not accept runtime value {value:?}",
                field.kind()
            ),
        ));
    }

    ensure_decimal_scale_matches(field.name(), field.kind(), value)?;
    ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value)
}

// Enforce fixed decimal scales through nested collection/map shapes before a
// field-level patch value is persisted.
#[allow(dead_code)]
fn ensure_decimal_scale_matches(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    match (kind, value) {
        (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
            if decimal.scale() != scale {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!(
                        "decimal scale mismatch: expected {scale}, found {}",
                        decimal.scale()
                    ),
                ));
            }

            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            ensure_decimal_scale_matches(field_name, *key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            for item in items {
                ensure_decimal_scale_matches(field_name, *inner, item)?;
            }

            Ok(())
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => {
            for (entry_key, entry_value) in entries {
                ensure_decimal_scale_matches(field_name, *key, entry_key)?;
                ensure_decimal_scale_matches(field_name, *map_value, entry_value)?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Enforce the canonical persisted ordering rules for set/map shapes before one
// field-level patch value becomes row bytes.
#[allow(dead_code)]
fn ensure_value_is_deterministic_for_storage(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    match (kind, value) {
        (FieldKind::Set(_), Value::List(items)) => {
            for pair in items.windows(2) {
                let [left, right] = pair else {
                    continue;
                };
                if Value::canonical_cmp(left, right) != Ordering::Less {
                    return Err(InternalError::persisted_row_field_encode_failed(
                        field_name,
                        "set payload must already be canonical and deduplicated",
                    ));
                }
            }

            Ok(())
        }
        (FieldKind::Map { .. }, Value::Map(entries)) => {
            Value::validate_map_entries(entries.as_slice())
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?;

            if !Value::map_entries_are_strictly_canonical(entries.as_slice()) {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    "map payload must already be canonical and deduplicated",
                ));
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Materialize the last-write-wins serialized patch view indexed by stable slot.
fn serialized_patch_payload_by_slot<'a>(
    model: &'static EntityModel,
    patch: &'a SerializedUpdatePatch,
) -> Result<Vec<Option<SerializedSlotPatchRef<'a>>>, InternalError> {
    let mut payloads = vec![None; model.fields().len()];

    for entry in patch.entries() {
        let slot = entry.slot().index();
        field_model_for_slot(model, slot)?;
        payloads[slot] = Some(match entry.payload() {
            Some(payload) => SerializedSlotPatchRef::Set(payload),
            None => SerializedSlotPatchRef::Clear,
        });
    }

    Ok(payloads)
}

///
/// SerializedSlotPatchRef
///
/// SerializedSlotPatchRef
///
/// SerializedSlotPatchRef is the borrowed replay view used while applying one
/// serialized patch over an existing row layout.
/// It preserves the distinction between "set these bytes" and "clear this
/// slot" without forcing row replay to reason about runtime `Value`s.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SerializedSlotPatchRef<'a> {
    Clear,
    Set(&'a [u8]),
}

// Encode one `ByKind` field payload into the raw CBOR shape expected by the
// structural field decoder.
fn encode_structural_field_bytes_by_kind(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let cbor_value = encode_structural_field_cbor_by_kind(kind, value, field_name)?;

    serialize(&cbor_value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Encode one `ByKind` field payload into its raw CBOR value form.
fn encode_structural_field_cbor_by_kind(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<CborValue, InternalError> {
    match (kind, value) {
        (_, Value::Null) => Ok(CborValue::Null),
        (FieldKind::Blob, Value::Blob(value)) => Ok(CborValue::Bytes(value.clone())),
        (FieldKind::Bool, Value::Bool(value)) => Ok(CborValue::Bool(*value)),
        (FieldKind::Text, Value::Text(value)) => Ok(CborValue::Text(value.clone())),
        (FieldKind::Int, Value::Int(value)) => Ok(CborValue::Integer(i128::from(*value))),
        (FieldKind::Uint, Value::Uint(value)) => Ok(CborValue::Integer(i128::from(*value))),
        (FieldKind::Float32, Value::Float32(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Float64, Value::Float64(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Int128, Value::Int128(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Uint128, Value::Uint128(value)) => to_cbor_value(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err)),
        (FieldKind::Ulid, Value::Ulid(value)) => Ok(CborValue::Text(value.to_string())),
        (FieldKind::Account, Value::Account(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Date, Value::Date(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Decimal { .. }, Value::Decimal(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Duration, Value::Duration(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::IntBig, Value::IntBig(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Principal, Value::Principal(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Subaccount, Value::Subaccount(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::Timestamp, Value::Timestamp(value)) => {
            encode_leaf_cbor_value(value, field_name)
        }
        (FieldKind::UintBig, Value::UintBig(value)) => encode_leaf_cbor_value(value, field_name),
        (FieldKind::Unit, Value::Unit) => encode_leaf_cbor_value(&(), field_name),
        (FieldKind::Relation { key_kind, .. }, value) => {
            encode_structural_field_cbor_by_kind(*key_kind, value, field_name)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            Ok(CborValue::Array(
                items
                    .iter()
                    .map(|item| encode_structural_field_cbor_by_kind(*inner, item, field_name))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            let mut encoded = BTreeMap::new();
            for (entry_key, entry_value) in entries {
                encoded.insert(
                    encode_structural_field_cbor_by_kind(*key, entry_key, field_name)?,
                    encode_structural_field_cbor_by_kind(*value, entry_value, field_name)?,
                );
            }

            Ok(CborValue::Map(encoded))
        }
        (FieldKind::Enum { path, variants }, Value::Enum(value)) => {
            encode_enum_cbor_value(path, variants, value, field_name)
        }
        (FieldKind::Structured { .. }, _) => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            "structured ByKind field encoding is unsupported",
        )),
        _ => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {kind:?} does not accept runtime value {value:?}"),
        )),
    }
}

// Encode one typed leaf wrapper into its raw CBOR value form.
fn encode_leaf_cbor_value<T>(value: &T, field_name: &str) -> Result<CborValue, InternalError>
where
    T: serde::Serialize,
{
    to_cbor_value(value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Encode one enum field using the same unit-vs-one-entry-map envelope expected
// by structural enum decode.
fn encode_enum_cbor_value(
    path: &'static str,
    variants: &'static [crate::model::field::EnumVariantModel],
    value: &ValueEnum,
    field_name: &str,
) -> Result<CborValue, InternalError> {
    if let Some(actual_path) = value.path()
        && actual_path != path
    {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("enum path mismatch: expected '{path}', found '{actual_path}'"),
        ));
    }

    let Some(payload) = value.payload() else {
        return Ok(CborValue::Text(value.variant().to_string()));
    };

    let Some(variant_model) = variants.iter().find(|item| item.ident() == value.variant()) else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "unknown enum variant '{}' for path '{path}'",
                value.variant()
            ),
        ));
    };
    let Some(payload_kind) = variant_model.payload_kind() else {
        return Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "enum variant '{}' does not accept a payload",
                value.variant()
            ),
        ));
    };

    let payload_value = match variant_model.payload_storage_decode() {
        FieldStorageDecode::ByKind => {
            encode_structural_field_cbor_by_kind(*payload_kind, payload, field_name)?
        }
        FieldStorageDecode::Value => to_cbor_value(payload)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?,
    };

    let mut encoded = BTreeMap::new();
    encoded.insert(CborValue::Text(value.variant().to_string()), payload_value);

    Ok(CborValue::Map(encoded))
}

// Resolve one field model entry by stable slot index.
fn field_model_for_slot(
    model: &'static EntityModel,
    slot: usize,
) -> Result<&'static FieldModel, InternalError> {
    model
        .fields()
        .get(slot)
        .ok_or_else(|| InternalError::persisted_row_slot_lookup_out_of_bounds(model.path(), slot))
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
    serialize(value)
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

/// Decode one persisted slot payload using the shared leaf codec boundary.
pub fn decode_persisted_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: serde::de::DeserializeOwned,
{
    deserialize(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
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
            InternalError::persisted_row_encode_failed(format!(
                "field count {} exceeds u16 slot table capacity",
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
                        InternalError::persisted_row_encode_failed(
                            "slot payload start exceeds u32 range",
                        )
                    })?;
                    let len = u32::try_from(bytes.len()).map_err(|_| {
                        InternalError::persisted_row_encode_failed(
                            "slot payload length exceeds u32 range",
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
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is outside the row layout",
            ))
        })?;
        *entry = payload.map(<[u8]>::to_vec);

        Ok(())
    }
}

///
/// SerializedPatchWriter
///
/// SerializedPatchWriter
///
/// SerializedPatchWriter captures a dense typed entity slot image into the
/// serialized patch artifact used by `0.64` mutation staging.
/// Unlike `SlotBufferWriter`, this writer does not flatten into one row payload;
/// it preserves slot-level ownership so later stages can replay the row through
/// the structural patch boundary.
///

struct SerializedPatchWriter {
    model: &'static EntityModel,
    slots: Vec<PatchWriterSlot>,
}

impl SerializedPatchWriter {
    /// Build one empty serialized patch writer for one entity model.
    fn for_model(model: &'static EntityModel) -> Self {
        Self {
            model,
            slots: vec![PatchWriterSlot::Missing; model.fields().len()],
        }
    }

    /// Materialize one dense serialized patch, erroring if the writer failed
    /// to emit any declared slot.
    fn finish_complete(self) -> Result<SerializedUpdatePatch, InternalError> {
        let mut entries = Vec::with_capacity(self.slots.len());

        // Phase 1: require a complete slot image so typed save/update staging
        // stays equivalent to the existing full-row encoder.
        for (slot, payload) in self.slots.into_iter().enumerate() {
            let field_slot = FieldSlot::from_index(self.model, slot)?;
            let serialized = match payload {
                PatchWriterSlot::Set(payload) => {
                    SerializedFieldUpdate::new(field_slot, Some(payload))
                }
                PatchWriterSlot::Clear => SerializedFieldUpdate::new(field_slot, None),
                PatchWriterSlot::Missing => {
                    return Err(InternalError::persisted_row_encode_failed(format!(
                        "serialized patch writer did not emit slot {slot} for entity '{}'",
                        self.model.path()
                    )));
                }
            };
            entries.push(serialized);
        }

        Ok(SerializedUpdatePatch::new(entries))
    }
}

impl SlotWriter for SerializedPatchWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = self.slots.get_mut(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is outside the row layout",
            ))
        })?;
        *entry = match payload {
            Some(payload) => PatchWriterSlot::Set(payload.to_vec()),
            None => PatchWriterSlot::Clear,
        };

        Ok(())
    }
}

///
/// PatchWriterSlot
///
/// PatchWriterSlot
///
/// PatchWriterSlot tracks whether one dense slot-image writer has emitted a
/// payload, emitted an explicit clear, or failed to visit the slot at all.
/// That lets the typed save/update bridge reject incomplete writers instead of
/// silently leaving stale bytes in the baseline row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum PatchWriterSlot {
    Missing,
    Clear,
    Set(Vec<u8>),
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
        let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
            .map_err(StructuralRowDecodeError::into_internal_error)?;
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
    pub(in crate::db) fn validate_storage_key(
        &self,
        data_key: &DataKey,
    ) -> Result<(), InternalError> {
        let Some(primary_key_slot) = resolve_primary_key_slot(self.model) else {
            return Err(InternalError::persisted_row_primary_key_field_missing(
                self.model.path(),
            ));
        };
        let field = self.field_model(primary_key_slot)?;
        let decoded_key = match self.get_scalar(primary_key_slot)? {
            Some(ScalarSlotValueRef::Null) => None,
            Some(ScalarSlotValueRef::Value(value)) => storage_key_from_scalar_ref(value),
            None => match self.field_bytes.field(primary_key_slot) {
                Some(raw_value) => Some(
                    decode_storage_key_field_bytes(raw_value, field.kind).map_err(|err| {
                        InternalError::persisted_row_primary_key_not_storage_encodable(
                            data_key, err,
                        )
                    })?,
                ),
                None => None,
            },
        };
        let Some(decoded_key) = decoded_key else {
            return Err(InternalError::persisted_row_primary_key_slot_missing(
                data_key,
            ));
        };
        let expected_key = data_key.storage_key();

        if decoded_key != expected_key {
            return Err(InternalError::persisted_row_key_mismatch(
                expected_key,
                decoded_key,
            ));
        }

        Ok(())
    }

    // Resolve one field model entry by stable slot index.
    fn field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        field_model_for_slot(self.model, slot)
    }
}

// Convert one scalar slot fast-path value into its storage-key form when the
// field kind is storage-key-compatible.
const fn storage_key_from_scalar_ref(value: ScalarValueRef<'_>) -> Option<StorageKey> {
    match value {
        ScalarValueRef::Int(value) => Some(StorageKey::Int(value)),
        ScalarValueRef::Principal(value) => Some(StorageKey::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(StorageKey::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(StorageKey::Timestamp(value)),
        ScalarValueRef::Uint(value) => Some(StorageKey::Uint(value)),
        ScalarValueRef::Ulid(value) => Some(StorageKey::Ulid(value)),
        ScalarValueRef::Unit => Some(StorageKey::Unit),
        _ => None,
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
            InternalError::persisted_row_slot_cache_lookup_out_of_bounds(self.model.path(), slot)
        })?;
        if let CachedSlotValue::Decoded(value) = cached {
            return Ok(value.clone());
        }

        let value = match self.field_bytes.field(slot) {
            Some(raw_value) => Some(decode_slot_value_from_bytes(self.model, slot, raw_value)?),
            None => None,
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
fn encode_scalar_slot_value(value: ScalarSlotValueRef<'_>) -> Vec<u8> {
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
            "scalar payload prefix mismatch",
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        FieldSlot, ScalarSlotValueRef, ScalarValueRef, SlotBufferWriter, SlotReader, SlotWriter,
        UpdatePatch, apply_update_patch_to_raw_row, decode_slot_value_by_contract,
        decode_slot_value_from_bytes, encode_scalar_slot_value, encode_slot_value_from_value,
        serialize_entity_slots_as_update_patch, serialize_update_patch_fields,
    };
    use crate::{
        db::{
            codec::serialize_row_payload,
            data::{RawRow, StructuralSlotReader},
        },
        model::{
            EntityModel,
            field::{EnumVariantModel, FieldKind, FieldModel, FieldStorageDecode},
        },
        testing::SIMPLE_ENTITY_TAG,
        traits::EntitySchema,
        types::{Account, Principal, Subaccount},
        value::{Value, ValueEnum},
    };
    use icydb_derive::{FieldProjection, PersistedRow};
    use serde::{Deserialize, Serialize};

    crate::test_canister! {
        ident = PersistedRowPatchBridgeCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = PersistedRowPatchBridgeStore,
        canister = PersistedRowPatchBridgeCanister,
    }

    ///
    /// PersistedRowPatchBridgeEntity
    ///
    /// PersistedRowPatchBridgeEntity
    ///
    /// PersistedRowPatchBridgeEntity is the smallest derive-owned entity used
    /// to validate the typed-entity -> serialized-patch bridge.
    /// It lets the persisted-row tests exercise the same dense slot writer the
    /// save/update path now uses.
    ///

    #[derive(
        Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
    )]
    struct PersistedRowPatchBridgeEntity {
        id: crate::types::Ulid,
        name: String,
    }

    crate::test_entity_schema! {
        ident = PersistedRowPatchBridgeEntity,
        id = crate::types::Ulid,
        id_field = id,
        entity_name = "PersistedRowPatchBridgeEntity",
        entity_tag = SIMPLE_ENTITY_TAG,
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text),
        ],
        indexes = [],
        store = PersistedRowPatchBridgeStore,
        canister = PersistedRowPatchBridgeCanister,
    }

    static STATE_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
        "Loaded",
        Some(&FieldKind::Uint),
        FieldStorageDecode::ByKind,
    )];
    static FIELD_MODELS: [FieldModel; 2] = [
        FieldModel::new("name", FieldKind::Text),
        FieldModel::new_with_storage_decode("payload", FieldKind::Text, FieldStorageDecode::Value),
    ];
    static LIST_FIELD_MODELS: [FieldModel; 1] =
        [FieldModel::new("tags", FieldKind::List(&FieldKind::Text))];
    static MAP_FIELD_MODELS: [FieldModel; 1] = [FieldModel::new(
        "props",
        FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Uint,
        },
    )];
    static ENUM_FIELD_MODELS: [FieldModel; 1] = [FieldModel::new(
        "state",
        FieldKind::Enum {
            path: "tests::State",
            variants: STATE_VARIANTS,
        },
    )];
    static ACCOUNT_FIELD_MODELS: [FieldModel; 1] = [FieldModel::new("owner", FieldKind::Account)];
    static INDEX_MODELS: [&crate::model::index::IndexModel; 0] = [];
    static TEST_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowFieldCodecEntity",
        "persisted_row_field_codec_entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_MODELS,
    );
    static LIST_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowListFieldCodecEntity",
        "persisted_row_list_field_codec_entity",
        &LIST_FIELD_MODELS[0],
        &LIST_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static MAP_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowMapFieldCodecEntity",
        "persisted_row_map_field_codec_entity",
        &MAP_FIELD_MODELS[0],
        &MAP_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static ENUM_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowEnumFieldCodecEntity",
        "persisted_row_enum_field_codec_entity",
        &ENUM_FIELD_MODELS[0],
        &ENUM_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static ACCOUNT_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowAccountFieldCodecEntity",
        "persisted_row_account_field_codec_entity",
        &ACCOUNT_FIELD_MODELS[0],
        &ACCOUNT_FIELD_MODELS,
        &INDEX_MODELS,
    );

    #[test]
    fn decode_slot_value_from_bytes_decodes_scalar_slots_through_one_owner() {
        let payload =
            encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
        let value =
            decode_slot_value_from_bytes(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(value, Value::Text("Ada".to_string()));
    }

    #[test]
    fn decode_slot_value_from_bytes_respects_value_storage_decode_contract() {
        let payload = crate::serialize::serialize(&Value::Text("Ada".to_string()))
            .expect("encode value-storage payload");

        let value =
            decode_slot_value_from_bytes(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

        assert_eq!(value, Value::Text("Ada".to_string()));
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_scalar_slots() {
        let payload = encode_slot_value_from_value(&TEST_MODEL, 0, &Value::Text("Ada".to_string()))
            .expect("encode slot");
        let decoded =
            decode_slot_value_from_bytes(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(decoded, Value::Text("Ada".to_string()));
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_value_storage_slots() {
        let payload = encode_slot_value_from_value(&TEST_MODEL, 1, &Value::Text("Ada".to_string()))
            .expect("encode slot");
        let decoded =
            decode_slot_value_from_bytes(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

        assert_eq!(decoded, Value::Text("Ada".to_string()));
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_list_by_kind_slots() {
        let payload = encode_slot_value_from_value(
            &LIST_MODEL,
            0,
            &Value::List(vec![Value::Text("alpha".to_string())]),
        )
        .expect("encode list slot");
        let decoded =
            decode_slot_value_from_bytes(&LIST_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(decoded, Value::List(vec![Value::Text("alpha".to_string())]),);
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_map_by_kind_slots() {
        let payload = encode_slot_value_from_value(
            &MAP_MODEL,
            0,
            &Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
        )
        .expect("encode map slot");
        let decoded =
            decode_slot_value_from_bytes(&MAP_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(
            decoded,
            Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
        );
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_enum_by_kind_slots() {
        let payload = encode_slot_value_from_value(
            &ENUM_MODEL,
            0,
            &Value::Enum(
                ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7)),
            ),
        )
        .expect("encode enum slot");
        let decoded =
            decode_slot_value_from_bytes(&ENUM_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(
            decoded,
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7,))
            ),
        );
    }

    #[test]
    fn encode_slot_value_from_value_roundtrips_leaf_by_kind_wrapper_slots() {
        let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
        let payload = encode_slot_value_from_value(&ACCOUNT_MODEL, 0, &Value::Account(account))
            .expect("encode account slot");
        let decoded = decode_slot_value_from_bytes(&ACCOUNT_MODEL, 0, payload.as_slice())
            .expect("decode slot");

        assert_eq!(decoded, Value::Account(account));
    }

    #[test]
    fn encode_slot_value_from_value_rejects_unknown_enum_payload_variants() {
        let err = encode_slot_value_from_value(
            &ENUM_MODEL,
            0,
            &Value::Enum(
                ValueEnum::new("Unknown", Some("tests::State")).with_payload(Value::Uint(7)),
            ),
        )
        .expect_err("unknown enum payload should fail closed");

        assert!(err.message.contains("unknown enum variant"));
    }

    #[test]
    fn structural_slot_reader_and_direct_decode_share_the_same_field_codec_boundary() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload");
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        writer
            .write_slot(1, Some(payload.as_slice()))
            .expect("write value-storage slot");
        let raw_row = RawRow::try_new(
            serialize_row_payload(writer.finish().expect("finish slot payload"))
                .expect("serialize row payload"),
        )
        .expect("build raw row");

        let direct_slots =
            StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL).expect("decode row");
        let mut cached_slots =
            StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL).expect("decode row");

        let direct_name = decode_slot_value_by_contract(&direct_slots, 0).expect("decode name");
        let direct_payload =
            decode_slot_value_by_contract(&direct_slots, 1).expect("decode payload");
        let cached_name = cached_slots.get_value(0).expect("cached name");
        let cached_payload = cached_slots.get_value(1).expect("cached payload");

        assert_eq!(direct_name, cached_name);
        assert_eq!(direct_payload, cached_payload);
    }

    #[test]
    fn apply_update_patch_to_raw_row_updates_only_targeted_slots() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload");
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        writer
            .write_slot(1, Some(payload.as_slice()))
            .expect("write value-storage slot");
        let raw_row = RawRow::try_new(
            serialize_row_payload(writer.finish().expect("finish slot payload"))
                .expect("serialize row payload"),
        )
        .expect("build raw row");
        let patch = UpdatePatch::new().set(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            Value::Text("Grace".to_string()),
        );

        let patched =
            apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch).expect("apply patch");
        let mut reader =
            StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

        assert_eq!(
            reader.get_value(0).expect("decode slot"),
            Some(Value::Text("Grace".to_string()))
        );
        assert_eq!(
            reader.get_value(1).expect("decode slot"),
            Some(Value::Text("payload".to_string()))
        );
    }

    #[test]
    fn serialize_update_patch_fields_encodes_canonical_slot_payloads() {
        let patch = UpdatePatch::new()
            .set(
                FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
                Value::Text("Grace".to_string()),
            )
            .set(
                FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
                Value::Text("payload".to_string()),
            );

        let serialized =
            serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

        assert_eq!(serialized.entries().len(), 2);
        assert_eq!(
            decode_slot_value_from_bytes(
                &TEST_MODEL,
                serialized.entries()[0].slot().index(),
                serialized.entries()[0]
                    .payload()
                    .expect("serialized field update should carry payload"),
            )
            .expect("decode slot payload"),
            Value::Text("Grace".to_string())
        );
        assert_eq!(
            decode_slot_value_from_bytes(
                &TEST_MODEL,
                serialized.entries()[1].slot().index(),
                serialized.entries()[1]
                    .payload()
                    .expect("serialized field update should carry payload"),
            )
            .expect("decode slot payload"),
            Value::Text("payload".to_string())
        );
    }

    #[test]
    fn apply_update_patch_to_raw_row_uses_last_write_wins() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        let raw_row = RawRow::try_new(
            serialize_row_payload(writer.finish().expect("finish slot payload"))
                .expect("serialize row payload"),
        )
        .expect("build raw row");
        let slot = FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot");
        let patch = UpdatePatch::new()
            .set(slot, Value::Text("Grace".to_string()))
            .set(slot, Value::Text("Lin".to_string()));

        let patched =
            apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch).expect("apply patch");
        let mut reader =
            StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

        assert_eq!(
            reader.get_value(0).expect("decode slot"),
            Some(Value::Text("Lin".to_string()))
        );
    }

    #[test]
    fn apply_update_patch_to_raw_row_can_fill_previously_absent_slot() {
        let raw_row = RawRow::try_new(
            serialize_row_payload(
                SlotBufferWriter::for_model(&TEST_MODEL)
                    .finish()
                    .expect("finish slot payload"),
            )
            .expect("serialize row payload"),
        )
        .expect("build raw row");
        let patch = UpdatePatch::new().set(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            Value::Text("payload".to_string()),
        );

        let patched =
            apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch).expect("apply patch");
        let mut reader =
            StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

        assert_eq!(
            reader.get_value(1).expect("decode slot"),
            Some(Value::Text("payload".to_string()))
        );
    }

    #[test]
    fn apply_serialized_update_patch_to_raw_row_replays_preencoded_slots() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        let raw_row = RawRow::try_new(
            serialize_row_payload(writer.finish().expect("finish slot payload"))
                .expect("serialize row payload"),
        )
        .expect("build raw row");
        let patch = UpdatePatch::new().set(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            Value::Text("Grace".to_string()),
        );
        let serialized =
            serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

        let patched = raw_row
            .apply_serialized_update_patch(&TEST_MODEL, &serialized)
            .expect("apply serialized patch");
        let mut reader =
            StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

        assert_eq!(
            reader.get_value(0).expect("decode slot"),
            Some(Value::Text("Grace".to_string()))
        );
    }

    #[test]
    fn serialize_entity_slots_as_update_patch_replays_full_typed_after_image() {
        let old_entity = PersistedRowPatchBridgeEntity {
            id: crate::types::Ulid::from_u128(7),
            name: "Ada".to_string(),
        };
        let new_entity = PersistedRowPatchBridgeEntity {
            id: crate::types::Ulid::from_u128(7),
            name: "Grace".to_string(),
        };
        let raw_row = RawRow::from_entity(&old_entity).expect("encode old row");
        let old_decoded = raw_row
            .try_decode::<PersistedRowPatchBridgeEntity>()
            .expect("decode old entity");
        let serialized =
            serialize_entity_slots_as_update_patch(&new_entity).expect("serialize entity patch");

        let patched = raw_row
            .apply_serialized_update_patch(PersistedRowPatchBridgeEntity::MODEL, &serialized)
            .expect("apply serialized patch");
        let decoded = patched
            .try_decode::<PersistedRowPatchBridgeEntity>()
            .expect("decode patched entity");

        assert_eq!(old_decoded, old_entity);
        assert_eq!(decoded, new_entity);
    }
}
