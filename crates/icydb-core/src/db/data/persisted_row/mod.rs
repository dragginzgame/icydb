//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: row envelope versions, typed entity materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.

mod codec;

use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, DataKey, RawRow, StructuralRowDecodeError, StructuralRowFieldBytes,
            decode_storage_key_field_bytes, decode_structural_field_by_kind_bytes,
            decode_structural_value_storage_bytes,
        },
        scalar_expr::compile_scalar_literal_expr_value,
        schema::{field_type_from_model_kind, literal_matches_type},
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_field_slot, resolve_primary_key_slot},
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
    serialize::serialize,
    traits::EntityKind,
    value::{StorageKey, Value, ValueEnum},
};
use serde_cbor::{Value as CborValue, value::to_value as to_cbor_value};
use std::{borrow::Cow, cmp::Ordering, collections::BTreeMap};

use self::codec::{decode_scalar_slot_value, encode_scalar_slot_value};

pub use self::codec::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef, decode_persisted_custom_many_slot_payload,
    decode_persisted_custom_slot_payload, decode_persisted_non_null_slot_payload,
    decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload,
    encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
    encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload,
};

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
    payload: Vec<u8>,
}

#[allow(dead_code)]
impl SerializedFieldUpdate {
    /// Build one serialized structural field update.
    #[must_use]
    pub(in crate::db) const fn new(slot: FieldSlot, payload: Vec<u8>) -> Self {
        Self { slot, payload }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Borrow the canonical slot payload bytes for this update when present.
    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &[u8] {
        self.payload.as_slice()
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
/// CanonicalSlotReader
///
/// CanonicalSlotReader
///
/// CanonicalSlotReader is the stricter structural row-reader contract used
/// once `0.65` canonical-row invariants are in force.
/// Declared slots must already exist, so callers can fail closed on missing
/// payloads instead of carrying absent-slot fallback branches.
///

pub(in crate::db) trait CanonicalSlotReader: SlotReader {
    /// Borrow one declared slot payload, erroring when the persisted row is not canonical.
    fn required_bytes(&self, slot: usize) -> Result<&[u8], InternalError> {
        let field = field_model_for_slot(self.model(), slot)?;

        self.get_bytes(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    /// Read one scalar slot through the structural fast path without allowing
    /// declared-slot absence.
    fn required_scalar(&self, slot: usize) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        let field = field_model_for_slot(self.model(), slot)?;
        debug_assert!(matches!(field.leaf_codec(), LeafCodec::Scalar(_)));

        self.get_scalar(slot)?
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    /// Decode one declared slot through the owning field contract without
    /// allowing absent payloads.
    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        decode_slot_value_from_bytes(self.model(), slot, self.required_bytes(slot)?)
    }

    /// Borrow one declared slot value when the concrete reader already owns a
    /// validated decoded cache, while preserving the existing owned fallback
    /// for reader implementations that still decode on demand.
    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Owned(self.required_value_by_contract(slot)?))
    }
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

// Resolve one staged slot cell by layout index before writer-specific payload handling.
fn slot_cell_mut<T>(slots: &mut [T], slot: usize) -> Result<&mut T, InternalError> {
    slots.get_mut(slot).ok_or_else(|| {
        InternalError::persisted_row_encode_failed(
            format!("slot {slot} is outside the row layout",),
        )
    })
}

// Reject slot clears at the canonical slot-image staging boundary while keeping
// writer-specific error wording at the call site.
fn required_slot_payload_bytes<'a>(
    model: &'static EntityModel,
    writer_label: &str,
    slot: usize,
    payload: Option<&'a [u8]>,
) -> Result<&'a [u8], InternalError> {
    payload.ok_or_else(|| {
        InternalError::persisted_row_encode_failed(format!(
            "{writer_label} cannot clear slot {slot} for entity '{}'",
            model.path()
        ))
    })
}

// Encode one fixed-width slot table plus concatenated slot payload bytes into
// the canonical row payload container.
fn encode_slot_payload_from_parts(
    slot_count: usize,
    slot_table: &[(u32, u32)],
    payload_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let field_count = u16::try_from(slot_count).map_err(|_| {
        InternalError::persisted_row_encode_failed(format!(
            "field count {slot_count} exceeds u16 slot table capacity",
        ))
    })?;
    let mut encoded = Vec::with_capacity(
        usize::from(field_count) * (u32::BITS as usize / 4) + 2 + payload_bytes.len(),
    );
    encoded.extend_from_slice(&field_count.to_be_bytes());
    for (start, len) in slot_table {
        encoded.extend_from_slice(&start.to_be_bytes());
        encoded.extend_from_slice(&len.to_be_bytes());
    }
    encoded.extend_from_slice(payload_bytes);

    Ok(encoded)
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
#[cfg(test)]
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

    decode_slot_value_for_field(field, raw_value)
}

// Decode one structural slot payload once the owning field contract has
// already been resolved.
fn decode_slot_value_for_field(
    field: &FieldModel,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
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

// Decode one slot payload and immediately re-encode it through the current
// field contract so every row-emission path normalizes bytes at the boundary.
fn canonicalize_slot_payload(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let value = decode_slot_value_from_bytes(model, slot, raw_value)?;

    encode_slot_value_from_value(model, slot, &value)
}

// Build one dense canonical slot image from any slot-addressable payload source.
// Callers keep ownership of missing-slot policy while this helper centralizes
// the slot-by-slot canonicalization loop.
fn dense_canonical_slot_image_from_payload_source<'a, F>(
    model: &'static EntityModel,
    mut payload_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<&'a [u8], InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(model.fields().len());

    for slot in 0..model.fields().len() {
        let payload = payload_for_slot(slot)?;
        slot_payloads.push(canonicalize_slot_payload(model, slot, payload)?);
    }

    Ok(slot_payloads)
}

// Build one dense canonical slot image from already-decoded runtime values.
// This keeps row-emission paths from re-decoding raw slot bytes when a caller
// already owns the validated structural value cache.
fn dense_canonical_slot_image_from_value_source<'a, F>(
    model: &'static EntityModel,
    mut value_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(model.fields().len());

    for slot in 0..model.fields().len() {
        let value = value_for_slot(slot)?;
        slot_payloads.push(encode_slot_value_from_value(model, slot, value.as_ref())?);
    }

    Ok(slot_payloads)
}

// Emit one raw row from a dense canonical slot image.
fn emit_raw_row_from_slot_payloads(
    model: &'static EntityModel,
    slot_payloads: &[Vec<u8>],
) -> Result<CanonicalRow, InternalError> {
    if slot_payloads.len() != model.fields().len() {
        return Err(InternalError::persisted_row_encode_failed(format!(
            "canonical slot image expected {} slots for entity '{}', found {}",
            model.fields().len(),
            model.path(),
            slot_payloads.len()
        )));
    }

    let payload_capacity = slot_payloads
        .iter()
        .try_fold(0usize, |len, payload| len.checked_add(payload.len()))
        .ok_or_else(|| {
            InternalError::persisted_row_encode_failed(
                "canonical slot image payload length overflow",
            )
        })?;
    let mut payload_bytes = Vec::with_capacity(payload_capacity);
    let mut slot_table = Vec::with_capacity(slot_payloads.len());

    // Phase 1: flatten the already canonicalized dense slot image directly so
    // row re-emission does not clone each slot payload back through the
    // mutable slot-writer staging buffer first.
    for (slot, payload) in slot_payloads.iter().enumerate() {
        let start = u32::try_from(payload_bytes.len()).map_err(|_| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload start exceeds u32 range: slot={slot}",
            ))
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload length exceeds u32 range: slot={slot}",
            ))
        })?;
        payload_bytes.extend_from_slice(payload.as_slice());
        slot_table.push((start, len));
    }

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    let row_payload =
        encode_slot_payload_from_parts(slot_payloads.len(), slot_table.as_slice(), &payload_bytes)?;
    let encoded = serialize_row_payload(row_payload)?;
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;

    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

// Build one dense canonical slot image from a serialized patch, failing closed
// when any declared slot is missing or any payload is non-canonical.
fn dense_canonical_slot_image_from_serialized_patch(
    model: &'static EntityModel,
    patch: &SerializedUpdatePatch,
) -> Result<Vec<Vec<u8>>, InternalError> {
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;

    dense_canonical_slot_image_from_payload_source(model, |slot| {
        patch_payloads[slot].ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "serialized patch did not emit slot {slot} for entity '{}'",
                model.path()
            ))
        })
    })
}

/// Build one canonical row from one serialized structural patch that already
/// describes a full logical row image.
pub(in crate::db) fn canonical_row_from_serialized_update_patch(
    model: &'static EntityModel,
    patch: &SerializedUpdatePatch,
) -> Result<CanonicalRow, InternalError> {
    let slot_payloads = dense_canonical_slot_image_from_serialized_patch(model, patch)?;

    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

/// Build one canonical row directly from one typed entity slot writer.
pub(in crate::db) fn canonical_row_from_entity<E>(entity: &E) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SlotBufferWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned slot writer emit the complete typed row image.
    entity.write_slots(&mut writer)?;

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    let encoded = serialize_row_payload(writer.finish()?)?;
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;

    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

/// Build one canonical row from one already-decoded structural slot reader.
pub(in crate::db) fn canonical_row_from_structural_slot_reader(
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    // Phase 1: re-encode every declared slot from the already-decoded cache so
    // commit preparation does not re-enter raw field-byte decode after the
    // structural reader has already validated the row.
    let slot_payloads = dense_canonical_slot_image_from_value_source(row_fields.model, |slot| {
        row_fields
            .required_cached_value(slot)
            .map(Cow::Borrowed)
            .map_err(|_| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the structural value cache for entity '{}'",
                    row_fields.model.path()
                ))
            })
    })?;

    // Phase 2: re-emit the full image through the single row-emission owner.
    emit_raw_row_from_slot_payloads(row_fields.model, slot_payloads.as_slice())
}

// Rebuild one full canonical row image from an existing raw row before it
// crosses a storage write boundary.
pub(in crate::db) fn canonical_row_from_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 1: canonicalize every declared slot from the existing row image.
    let slot_payloads = dense_canonical_slot_image_from_payload_source(model, |slot| {
        field_bytes.field(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from the baseline row for entity '{}'",
                model.path()
            ))
        })
    })?;

    // Phase 2: re-emit the full image through the single row-emission owner.
    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

/// Apply one ordered structural patch to one raw row using the current
/// persisted-row field codec authority.
#[allow(dead_code)]
pub(in crate::db) fn apply_update_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &UpdatePatch,
) -> Result<CanonicalRow, InternalError> {
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
        entries.push(SerializedFieldUpdate::new(slot, payload));
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
) -> Result<CanonicalRow, InternalError> {
    if patch.is_empty() {
        return canonical_row_from_raw_row(model, raw_row);
    }

    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;

    // Phase 1: replay the current row layout slot-by-slot.
    // Both patch and baseline bytes are normalized through the field contract
    // so no opaque payload can cross into the emitted row image.
    let slot_payloads = dense_canonical_slot_image_from_payload_source(model, |slot| {
        if let Some(payload) = patch_payloads[slot] {
            Ok(payload)
        } else {
            field_bytes.field(slot).ok_or_else(|| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the baseline row for entity '{}'",
                    model.path()
                ))
            })
        }
    })?;

    // Phase 2: emit the rebuilt row through the single row-construction owner.
    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
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
        if field.nullable() {
            return Ok(());
        }

        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            "required field cannot store null",
        ));
    }

    // `FieldStorageDecode::Value` fields persist the generic `Value` envelope
    // directly, so storage-side validation must accept structured leaves nested
    // under collection contracts instead of reusing the predicate literal gate.
    if matches!(field.storage_decode(), FieldStorageDecode::Value) {
        if !storage_value_matches_field_kind(field.kind(), value) {
            return Err(InternalError::persisted_row_field_encode_failed(
                field.name(),
                format!(
                    "field kind {:?} does not accept runtime value {value:?}",
                    field.kind()
                ),
            ));
        }

        ensure_decimal_scale_matches(field.name(), field.kind(), value)?;

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

// Match one runtime value against the semantic field kind used by value-backed
// storage. Unlike predicate literals, non-queryable structured leaves are valid
// persisted payloads when they arrive as canonical `Value::List` / `Value::Map`
// shapes.
fn storage_value_matches_field_kind(kind: FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Account, Value::Account(_))
        | (FieldKind::Blob, Value::Blob(_))
        | (FieldKind::Bool, Value::Bool(_))
        | (FieldKind::Date, Value::Date(_))
        | (FieldKind::Decimal { .. }, Value::Decimal(_))
        | (FieldKind::Duration, Value::Duration(_))
        | (FieldKind::Enum { .. }, Value::Enum(_))
        | (FieldKind::Float32, Value::Float32(_))
        | (FieldKind::Float64, Value::Float64(_))
        | (FieldKind::Int, Value::Int(_))
        | (FieldKind::Int128, Value::Int128(_))
        | (FieldKind::IntBig, Value::IntBig(_))
        | (FieldKind::Principal, Value::Principal(_))
        | (FieldKind::Subaccount, Value::Subaccount(_))
        | (FieldKind::Text, Value::Text(_))
        | (FieldKind::Timestamp, Value::Timestamp(_))
        | (FieldKind::Uint, Value::Uint(_))
        | (FieldKind::Uint128, Value::Uint128(_))
        | (FieldKind::UintBig, Value::UintBig(_))
        | (FieldKind::Ulid, Value::Ulid(_))
        | (FieldKind::Unit, Value::Unit)
        | (FieldKind::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
        (FieldKind::Relation { key_kind, .. }, value) => {
            storage_value_matches_field_kind(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| storage_value_matches_field_kind(*inner, item)),
        (FieldKind::Map { key, value }, Value::Map(entries)) => {
            if Value::validate_map_entries(entries.as_slice()).is_err() {
                return false;
            }

            entries.iter().all(|(entry_key, entry_value)| {
                storage_value_matches_field_kind(*key, entry_key)
                    && storage_value_matches_field_kind(*value, entry_value)
            })
        }
        _ => false,
    }
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
) -> Result<Vec<Option<&'a [u8]>>, InternalError> {
    let mut payloads = vec![None; model.fields().len()];

    for entry in patch.entries() {
        let slot = entry.slot().index();
        field_model_for_slot(model, slot)?;
        payloads[slot] = Some(entry.payload());
    }

    Ok(payloads)
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
/// SlotBufferWriter
///
/// SlotBufferWriter captures one dense canonical row worth of slot payloads
/// before they are encoded into the canonical slot container.
///

pub(in crate::db) struct SlotBufferWriter {
    model: &'static EntityModel,
    slots: Vec<SlotBufferSlot>,
}

impl SlotBufferWriter {
    /// Build one empty slot buffer for one entity model.
    pub(in crate::db) fn for_model(model: &'static EntityModel) -> Self {
        Self {
            model,
            slots: vec![SlotBufferSlot::Missing; model.fields().len()],
        }
    }

    /// Encode the buffered slots into the canonical row payload.
    pub(in crate::db) fn finish(self) -> Result<Vec<u8>, InternalError> {
        let slot_count = self.slots.len();
        let mut payload_bytes = Vec::new();
        let mut slot_table = Vec::with_capacity(slot_count);

        // Phase 1: require one payload for every declared slot before the row
        // can cross the canonical persisted-row boundary.
        for (slot, slot_payload) in self.slots.into_iter().enumerate() {
            match slot_payload {
                SlotBufferSlot::Set(bytes) => {
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
                SlotBufferSlot::Missing => {
                    return Err(InternalError::persisted_row_encode_failed(format!(
                        "slot buffer writer did not emit slot {slot} for entity '{}'",
                        self.model.path()
                    )));
                }
            }
        }

        // Phase 2: flatten the slot table plus payload bytes into the canonical row image.
        encode_slot_payload_from_parts(slot_count, slot_table.as_slice(), payload_bytes.as_slice())
    }
}

impl SlotWriter for SlotBufferWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = slot_cell_mut(self.slots.as_mut_slice(), slot)?;
        let payload = required_slot_payload_bytes(self.model, "slot buffer writer", slot, payload)?;
        *entry = SlotBufferSlot::Set(payload.to_vec());

        Ok(())
    }
}

///
/// SlotBufferSlot
///
/// SlotBufferSlot tracks whether one canonical row encoder has emitted a
/// payload for every declared slot before flattening the row payload.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum SlotBufferSlot {
    Missing,
    Set(Vec<u8>),
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
                PatchWriterSlot::Set(payload) => SerializedFieldUpdate::new(field_slot, payload),
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
        let entry = slot_cell_mut(self.slots.as_mut_slice(), slot)?;
        let payload =
            required_slot_payload_bytes(self.model, "serialized patch writer", slot, payload)?;
        *entry = PatchWriterSlot::Set(payload.to_vec());

        Ok(())
    }
}

///
/// PatchWriterSlot
///
/// PatchWriterSlot
///
/// PatchWriterSlot tracks whether one dense slot-image writer has emitted a
/// payload or failed to visit the slot at all.
/// That lets the typed save/update bridge reject incomplete writers instead of
/// silently leaving stale bytes in the baseline row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum PatchWriterSlot {
    Missing,
    Set(Vec<u8>),
}

///
/// StructuralSlotReader
///
/// StructuralSlotReader adapts the current persisted-row bytes into the
/// canonical slot-reader seam.
/// It validates row shape and fully decodes every declared field before any
/// consumer can observe the row, then keeps those decoded values cached so
/// later index/predicate reads do not re-run field decoders.
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
        let mut reader = Self {
            model,
            field_bytes,
            cached_values,
        };

        // Phase 1: force every declared slot through the field decode contract
        // once so malformed persisted bytes cannot stay latent behind later
        // hot-path reads.
        reader.decode_all_declared_slots()?;

        Ok(reader)
    }

    /// Validate the decoded primary-key slot against the authoritative row key.
    pub(in crate::db) fn validate_storage_key(
        &self,
        data_key: &DataKey,
    ) -> Result<(), InternalError> {
        self.validate_storage_key_value(data_key.storage_key())
    }

    // Validate the decoded primary-key slot against one authoritative storage
    // key without rebuilding a full `DataKey` wrapper at the call site.
    pub(in crate::db) fn validate_storage_key_value(
        &self,
        expected_key: StorageKey,
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
            None => Some(
                decode_storage_key_field_bytes(
                    self.required_field_bytes(primary_key_slot, field.name())?,
                    field.kind,
                )
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_storage_encodable(
                        expected_key,
                        err,
                    )
                })?,
            ),
        };
        let Some(decoded_key) = decoded_key else {
            return Err(InternalError::persisted_row_primary_key_slot_missing(
                expected_key,
            ));
        };

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

    // Decode every declared slot exactly once at the structural row boundary so
    // later consumers inherit one globally enforced canonical-row contract.
    fn decode_all_declared_slots(&mut self) -> Result<(), InternalError> {
        for slot in 0..self.model.fields().len() {
            self.decode_slot_into_cache(slot)?;
        }

        Ok(())
    }

    // Decode one declared slot directly into the owned cache without cloning
    // the decoded value back out through `get_value` when the caller only
    // needs eager validation of canonical row shape.
    fn decode_slot_into_cache(&mut self, slot: usize) -> Result<(), InternalError> {
        if matches!(
            self.cached_values.get(slot),
            Some(CachedSlotValue::Decoded(_))
        ) {
            return Ok(());
        }

        let field = self.field_model(slot)?;
        let value =
            decode_slot_value_for_field(field, self.required_field_bytes(slot, field.name())?)?;
        self.cached_values[slot] = CachedSlotValue::Decoded(value);

        Ok(())
    }

    // Consume the structural slot reader into one slot-indexed decoded-value
    // vector once the canonical row boundary has already forced every slot
    // through decode. This lets hot row-decode callers reuse that validated
    // cache instead of decoding the same declared fields a second time.
    pub(in crate::db) fn into_decoded_values(self) -> Result<Vec<Option<Value>>, InternalError> {
        let mut values = Vec::with_capacity(self.cached_values.len());

        for (slot, cached) in self.cached_values.into_iter().enumerate() {
            match cached {
                CachedSlotValue::Decoded(value) => values.push(Some(value)),
                CachedSlotValue::Pending => {
                    return Err(InternalError::persisted_row_decode_failed(format!(
                        "structural slot cache was not fully decoded before consumption: slot={slot}",
                    )));
                }
            }
        }

        Ok(values)
    }

    // Borrow one already-decoded slot value from the eager structural cache so
    // callers that only need the canonical field value do not re-enter raw
    // field-byte decoding after `from_raw_row` has already validated the row.
    fn required_cached_value(&self, slot: usize) -> Result<&Value, InternalError> {
        let cached = self.cached_values.get(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_cache_lookup_out_of_bounds(self.model.path(), slot)
        })?;

        match cached {
            CachedSlotValue::Decoded(value) => Ok(value),
            CachedSlotValue::Pending => Err(InternalError::persisted_row_decode_failed(format!(
                "structural slot cache missing decoded value after eager decode: slot={slot}",
            ))),
        }
    }

    // Borrow one declared slot payload, treating absence as a persisted-row
    // invariant violation instead of a normal structural branch.
    pub(in crate::db) fn required_field_bytes(
        &self,
        slot: usize,
        field_name: &str,
    ) -> Result<&[u8], InternalError> {
        self.field_bytes
            .field(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
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

// Borrow one scalar-slot view directly from one already-decoded runtime value.
fn scalar_slot_value_ref_from_cached_value(
    value: &Value,
) -> Result<ScalarSlotValueRef<'_>, InternalError> {
    let scalar = match value {
        Value::Null => return Ok(ScalarSlotValueRef::Null),
        Value::Blob(value) => ScalarValueRef::Blob(value.as_slice()),
        Value::Bool(value) => ScalarValueRef::Bool(*value),
        Value::Date(value) => ScalarValueRef::Date(*value),
        Value::Duration(value) => ScalarValueRef::Duration(*value),
        Value::Float32(value) => ScalarValueRef::Float32(*value),
        Value::Float64(value) => ScalarValueRef::Float64(*value),
        Value::Int(value) => ScalarValueRef::Int(*value),
        Value::Principal(value) => ScalarValueRef::Principal(*value),
        Value::Subaccount(value) => ScalarValueRef::Subaccount(*value),
        Value::Text(value) => ScalarValueRef::Text(value.as_str()),
        Value::Timestamp(value) => ScalarValueRef::Timestamp(*value),
        Value::Uint(value) => ScalarValueRef::Uint(*value),
        Value::Ulid(value) => ScalarValueRef::Ulid(*value),
        Value::Unit => ScalarValueRef::Unit,
        _ => {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "cached structural scalar slot cannot borrow non-scalar value variant: {value:?}",
            )));
        }
    };

    Ok(ScalarSlotValueRef::Value(scalar))
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

        match field.leaf_codec() {
            LeafCodec::Scalar(_codec) => {
                scalar_slot_value_ref_from_cached_value(self.required_cached_value(slot)?).map(Some)
            }
            LeafCodec::CborFallback => Ok(None),
        }
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        self.decode_slot_into_cache(slot)?;
        Ok(Some(self.required_cached_value(slot)?.clone()))
    }
}

impl CanonicalSlotReader for StructuralSlotReader<'_> {
    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        Ok(self.required_cached_value(slot)?.clone())
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Borrowed(self.required_cached_value(slot)?))
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
    Decoded(Value),
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        FieldSlot, ScalarSlotValueRef, ScalarValueRef, SerializedFieldUpdate,
        SerializedPatchWriter, SerializedUpdatePatch, SlotBufferWriter, SlotReader, SlotWriter,
        UpdatePatch, apply_serialized_update_patch_to_raw_row, apply_update_patch_to_raw_row,
        decode_persisted_custom_many_slot_payload, decode_persisted_custom_slot_payload,
        decode_persisted_non_null_slot_payload, decode_persisted_option_slot_payload,
        decode_persisted_slot_payload, decode_slot_value_by_contract, decode_slot_value_from_bytes,
        encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
        encode_scalar_slot_value, encode_slot_payload_from_parts, encode_slot_value_from_value,
        serialize_entity_slots_as_update_patch, serialize_update_patch_fields,
    };
    use crate::{
        db::{
            codec::serialize_row_payload,
            data::{RawRow, StructuralSlotReader, decode_structural_value_storage_bytes},
        },
        error::InternalError,
        model::{
            EntityModel,
            field::{EnumVariantModel, FieldKind, FieldModel, FieldStorageDecode},
        },
        serialize::serialize,
        testing::SIMPLE_ENTITY_TAG,
        traits::{EntitySchema, FieldValue},
        types::{
            Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
            Principal, Subaccount, Timestamp, Ulid,
        },
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

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct PersistedRowProfileValue {
        bio: String,
    }

    impl FieldValue for PersistedRowProfileValue {
        fn kind() -> crate::traits::FieldValueKind {
            crate::traits::FieldValueKind::Structured { queryable: false }
        }

        fn to_value(&self) -> Value {
            Value::from_map(vec![(
                Value::Text("bio".to_string()),
                Value::Text(self.bio.clone()),
            )])
            .expect("profile test value should encode as canonical map")
        }

        fn from_value(value: &Value) -> Option<Self> {
            let Value::Map(entries) = value else {
                return None;
            };
            let normalized = Value::normalize_map_entries(entries.clone()).ok()?;
            let bio = normalized
                .iter()
                .find_map(|(entry_key, entry_value)| match entry_key {
                    Value::Text(entry_key) if entry_key == "bio" => match entry_value {
                        Value::Text(bio) => Some(bio.clone()),
                        _ => None,
                    },
                    _ => None,
                })?;

            if normalized.len() != 1 {
                return None;
            }

            Some(Self { bio })
        }
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
    static REQUIRED_STRUCTURED_FIELD_MODELS: [FieldModel; 1] = [FieldModel::new(
        "profile",
        FieldKind::Structured { queryable: false },
    )];
    static OPTIONAL_STRUCTURED_FIELD_MODELS: [FieldModel; 1] =
        [FieldModel::new_with_storage_decode_and_nullability(
            "profile",
            FieldKind::Structured { queryable: false },
            FieldStorageDecode::ByKind,
            true,
        )];
    static VALUE_STORAGE_STRUCTURED_FIELD_MODELS: [FieldModel; 1] =
        [FieldModel::new_with_storage_decode(
            "manifest",
            FieldKind::Structured { queryable: false },
            FieldStorageDecode::Value,
        )];
    static STRUCTURED_MAP_VALUE_KIND: FieldKind = FieldKind::Structured { queryable: false };
    static STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS: [FieldModel; 1] =
        [FieldModel::new_with_storage_decode(
            "projects",
            FieldKind::Map {
                key: &FieldKind::Principal,
                value: &STRUCTURED_MAP_VALUE_KIND,
            },
            FieldStorageDecode::Value,
        )];
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
    static REQUIRED_STRUCTURED_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowRequiredStructuredFieldCodecEntity",
        "persisted_row_required_structured_field_codec_entity",
        &REQUIRED_STRUCTURED_FIELD_MODELS[0],
        &REQUIRED_STRUCTURED_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static OPTIONAL_STRUCTURED_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowOptionalStructuredFieldCodecEntity",
        "persisted_row_optional_structured_field_codec_entity",
        &OPTIONAL_STRUCTURED_FIELD_MODELS[0],
        &OPTIONAL_STRUCTURED_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static VALUE_STORAGE_STRUCTURED_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowValueStorageStructuredFieldCodecEntity",
        "persisted_row_value_storage_structured_field_codec_entity",
        &VALUE_STORAGE_STRUCTURED_FIELD_MODELS[0],
        &VALUE_STORAGE_STRUCTURED_FIELD_MODELS,
        &INDEX_MODELS,
    );
    static STRUCTURED_MAP_VALUE_STORAGE_MODEL: EntityModel = EntityModel::new(
        "tests::PersistedRowStructuredMapValueStorageEntity",
        "persisted_row_structured_map_value_storage_entity",
        &STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS[0],
        &STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS,
        &INDEX_MODELS,
    );

    fn representative_value_storage_cases() -> Vec<Value> {
        let nested = Value::from_map(vec![
            (
                Value::Text("blob".to_string()),
                Value::Blob(vec![0x10, 0x20, 0x30]),
            ),
            (
                Value::Text("i128".to_string()),
                Value::Int128(Int128::from(-123i128)),
            ),
            (
                Value::Text("u128".to_string()),
                Value::Uint128(Nat128::from(456u128)),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Loaded", Some("tests::PersistedRowManifest"))
                        .with_payload(Value::Blob(vec![0xAA, 0xBB])),
                ),
            ),
        ])
        .expect("nested value storage case should normalize");

        vec![
            Value::Account(Account::dummy(7)),
            Value::Blob(vec![1u8, 2u8, 3u8]),
            Value::Bool(true),
            Value::Date(Date::new(2024, 1, 2)),
            Value::Decimal(Decimal::new(123, 2)),
            Value::Duration(Duration::from_secs(1)),
            Value::Enum(
                ValueEnum::new("Ready", Some("tests::PersistedRowState"))
                    .with_payload(nested.clone()),
            ),
            Value::Float32(Float32::try_new(1.25).expect("float32 sample should be finite")),
            Value::Float64(Float64::try_new(2.5).expect("float64 sample should be finite")),
            Value::Int(-7),
            Value::Int128(Int128::from(123i128)),
            Value::IntBig(Int::from(99i32)),
            Value::List(vec![
                Value::Blob(vec![0xCC, 0xDD]),
                Value::Text("nested".to_string()),
                nested.clone(),
            ]),
            nested,
            Value::Null,
            Value::Principal(Principal::dummy(9)),
            Value::Subaccount(Subaccount::new([7u8; 32])),
            Value::Text("example".to_string()),
            Value::Timestamp(Timestamp::from_secs(1)),
            Value::Uint(7),
            Value::Uint128(Nat128::from(9u128)),
            Value::UintBig(Nat::from(11u64)),
            Value::Ulid(Ulid::from_u128(42)),
            Value::Unit,
        ]
    }

    fn representative_structured_value_storage_cases() -> Vec<Value> {
        let nested_map = Value::from_map(vec![
            (
                Value::Text("account".to_string()),
                Value::Account(Account::dummy(7)),
            ),
            (
                Value::Text("blob".to_string()),
                Value::Blob(vec![1u8, 2u8, 3u8]),
            ),
            (Value::Text("bool".to_string()), Value::Bool(true)),
            (
                Value::Text("date".to_string()),
                Value::Date(Date::new(2024, 1, 2)),
            ),
            (
                Value::Text("decimal".to_string()),
                Value::Decimal(Decimal::new(123, 2)),
            ),
            (
                Value::Text("duration".to_string()),
                Value::Duration(Duration::from_secs(1)),
            ),
            (
                Value::Text("enum".to_string()),
                Value::Enum(
                    ValueEnum::new("Loaded", Some("tests::PersistedRowManifest"))
                        .with_payload(Value::Blob(vec![0xAA, 0xBB])),
                ),
            ),
            (
                Value::Text("f32".to_string()),
                Value::Float32(Float32::try_new(1.25).expect("float32 sample should be finite")),
            ),
            (
                Value::Text("f64".to_string()),
                Value::Float64(Float64::try_new(2.5).expect("float64 sample should be finite")),
            ),
            (Value::Text("i64".to_string()), Value::Int(-7)),
            (
                Value::Text("i128".to_string()),
                Value::Int128(Int128::from(123i128)),
            ),
            (
                Value::Text("ibig".to_string()),
                Value::IntBig(Int::from(99i32)),
            ),
            (Value::Text("null".to_string()), Value::Null),
            (
                Value::Text("principal".to_string()),
                Value::Principal(Principal::dummy(9)),
            ),
            (
                Value::Text("subaccount".to_string()),
                Value::Subaccount(Subaccount::new([7u8; 32])),
            ),
            (
                Value::Text("text".to_string()),
                Value::Text("example".to_string()),
            ),
            (
                Value::Text("timestamp".to_string()),
                Value::Timestamp(Timestamp::from_secs(1)),
            ),
            (Value::Text("u64".to_string()), Value::Uint(7)),
            (
                Value::Text("u128".to_string()),
                Value::Uint128(Nat128::from(9u128)),
            ),
            (
                Value::Text("ubig".to_string()),
                Value::UintBig(Nat::from(11u64)),
            ),
            (
                Value::Text("ulid".to_string()),
                Value::Ulid(Ulid::from_u128(42)),
            ),
            (Value::Text("unit".to_string()), Value::Unit),
        ])
        .expect("structured value-storage map should normalize");

        vec![
            nested_map.clone(),
            Value::List(vec![
                Value::Blob(vec![0xCC, 0xDD]),
                Value::Text("nested".to_string()),
                nested_map,
            ]),
        ]
    }

    fn encode_slot_payload_allowing_missing_for_tests(
        model: &'static EntityModel,
        slots: &[Option<&[u8]>],
    ) -> Result<Vec<u8>, InternalError> {
        if slots.len() != model.fields().len() {
            return Err(InternalError::persisted_row_encode_failed(format!(
                "noncanonical slot payload test helper expected {} slots for entity '{}', found {}",
                model.fields().len(),
                model.path(),
                slots.len()
            )));
        }
        let mut payload_bytes = Vec::new();
        let mut slot_table = Vec::with_capacity(slots.len());

        for slot_payload in slots {
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
                    payload_bytes.extend_from_slice(bytes);
                    slot_table.push((start, len));
                }
                None => slot_table.push((0_u32, 0_u32)),
            }
        }

        encode_slot_payload_from_parts(slots.len(), slot_table.as_slice(), payload_bytes.as_slice())
    }

    #[test]
    fn decode_slot_value_from_bytes_decodes_scalar_slots_through_one_owner() {
        let payload =
            encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
        let value =
            decode_slot_value_from_bytes(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

        assert_eq!(value, Value::Text("Ada".to_string()));
    }

    #[test]
    fn decode_slot_value_from_bytes_reports_scalar_prefix_bytes() {
        let err = decode_slot_value_from_bytes(&TEST_MODEL, 0, &[0x00, 1])
            .expect_err("invalid scalar slot prefix should fail closed");

        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF, found 0x00"),
            "unexpected error: {err:?}"
        );
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
    fn encode_slot_value_from_value_roundtrips_structured_value_storage_slots_for_all_cases() {
        for value in representative_structured_value_storage_cases() {
            let payload = encode_slot_value_from_value(&VALUE_STORAGE_STRUCTURED_MODEL, 0, &value)
                .unwrap_or_else(|err| {
                    panic!(
                        "structured value-storage slot should encode for value {value:?}: {err:?}"
                    )
                });
            let decoded = decode_slot_value_from_bytes(
                &VALUE_STORAGE_STRUCTURED_MODEL,
                0,
                payload.as_slice(),
            )
            .unwrap_or_else(|err| {
                panic!(
                    "structured value-storage slot should decode for value {value:?} with payload {payload:?}: {err:?}"
                )
            });

            assert_eq!(decoded, value);
        }
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
    fn encode_slot_value_from_value_accepts_value_storage_maps_with_structured_values() {
        let principal = Principal::dummy(7);
        let project = Value::from_map(vec![
            (Value::Text("pid".to_string()), Value::Principal(principal)),
            (
                Value::Text("status".to_string()),
                Value::Enum(ValueEnum::new(
                    "Saved",
                    Some("design::app::user::customise::project::ProjectStatus"),
                )),
            ),
        ])
        .expect("project value should normalize into a canonical map");
        let projects = Value::from_map(vec![(Value::Principal(principal), project)])
            .expect("outer map should normalize into a canonical map");

        let payload =
            encode_slot_value_from_value(&STRUCTURED_MAP_VALUE_STORAGE_MODEL, 0, &projects)
                .expect("encode structured map slot");
        let decoded = decode_slot_value_from_bytes(
            &STRUCTURED_MAP_VALUE_STORAGE_MODEL,
            0,
            payload.as_slice(),
        )
        .expect("decode structured map slot");

        assert_eq!(decoded, projects);
    }

    #[test]
    fn structured_value_storage_cases_decode_through_direct_value_storage_boundary() {
        for value in representative_value_storage_cases() {
            let payload = serialize(&value).unwrap_or_else(|err| {
                panic!(
                    "structured value-storage payload should serialize for value {value:?}: {err:?}"
                )
            });
            let decoded = decode_structural_value_storage_bytes(payload.as_slice()).unwrap_or_else(
                |err| {
                    panic!(
                        "structured value-storage payload should decode for value {value:?} with payload {payload:?}: {err:?}"
                    )
                },
            );

            assert_eq!(decoded, value);
        }
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
    fn custom_slot_payload_roundtrips_structured_field_value() {
        let profile = PersistedRowProfileValue {
            bio: "Ada".to_string(),
        };
        let payload = encode_persisted_custom_slot_payload(&profile, "profile")
            .expect("encode custom structured payload");
        let decoded = decode_persisted_custom_slot_payload::<PersistedRowProfileValue>(
            payload.as_slice(),
            "profile",
        )
        .expect("decode custom structured payload");

        assert_eq!(decoded, profile);
        assert_eq!(
            decode_persisted_slot_payload::<Value>(payload.as_slice(), "profile")
                .expect("decode raw value payload"),
            profile.to_value(),
        );
    }

    #[test]
    fn custom_many_slot_payload_roundtrips_structured_value_lists() {
        let profiles = vec![
            PersistedRowProfileValue {
                bio: "Ada".to_string(),
            },
            PersistedRowProfileValue {
                bio: "Grace".to_string(),
            },
        ];
        let payload = encode_persisted_custom_many_slot_payload(profiles.as_slice(), "profiles")
            .expect("encode custom structured list payload");
        let decoded = decode_persisted_custom_many_slot_payload::<PersistedRowProfileValue>(
            payload.as_slice(),
            "profiles",
        )
        .expect("decode custom structured list payload");

        assert_eq!(decoded, profiles);
    }

    #[test]
    fn decode_persisted_non_null_slot_payload_rejects_null_for_required_structured_fields() {
        let err =
            decode_persisted_non_null_slot_payload::<PersistedRowProfileValue>(&[0xF6], "profile")
                .expect_err("required structured payload must reject null");

        assert!(
            err.message
                .contains("unexpected null for non-nullable field"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn decode_persisted_option_slot_payload_treats_cbor_null_as_none() {
        let decoded =
            decode_persisted_option_slot_payload::<PersistedRowProfileValue>(&[0xF6], "profile")
                .expect("optional structured payload should decode");

        assert_eq!(decoded, None);
    }

    #[test]
    fn encode_slot_value_from_value_rejects_null_for_required_structured_slots() {
        let err = encode_slot_value_from_value(&REQUIRED_STRUCTURED_MODEL, 0, &Value::Null)
            .expect_err("required structured slot must reject null");

        assert!(
            err.message.contains("required field cannot store null"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn encode_slot_value_from_value_allows_null_for_optional_structured_slots() {
        let payload = encode_slot_value_from_value(&OPTIONAL_STRUCTURED_MODEL, 0, &Value::Null)
            .expect("optional structured slot should allow null");
        let decoded =
            decode_slot_value_from_bytes(&OPTIONAL_STRUCTURED_MODEL, 0, payload.as_slice())
                .expect("optional structured slot should decode");

        assert_eq!(decoded, Value::Null);
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
                serialized.entries()[0].payload(),
            )
            .expect("decode slot payload"),
            Value::Text("Grace".to_string())
        );
        assert_eq!(
            decode_slot_value_from_bytes(
                &TEST_MODEL,
                serialized.entries()[1].slot().index(),
                serialized.entries()[1].payload(),
            )
            .expect("decode slot payload"),
            Value::Text("payload".to_string())
        );
    }

    #[test]
    fn serialized_patch_writer_rejects_clear_slots() {
        let mut writer = SerializedPatchWriter::for_model(&TEST_MODEL);

        let err = writer
            .write_slot(0, None)
            .expect_err("0.65 patch staging must reject missing-slot clears");

        assert!(
            err.message
                .contains("serialized patch writer cannot clear slot 0"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message.contains(TEST_MODEL.path()),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn slot_buffer_writer_rejects_clear_slots() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);

        let err = writer
            .write_slot(0, None)
            .expect_err("canonical row staging must reject missing-slot clears");

        assert!(
            err.message
                .contains("slot buffer writer cannot clear slot 0"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message.contains(TEST_MODEL.path()),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn apply_update_patch_to_raw_row_uses_last_write_wins() {
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
    fn apply_update_patch_to_raw_row_rejects_noncanonical_missing_slot_baseline() {
        let empty_slots = vec![None::<&[u8]>; TEST_MODEL.fields().len()];
        let raw_row = RawRow::try_new(
            serialize_row_payload(
                encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, empty_slots.as_slice())
                    .expect("encode malformed slot payload"),
            )
            .expect("serialize row payload"),
        )
        .expect("build raw row");
        let patch = UpdatePatch::new().set(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            Value::Text("payload".to_string()),
        );

        let err = apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch)
            .expect_err("noncanonical rows with missing slots must fail closed");

        assert_eq!(err.message, "row decode: missing slot payload: slot=0");
    }

    #[test]
    fn apply_serialized_update_patch_to_raw_row_rejects_noncanonical_scalar_baseline() {
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload");
        let malformed_slots = [Some([0xF6].as_slice()), Some(payload.as_slice())];
        let raw_row = RawRow::try_new(
            serialize_row_payload(
                encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, &malformed_slots)
                    .expect("encode malformed slot payload"),
            )
            .expect("serialize row payload"),
        )
        .expect("build raw row");
        let patch = UpdatePatch::new().set(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            Value::Text("patched".to_string()),
        );
        let serialized =
            serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

        let err = apply_serialized_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &serialized)
            .expect_err("noncanonical scalar baseline must fail closed");

        assert!(
            err.message.contains("field 'name'"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn apply_serialized_update_patch_to_raw_row_rejects_noncanonical_scalar_patch_payload() {
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
        let serialized = SerializedUpdatePatch::new(vec![SerializedFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        )]);

        let err = apply_serialized_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &serialized)
            .expect_err("noncanonical serialized patch payload must fail closed");

        assert!(
            err.message.contains("field 'name'"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn structural_slot_reader_rejects_slot_count_mismatch() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode payload");
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        writer
            .write_slot(1, Some(payload.as_slice()))
            .expect("write payload slot");
        let mut payload = writer.finish().expect("finish slot payload");
        payload[..2].copy_from_slice(&1_u16.to_be_bytes());
        let raw_row =
            RawRow::try_new(serialize_row_payload(payload).expect("serialize row payload"))
                .expect("build raw row");

        let err = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
            .err()
            .expect("slot-count drift must fail closed");

        assert_eq!(
            err.message,
            "row decode: slot count mismatch: expected 2, found 1"
        );
    }

    #[test]
    fn structural_slot_reader_rejects_slot_span_exceeds_payload_length() {
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode payload");
        writer
            .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
            .expect("write scalar slot");
        writer
            .write_slot(1, Some(payload.as_slice()))
            .expect("write payload slot");
        let mut payload = writer.finish().expect("finish slot payload");

        // Corrupt the second slot span so the payload table points past the
        // available data section.
        payload[14..18].copy_from_slice(&u32::MAX.to_be_bytes());
        let raw_row =
            RawRow::try_new(serialize_row_payload(payload).expect("serialize row payload"))
                .expect("build raw row");

        let err = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
            .err()
            .expect("slot span drift must fail closed");

        assert_eq!(err.message, "row decode: slot span exceeds payload length");
    }

    #[test]
    fn apply_serialized_update_patch_to_raw_row_replays_preencoded_slots() {
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
        let direct =
            RawRow::from_serialized_update_patch(PersistedRowPatchBridgeEntity::MODEL, &serialized)
                .expect("direct row emission should succeed");

        let patched = raw_row
            .apply_serialized_update_patch(PersistedRowPatchBridgeEntity::MODEL, &serialized)
            .expect("apply serialized patch");
        let decoded = patched
            .try_decode::<PersistedRowPatchBridgeEntity>()
            .expect("decode patched entity");

        assert_eq!(
            direct, patched,
            "fresh row emission and replayed full-image patch must converge on identical bytes",
        );
        assert_eq!(old_decoded, old_entity);
        assert_eq!(decoded, new_entity);
    }

    #[test]
    fn canonical_row_from_raw_row_replays_canonical_full_image_bytes() {
        let entity = PersistedRowPatchBridgeEntity {
            id: crate::types::Ulid::from_u128(11),
            name: "Ada".to_string(),
        };
        let raw_row = RawRow::from_entity(&entity).expect("encode canonical row");
        let canonical =
            super::canonical_row_from_raw_row(PersistedRowPatchBridgeEntity::MODEL, &raw_row)
                .expect("canonical re-emission should succeed");

        assert_eq!(
            canonical.as_bytes(),
            raw_row.as_bytes(),
            "canonical raw-row rebuild must preserve already canonical row bytes",
        );
    }

    #[test]
    fn canonical_row_from_raw_row_rejects_noncanonical_scalar_payload() {
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload");
        let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
        writer
            .write_slot(0, Some(&[0xF6]))
            .expect("write malformed scalar slot");
        writer
            .write_slot(1, Some(payload.as_slice()))
            .expect("write value-storage slot");
        let raw_row = RawRow::try_new(
            serialize_row_payload(writer.finish().expect("finish slot payload"))
                .expect("serialize malformed row"),
        )
        .expect("build malformed raw row");

        let err = super::canonical_row_from_raw_row(&TEST_MODEL, &raw_row)
            .expect_err("canonical raw-row rebuild must reject malformed scalar payloads");

        assert!(
            err.message.contains("field 'name'"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn raw_row_from_serialized_update_patch_rejects_noncanonical_scalar_payload() {
        let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload");
        let serialized = SerializedUpdatePatch::new(vec![
            SerializedFieldUpdate::new(
                FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
                vec![0xF6],
            ),
            SerializedFieldUpdate::new(
                FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
                payload,
            ),
        ]);

        let err = RawRow::from_serialized_update_patch(&TEST_MODEL, &serialized)
            .expect_err("fresh row emission must reject noncanonical serialized patch payloads");

        assert!(
            err.message.contains("field 'name'"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn raw_row_from_serialized_update_patch_rejects_incomplete_slot_image() {
        let serialized = SerializedUpdatePatch::new(vec![SerializedFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            crate::serialize::serialize(&Value::Text("payload".to_string()))
                .expect("encode value-storage payload"),
        )]);

        let err = RawRow::from_serialized_update_patch(&TEST_MODEL, &serialized)
            .expect_err("fresh row emission must reject missing declared slots");

        assert!(
            err.message.contains("serialized patch did not emit slot 0"),
            "unexpected error: {err:?}"
        );
    }
}
