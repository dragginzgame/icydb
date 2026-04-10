use crate::{
    db::data::persisted_row::{
        codec::{ScalarSlotValueRef, encode_scalar_slot_value},
        contract::{decode_slot_value_from_bytes, field_model_for_slot},
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    traits::EntityKind,
    value::Value,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldSlot {
    index: usize,
}

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
/// FieldUpdate carries one ordered field-level mutation over the structural
/// persisted-row boundary.
/// `UpdatePatch` applies these entries in order and last write wins for the
/// same slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldUpdate {
    slot: FieldSlot,
    value: Value,
}

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
/// SerializedFieldUpdate carries one ordered field-level mutation after the
/// owning persisted-row field codec has already lowered the runtime `Value`
/// into canonical slot payload bytes.
/// This lets later patch-application stages consume one mechanical slot-patch
/// artifact instead of rebuilding per-field encode dispatch.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SerializedFieldUpdate {
    slot: FieldSlot,
    payload: Vec<u8>,
}

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
/// SerializedUpdatePatch is the canonical serialized form of `UpdatePatch`
/// over persisted-row slot payload bytes.
/// This is the structural patch artifact later write-path stages can stage or
/// replay without re-entering field-contract encode logic.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SerializedUpdatePatch {
    entries: Vec<SerializedFieldUpdate>,
}

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
        debug_assert!(matches!(
            field.leaf_codec(),
            crate::model::field::LeafCodec::Scalar(_)
        ));

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
    fn required_value_by_contract_cow(
        &self,
        slot: usize,
    ) -> Result<std::borrow::Cow<'_, Value>, InternalError> {
        Ok(std::borrow::Cow::Owned(
            self.required_value_by_contract(slot)?,
        ))
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
