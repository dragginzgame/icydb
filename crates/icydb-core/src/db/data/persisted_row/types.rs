#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::{
    db::data::persisted_row::codec::{ScalarSlotValueRef, encode_scalar_slot_value},
    error::InternalError,
    model::field::LeafCodec,
    traits::EntityKind,
    value::{InputValue, Value},
};

///
/// FieldSlot
///
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
    /// Build one stable field slot from an already validated index.
    #[cfg(test)]
    pub(in crate::db) fn from_index(
        model: &'static EntityModel,
        index: usize,
    ) -> Result<Self, InternalError> {
        model.fields().get(index).ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(model.path(), index)
        })?;

        Ok(Self { index })
    }

    /// Build one stable field slot from a non-generated authority.
    ///
    /// Accepted-schema write paths use this after the session has validated the
    /// slot against the current accepted row layout.
    #[must_use]
    pub(in crate::db) const fn from_validated_index(index: usize) -> Self {
        Self { index }
    }

    /// Return the accepted stable slot index.
    #[must_use]
    pub(in crate::db) const fn index(self) -> usize {
        self.index
    }
}

///
/// StructuralFieldUpdate
///
/// AuthoredStructuralFieldUpdate carries one ordered structural field assignment before
/// persisted-row slot serialization.
/// `AuthoredStructuralPatch` applies these entries in order and last write wins for the
/// same slot, but row-existence semantics remain owned by the mutation mode.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AuthoredStructuralFieldUpdate {
    slot: FieldSlot,
    value: InputValue,
}

impl AuthoredStructuralFieldUpdate {
    /// Build one field-level structural update.
    #[must_use]
    pub(in crate::db) const fn new(slot: FieldSlot, value: InputValue) -> Self {
        Self { slot, value }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Return the unresolved authored value payload for this update.
    #[must_use]
    pub(in crate::db) const fn value(&self) -> &InputValue {
        &self.value
    }
}

///
/// AuthoredStructuralPatch
///
///
/// AuthoredStructuralPatch is the ordered unresolved field patch applied by
/// structural write lanes before accepted-schema admission and slot serialization.
/// It carries caller `InputValue` payloads only; insert, update, and replace
/// semantics remain owned by `MutationMode`, not by the patch container.
/// Field-name resolution is owned by session/schema boundaries; this container
/// only records already validated slot assignments.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthoredStructuralPatch {
    entries: Vec<AuthoredStructuralFieldUpdate>,
}

impl AuthoredStructuralPatch {
    /// Build one empty patch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append one structural field update in declaration order.
    #[must_use]
    pub(in crate::db) fn set(mut self, slot: FieldSlot, value: impl Into<InputValue>) -> Self {
        self.entries
            .push(AuthoredStructuralFieldUpdate::new(slot, value.into()));
        self
    }

    /// Borrow the ordered field updates carried by this patch.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[AuthoredStructuralFieldUpdate] {
        self.entries.as_slice()
    }

    /// Return whether this patch carries no field updates.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

///
/// SerializedStructuralFieldUpdate
///
/// SerializedStructuralFieldUpdate carries one ordered field-level mutation after the
/// owning persisted-row field codec has already lowered the runtime `Value`
/// into canonical slot payload bytes.
/// This lets later patch-application stages consume one mechanical slot-patch
/// artifact instead of rebuilding per-field encode dispatch.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::data::persisted_row) struct SerializedStructuralFieldUpdate {
    slot: FieldSlot,
    payload: Vec<u8>,
}

impl SerializedStructuralFieldUpdate {
    /// Build one serialized structural field update.
    #[must_use]
    pub(in crate::db::data::persisted_row) const fn new(slot: FieldSlot, payload: Vec<u8>) -> Self {
        Self { slot, payload }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db::data::persisted_row) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Borrow the canonical slot payload bytes for this update when present.
    #[must_use]
    pub(in crate::db::data::persisted_row) const fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

///
/// SerializedStructuralPatch
///
/// SerializedStructuralPatch is the canonical serialized form of `AuthoredStructuralPatch`
/// over persisted-row slot payload bytes.
/// This is the structural patch artifact later write-path stages can stage or
/// replay without re-entering field-contract encode logic.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SerializedStructuralPatch {
    entries: Vec<SerializedStructuralFieldUpdate>,
}

impl SerializedStructuralPatch {
    /// Build one serialized patch from already encoded slot payloads.
    #[must_use]
    pub(in crate::db::data::persisted_row) const fn new(
        entries: Vec<SerializedStructuralFieldUpdate>,
    ) -> Self {
        Self { entries }
    }

    /// Borrow the ordered serialized field updates carried by this patch.
    #[must_use]
    pub(in crate::db::data::persisted_row) const fn entries(
        &self,
    ) -> &[SerializedStructuralFieldUpdate] {
        self.entries.as_slice()
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
    /// Return whether the given slot is present in the persisted row.
    fn has(&self, slot: usize) -> bool;

    /// Borrow the raw persisted payload for one slot when present.
    fn get_bytes(&self, slot: usize) -> Option<&[u8]>;

    /// Decode one slot as a scalar leaf when the field contract declares a scalar codec.
    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError>;

    /// Decode one slot value on demand through the reader's accepted contract.
    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError>;

    /// Borrow the accepted catalog context used to decode canonical enum IDs.
    #[doc(hidden)]
    fn runtime_enum_context(&self) -> Option<&dyn crate::traits::RuntimeEnumContext> {
        None
    }
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
    /// Borrow the accepted field name for one stable slot.
    fn field_name(&self, slot: usize) -> Result<&str, InternalError>;

    /// Return the declared leaf codec for one slot.
    fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError>;

    /// Borrow one declared slot payload, erroring when the persisted row is not canonical.
    fn required_bytes(&self, slot: usize) -> Result<&[u8], InternalError> {
        let field_name = self.field_name(slot)?;

        self.get_bytes(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
    }

    /// Read one scalar slot through the structural fast path without allowing
    /// declared-slot absence.
    fn required_scalar(&self, slot: usize) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        let field_name = self.field_name(slot)?;
        debug_assert!(matches!(self.field_leaf_codec(slot)?, LeafCodec::Scalar(_)));

        self.get_scalar(slot)?
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
    }

    /// Read one value-storage scalar when a concrete reader can expose it without full decode.
    fn required_value_storage_scalar(
        &self,
        _slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        Ok(None)
    }

    /// Decode one declared slot through the owning field contract without
    /// allowing absent payloads.
    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError>;

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
}
