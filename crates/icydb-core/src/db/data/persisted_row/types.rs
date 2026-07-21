use crate::{
    db::data::persisted_row::codec::ScalarSlotValueRef,
    entity::{EntityKind, EntityValue},
    error::InternalError,
    model::field::LeafCodec,
    value::{InputValue, Value},
};

///
/// FieldSlot
///
///
/// FieldSlot is the structural stable slot reference used by accepted writes.
/// It intentionally carries only the model-local slot index so field-level
/// mutation stays structural instead of reintroducing typed entity helpers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldSlot {
    index: usize,
}

impl FieldSlot {
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
    #[cfg(test)]
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
}

/// Accepted insertion-policy request carried by one unresolved field intent.
///
/// Omission remains represented by the absence of an entry. Explicit SQL
/// `DEFAULT` requests use the exact variants below so the accepted resolver
/// never has to reconstruct request provenance from an empty value or flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(any(test, feature = "sql")),
    expect(
        dead_code,
        reason = "explicit DEFAULT request variants are constructed only by the SQL frontend"
    )
)]
pub(in crate::db) enum AcceptedInsertPolicyRequest {
    /// Field omitted while constructing an insert or replacement after-image.
    OmittedInsert,
    /// Explicit `DEFAULT` in an insert or replacement value position.
    ExplicitInsertDefault,
    /// Explicit `DEFAULT` in an update assignment.
    ExplicitUpdateDefault,
}

/// One field intent admitted to the accepted mutation resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(any(test, feature = "sql")),
    expect(
        dead_code,
        reason = "unresolved DEFAULT intent is constructed only by the SQL frontend"
    )
)]
pub(in crate::db) enum AcceptedMutationFieldWriteIntent {
    /// Exact caller-authored input, including explicit `NULL`.
    Authored(InputValue),
    /// Database-owned primary-key value selected by replacement identity.
    ///
    /// This is not caller authorship: the keyed replacement boundary has
    /// already selected the row identity, and the accepted resolver must carry
    /// that identity through without rerunning insert generation.
    PreservedReplacementIdentity(InputValue),
    /// Resolve through the accepted policy appropriate to this exact request.
    Resolve(AcceptedInsertPolicyRequest),
}

/// One stable-slot field intent admitted to accepted mutation resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedMutationFieldUpdate {
    slot: FieldSlot,
    intent: AcceptedMutationFieldWriteIntent,
}

impl AcceptedMutationFieldUpdate {
    /// Build one unresolved accepted mutation field update.
    #[must_use]
    const fn new(slot: FieldSlot, intent: AcceptedMutationFieldWriteIntent) -> Self {
        Self { slot, intent }
    }

    /// Return the stable target slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }

    /// Borrow the exact unresolved write intent.
    #[must_use]
    pub(in crate::db) const fn intent(&self) -> &AcceptedMutationFieldWriteIntent {
        &self.intent
    }
}

/// Ordered unresolved field intents consumed by accepted mutation resolution.
///
/// This is private to the database implementation. Public structural callers
/// can author values only through [`AuthoredStructuralPatch`]; SQL lowering may
/// additionally construct exact contextual `DEFAULT` requests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedMutationIntentPatch {
    entries: Vec<AcceptedMutationFieldUpdate>,
}

impl AcceptedMutationIntentPatch {
    /// Convert one authored-only public patch without changing authorship.
    #[must_use]
    pub(in crate::db) fn from_authored(patch: AuthoredStructuralPatch) -> Self {
        let entries = patch
            .entries
            .into_iter()
            .map(|entry| {
                AcceptedMutationFieldUpdate::new(
                    entry.slot,
                    AcceptedMutationFieldWriteIntent::Authored(entry.value),
                )
            })
            .collect();

        Self { entries }
    }

    /// Build one empty accepted mutation intent patch.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append one authored field input.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) fn set_authored(mut self, slot: FieldSlot, value: InputValue) -> Self {
        self.entries.push(AcceptedMutationFieldUpdate::new(
            slot,
            AcceptedMutationFieldWriteIntent::Authored(value),
        ));
        self
    }

    /// Append one protected database-owned replacement identity component.
    #[must_use]
    pub(in crate::db) fn set_preserved_replacement_identity(
        mut self,
        slot: FieldSlot,
        value: InputValue,
    ) -> Self {
        self.entries.push(AcceptedMutationFieldUpdate::new(
            slot,
            AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(value),
        ));
        self
    }

    /// Append one explicit insert `DEFAULT` request.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) fn set_explicit_insert_default(mut self, slot: FieldSlot) -> Self {
        self.entries.push(AcceptedMutationFieldUpdate::new(
            slot,
            AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::ExplicitInsertDefault,
            ),
        ));
        self
    }

    /// Append one explicit update `DEFAULT` request.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) fn set_explicit_update_default(mut self, slot: FieldSlot) -> Self {
        self.entries.push(AcceptedMutationFieldUpdate::new(
            slot,
            AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
            ),
        ));
        self
    }

    /// Borrow the ordered unresolved field intents.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[AcceptedMutationFieldUpdate] {
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
    fn runtime_enum_context(&self) -> Option<&dyn crate::value::RuntimeEnumContext> {
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
/// PersistedRow
///
/// PersistedRow is the derive-owned bridge between typed entities and
/// slot-addressable persisted rows.
/// It combines the model/placement contract with a concrete entity value.
/// It owns entity-specific materialization/default semantics while runtime
/// paths stay structural at the row boundary.
///

pub trait PersistedRow: EntityKind + EntityValue {
    /// Materialize one typed entity from one slot reader.
    fn materialize_from_slots(slots: &mut dyn SlotReader) -> Result<Self, InternalError>;
}
