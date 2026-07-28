use crate::{
    db::data::persisted_row::codec::ScalarSlotValueRef,
    db::schema::LeafCodec,
    error::InternalError,
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

/// Accepted insertion-policy request carried by one unresolved field intent.
///
/// Omission remains represented by the absence of an entry. Explicit SQL
/// `DEFAULT` requests use the exact variants below so the accepted resolver
/// never has to reconstruct request provenance from an empty value or flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
/// This is private to the database implementation. Dynamic structural callers
/// and SQL lowering construct exact contextual write intents at the session
/// boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedMutationIntentPatch {
    entries: Vec<AcceptedMutationFieldUpdate>,
}

impl AcceptedMutationIntentPatch {
    /// Build one empty accepted mutation intent patch.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append one authored field input.
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

pub(crate) trait SlotReader {
    /// Borrow the raw persisted payload for one slot when present.
    fn get_bytes(&self, slot: usize) -> Option<&[u8]>;

    /// Decode one slot as a scalar leaf when the field contract declares a scalar codec.
    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError>;

    /// Decode one slot value on demand through the reader's accepted contract.
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
    /// Borrow the accepted field name for one stable slot.
    fn field_name(&self, slot: usize) -> Result<&str, InternalError>;

    /// Return the declared leaf codec for one slot.
    fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError>;

    /// Borrow one declared slot payload, erroring when the persisted row is not canonical.
    #[cfg(any(test, feature = "query"))]
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
