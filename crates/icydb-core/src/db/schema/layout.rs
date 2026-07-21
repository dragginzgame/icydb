//! Module: db::schema::layout
//! Responsibility: schema-owned row-layout identity contracts.
//! Does not own: executor row decoding or persisted schema reconciliation.
//! Boundary: maps durable schema field identity to physical row slots.

use crate::db::schema::FieldId;
use std::num::NonZeroU32;

///
/// SchemaVersion
///
/// Monotonic version for one entity's live schema snapshot.
/// It is intentionally schema-owned rather than executor-owned so layout
/// changes can be reconciled before any row decode path consumes them.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct SchemaVersion(u32);

impl SchemaVersion {
    /// Build one schema version from trusted persisted metadata.
    #[must_use]
    pub(in crate::db) const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Return the first live schema version for a newly initialized entity.
    #[must_use]
    pub(in crate::db) const fn initial() -> Self {
        Self(1)
    }

    /// Return the raw persisted version value.
    #[must_use]
    pub(in crate::db) const fn get(self) -> u32 {
        self.0
    }
}

///
/// RowLayoutVersion
///
/// Non-zero entity-local identity for one exact physical row-slot shape.
/// Unlike `SchemaVersion`, this identity advances only when the accepted
/// physical layout changes and is persisted in every canonical row envelope.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct RowLayoutVersion(NonZeroU32);

impl RowLayoutVersion {
    /// First admitted physical layout identity for a newly initialized entity.
    pub(in crate::db) const INITIAL: Self = Self(NonZeroU32::MIN);

    /// Admit one non-zero persisted layout identity.
    #[must_use]
    pub(in crate::db) const fn new(raw: u32) -> Option<Self> {
        match NonZeroU32::new(raw) {
            Some(raw) => Some(Self(raw)),
            None => None,
        }
    }

    /// Return the persisted integer identity.
    #[must_use]
    pub(in crate::db) const fn get(self) -> u32 {
        self.0.get()
    }

    /// Allocate the next physical layout identity without wrapping or reuse.
    #[must_use]
    pub(in crate::db) const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(raw) => Self::new(raw),
            None => None,
        }
    }
}

///
/// SchemaFieldSlot
///
/// Physical row slot assigned to one live schema field.
/// This wrapper keeps slot order separate from durable `FieldId` identity and
/// prevents later reconciliation code from passing bare `usize` values around.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct SchemaFieldSlot(u16);

impl SchemaFieldSlot {
    /// Build one schema field slot from trusted persisted metadata.
    #[must_use]
    pub(in crate::db) const fn new(raw: u16) -> Self {
        Self(raw)
    }

    /// Build one schema field slot from a generated field index.
    #[must_use]
    pub(in crate::db) fn from_generated_index(index: usize) -> Self {
        let slot = u16::try_from(index).expect("schema layout invariant");

        Self(slot)
    }

    /// Return the raw slot value used by the persisted row layout.
    #[must_use]
    pub(in crate::db) const fn get(self) -> u16 {
        self.0
    }
}

///
/// SchemaRowLayout
///
/// Schema-owned mapping from durable field IDs to physical row slots.
/// This is the persisted slot authority projected into the accepted runtime
/// decode and write contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaRowLayout {
    current_version: RowLayoutVersion,
    history_floor: RowLayoutVersion,
    field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
}

impl SchemaRowLayout {
    /// Build one schema row layout with an explicit admitted history window.
    #[must_use]
    pub(in crate::db) const fn new(
        current_version: RowLayoutVersion,
        history_floor: RowLayoutVersion,
        field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
    ) -> Self {
        Self {
            current_version,
            history_floor,
            field_to_slot,
        }
    }

    /// Build one layout whose current shape is its only admitted history.
    #[must_use]
    pub(in crate::db) const fn single_version(
        version: RowLayoutVersion,
        field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
    ) -> Self {
        Self::new(version, version, field_to_slot)
    }

    /// Build the sole initial physical layout for a new accepted entity.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn initial(field_to_slot: Vec<(FieldId, SchemaFieldSlot)>) -> Self {
        Self::single_version(RowLayoutVersion::INITIAL, field_to_slot)
    }

    /// Return the physical layout stamped by every current canonical writer.
    #[must_use]
    pub(in crate::db) const fn current_version(&self) -> RowLayoutVersion {
        self.current_version
    }

    /// Return the oldest physical layout version admitted for row decoding.
    #[must_use]
    pub(in crate::db) const fn history_floor(&self) -> RowLayoutVersion {
        self.history_floor
    }

    /// Return the durable field-ID to physical-slot mapping.
    #[must_use]
    pub(in crate::db) const fn field_to_slot(&self) -> &[(FieldId, SchemaFieldSlot)] {
        self.field_to_slot.as_slice()
    }

    /// Return the next dense physical slot index for additive field DDL.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn next_unallocated_slot(&self) -> SchemaFieldSlot {
        SchemaFieldSlot::from_generated_index(self.field_to_slot.len())
    }

    /// Return the current dense physical slot count.
    #[must_use]
    pub(in crate::db) const fn allocated_slot_count(&self) -> usize {
        self.field_to_slot.len()
    }

    /// Resolve one durable field identity into its accepted physical row slot.
    #[must_use]
    pub(in crate::db) fn slot_for_field(&self, field_id: FieldId) -> Option<SchemaFieldSlot> {
        self.field_to_slot
            .iter()
            .find_map(|(id, slot)| (*id == field_id).then_some(*slot))
    }
}
