//! Module: db::schema::layout
//! Responsibility: schema-owned row-layout identity contracts.
//! Does not own: executor row decoding or persisted schema reconciliation.
//! Boundary: maps durable schema field identity to physical row slots.

use crate::db::schema::FieldId;

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
        let slot = u16::try_from(index).expect("generated field slot should fit in persisted u16");

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
/// The executor has its own decode layout today; this type represents the
/// persisted schema layout authority that future reconciliation will feed into
/// runtime decode planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaRowLayout {
    version: SchemaVersion,
    field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
}

impl SchemaRowLayout {
    /// Build one schema row layout from already-validated field-to-slot pairs.
    #[must_use]
    pub(in crate::db) const fn new(
        version: SchemaVersion,
        field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
    ) -> Self {
        Self {
            version,
            field_to_slot,
        }
    }

    /// Return the schema version associated with this layout.
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Return the durable field-ID to physical-slot mapping.
    #[must_use]
    pub(in crate::db) const fn field_to_slot(&self) -> &[(FieldId, SchemaFieldSlot)] {
        self.field_to_slot.as_slice()
    }

    /// Resolve one durable field identity into its accepted physical row slot.
    #[must_use]
    pub(in crate::db) fn slot_for_field(&self, field_id: FieldId) -> Option<SchemaFieldSlot> {
        self.field_to_slot
            .iter()
            .find_map(|(id, slot)| (*id == field_id).then_some(*slot))
    }
}
