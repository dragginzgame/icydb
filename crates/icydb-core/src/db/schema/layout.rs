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
    retired_field_slots: Vec<(FieldId, SchemaFieldSlot)>,
}

impl SchemaRowLayout {
    /// Build one schema row layout from already-validated field-to-slot pairs.
    #[must_use]
    pub(in crate::db) const fn new(
        version: SchemaVersion,
        field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
    ) -> Self {
        Self::new_with_retired_slots(version, field_to_slot, Vec::new())
    }

    /// Build one schema row layout with active and retired field-slot pairs.
    ///
    /// Retired slots are not visible fields, but they remain durable allocation
    /// facts so later field DDL cannot reuse physical slots still present in
    /// older persisted rows.
    #[must_use]
    pub(in crate::db) const fn new_with_retired_slots(
        version: SchemaVersion,
        field_to_slot: Vec<(FieldId, SchemaFieldSlot)>,
        retired_field_slots: Vec<(FieldId, SchemaFieldSlot)>,
    ) -> Self {
        Self {
            version,
            field_to_slot,
            retired_field_slots,
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

    /// Return retired durable field-ID to physical-slot mappings.
    #[must_use]
    pub(in crate::db) const fn retired_field_slots(&self) -> &[(FieldId, SchemaFieldSlot)] {
        self.retired_field_slots.as_slice()
    }

    /// Return the next never-used physical slot index for additive field DDL.
    #[must_use]
    pub(in crate::db) fn next_unallocated_slot(&self) -> SchemaFieldSlot {
        let next = self
            .field_to_slot()
            .iter()
            .chain(self.retired_field_slots())
            .map(|(_, slot)| slot.get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .expect("accepted row slots should not be exhausted");

        SchemaFieldSlot::new(next)
    }

    /// Return the maximum physical slot count older rows may still carry.
    #[must_use]
    pub(in crate::db) fn allocated_slot_count(&self) -> usize {
        self.field_to_slot()
            .iter()
            .chain(self.retired_field_slots())
            .map(|(_, slot)| usize::from(slot.get()).saturating_add(1))
            .max()
            .unwrap_or(0)
    }

    /// Resolve one durable field identity into its accepted physical row slot.
    #[must_use]
    pub(in crate::db) fn slot_for_field(&self, field_id: FieldId) -> Option<SchemaFieldSlot> {
        self.field_to_slot
            .iter()
            .find_map(|(id, slot)| (*id == field_id).then_some(*slot))
    }

    /// Clone this layout after retiring one active field slot.
    #[must_use]
    pub(in crate::db) fn clone_retiring_field(&self, field_id: FieldId) -> Option<Self> {
        let mut field_to_slot = Vec::with_capacity(self.field_to_slot.len().saturating_sub(1));
        let mut retired_field_slots = self.retired_field_slots.clone();
        let mut retired = None;

        for (id, slot) in &self.field_to_slot {
            if *id == field_id {
                retired = Some((*id, *slot));
            } else {
                field_to_slot.push((*id, *slot));
            }
        }

        retired_field_slots.push(retired?);

        Some(Self::new_with_retired_slots(
            self.version,
            field_to_slot,
            retired_field_slots,
        ))
    }
}
