//! Module: db::schema::identity
//! Responsibility: durable schema identity primitives.
//! Does not own: generated model validation or persisted schema reconciliation.
//! Boundary: small copyable identifiers shared by schema proposal and live-schema code.

///
/// FieldId
///
/// Durable identity for one logical schema field.
/// This ID is distinct from generated Rust field order and from executor slot
/// indexes so future schema reconciliation can preserve identity across safe
/// reorders and renames.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct FieldId(u32);

impl FieldId {
    /// Build one field ID from a trusted persisted value.
    #[must_use]
    pub(in crate::db) const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Return the raw persisted field identity.
    #[must_use]
    pub(in crate::db) const fn get(self) -> u32 {
        self.0
    }

    /// Assign the initial schema ID for a generated field slot.
    ///
    /// The first generated snapshot has no prior durable identity source, so it
    /// derives IDs deterministically from generated slot order. Later schema
    /// reconciliation must preserve stored IDs instead of recalculating them.
    #[must_use]
    pub(in crate::db) fn from_initial_slot(slot: usize) -> Self {
        let next = u32::try_from(slot)
            .expect("generated field slot should fit in u32")
            .checked_add(1)
            .expect("generated field slot should not exhaust u32 field IDs");

        Self(next)
    }
}
