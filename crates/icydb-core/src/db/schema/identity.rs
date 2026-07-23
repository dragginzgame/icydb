//! Module: db::schema::identity
//! Responsibility: durable schema identity primitives.
//! Does not own: generated model validation or persisted schema reconciliation.
//! Boundary: small copyable identifiers shared by schema proposal and live-schema code.

use std::num::NonZeroU32;

///
/// ConstraintId
///
/// Stable non-zero identity for one accepted constraint within an entity's
/// schema history.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct ConstraintId(NonZeroU32);

impl ConstraintId {
    /// Admit one non-zero persisted constraint identity.
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
}

///
/// ConstraintIdAllocator
///
/// Persisted non-reusing high-water state for entity-local constraint IDs.
/// Dropping or aborting a constraint never lowers this value.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct ConstraintIdAllocator {
    high_water: Option<ConstraintId>,
}

impl ConstraintIdAllocator {
    /// Build allocator state from its trusted persisted high-water value.
    #[must_use]
    pub(in crate::db) const fn new(high_water: u32) -> Self {
        Self {
            high_water: ConstraintId::new(high_water),
        }
    }

    /// Return the greatest constraint ID ever reserved by this entity.
    #[must_use]
    pub(in crate::db) const fn high_water(self) -> u32 {
        match self.high_water {
            Some(high_water) => high_water.get(),
            None => 0,
        }
    }

    /// Reserve the next non-reusing constraint identity in candidate state.
    ///
    /// The returned allocator is not authoritative until its containing schema
    /// candidate publishes. Exhaustion leaves the accepted allocator unchanged.
    #[must_use]
    pub(in crate::db) const fn checked_reserve(self) -> Option<(Self, ConstraintId)> {
        let next = match self.high_water {
            Some(high_water) => match high_water.get().checked_add(1) {
                Some(next) => next,
                None => return None,
            },
            None => 1,
        };
        let Some(id) = ConstraintId::new(next) else {
            return None;
        };

        Some((
            Self {
                high_water: Some(id),
            },
            id,
        ))
    }
}

///
/// SchemaIndexId
///
/// Stable non-zero logical identity for one accepted secondary-index
/// definition. Physical index keys continue to use dense runtime ordinals.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct SchemaIndexId(NonZeroU32);

impl SchemaIndexId {
    /// Admit one non-zero persisted logical index identity.
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
}

///
/// RelationId
///
/// Stable non-zero logical identity for one accepted relation definition.
/// Relation key encoding remains owned by the existing relation contract.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct RelationId(NonZeroU32);

impl RelationId {
    /// Admit one non-zero persisted relation identity.
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
}

///
/// FieldId
///
/// Durable identity for one logical schema field.
/// This ID is distinct from generated Rust field order and from executor slot
/// indexes so schema reconciliation can preserve identity across safe reorders
/// and renames.
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
    /// derives IDs deterministically from generated slot order. Reconciliation
    /// preserves stored IDs instead of recalculating them.
    #[must_use]
    pub(in crate::db) fn from_initial_slot(slot: usize) -> Self {
        let next = u32::try_from(slot)
            .expect("schema identity invariant")
            .checked_add(1)
            .expect("schema identity invariant");

        Self(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_allocator_preserves_reserved_high_water_identity() {
        let empty = ConstraintIdAllocator::default();
        let reserved = ConstraintIdAllocator::new(7);

        assert_eq!(empty.high_water(), 0);
        assert_eq!(reserved.high_water(), 7);
    }

    #[test]
    fn constraint_allocator_reserves_monotonically_and_fails_closed_at_exhaustion() {
        let (first, first_id) = ConstraintIdAllocator::default()
            .checked_reserve()
            .expect("empty allocator should reserve its first identity");
        let (second, second_id) = first
            .checked_reserve()
            .expect("allocator should reserve its next identity");

        assert_eq!(first_id.get(), 1);
        assert_eq!(second_id.get(), 2);
        assert_eq!(second.high_water(), 2);
        assert_eq!(ConstraintIdAllocator::new(u32::MAX).checked_reserve(), None,);
    }

    #[test]
    fn logical_structural_ids_reject_zero() {
        assert_eq!(SchemaIndexId::new(0), None);
        assert_eq!(RelationId::new(0), None);
    }
}
