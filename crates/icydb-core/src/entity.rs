//! Module: entity
//!
//! Responsibility: typed entity declaration, placement, value, creation, and
//! singleton contracts.
//! Does not own: accepted schema authority, persisted-row codecs, key byte
//! formats, or generated model reconciliation.
//! Boundary: generated/model proposals and typed values -> database runtime.

use crate::{
    db::EntityKey,
    error::InternalError,
    model::EntityModel,
    traits::{AuthoredFieldProjection, CanisterKind, FieldProjection, StoreKind, TypeKind},
    types::{EntityTag, Id},
};

/// Generated declaration facts for an entity type.
///
/// `NAME` seeds self-referential generated relation metadata. `MODEL` is a
/// proposal consumed during reconciliation; accepted schema snapshots remain
/// the runtime authority for storage, planning, and execution.
pub trait EntityDeclaration: EntityKey {
    /// Stable schema-visible entity name.
    const NAME: &'static str;

    /// Generated model proposal for reconciliation and model-only tooling.
    const MODEL: &'static EntityModel;
}

/// Runtime placement of an entity in one store and canister.
pub trait EntityPlacement {
    /// Store that owns the entity's rows.
    type Store: StoreKind<Canister = Self::Canister>;

    /// Canister that owns the declared store.
    type Canister: CanisterKind;
}

/// Declaration- and placement-bound entity model.
///
/// This contract is sufficient for proposal-based, model-only planning and
/// does not imply that `Self` is a materializable entity value. Stored runtime
/// entities prove that additional capability through `PersistedRow`.
pub trait EntityKind: EntityDeclaration + EntityPlacement + TypeKind {
    /// Stable compact entity identity used by runtime routing.
    const ENTITY_TAG: EntityTag;
}

/// A concrete entity value that can present a typed identity at boundaries.
///
/// Implementors store primitive key material internally. `id()` constructs a
/// typed `Id<Self>` view on demand; that identifier is not proof of authority.
pub trait EntityValue: EntityKey + AuthoredFieldProjection + FieldProjection + Sized {
    /// Return this value's typed entity identity.
    fn id(&self) -> Id<Self>;
}

/// Materialized authored create payload produced by one generated create input.
///
/// Carries both the fully typed entity after-image and authored field slots so
/// save preflight can distinguish omission from authorship.
pub struct EntityCreateMaterialization<E> {
    entity: E,
    authored_slots: Vec<usize>,
}

impl<E> EntityCreateMaterialization<E> {
    /// Build one materialized typed create payload.
    #[must_use]
    pub const fn new(entity: E, authored_slots: Vec<usize>) -> Self {
        Self {
            entity,
            authored_slots,
        }
    }

    /// Consume and return the typed entity after-image.
    #[must_use]
    pub fn into_entity(self) -> E {
        self.entity
    }

    /// Borrow the authored field slots carried by this insert payload.
    #[must_use]
    pub const fn authored_slots(&self) -> &[usize] {
        self.authored_slots.as_slice()
    }
}

/// Create-authored typed input for one entity.
///
/// This is intentionally distinct from the readable entity shape so generated
/// and managed fields remain structurally un-authorable on typed creates.
pub trait EntityCreateInput: Sized {
    /// Entity materialized by this input.
    type Entity: EntityValue;

    /// Materialize one typed create payload plus authored-slot provenance.
    fn materialize_create(self)
    -> Result<EntityCreateMaterialization<Self::Entity>, InternalError>;
}

/// Entity-owned association with its generated create-input shape.
pub trait EntityCreateType: EntityValue {
    /// Generated authored create-input type.
    type Create: EntityCreateInput<Entity = Self>;
}

mod singleton {
    pub trait Key {}

    impl Key for () {}
    impl Key for crate::types::Unit {}

    pub trait Sealed {}

    impl<E> Sealed for E
    where
        E: super::EntityValue,
        E::Key: Key,
    {
    }
}

/// Marker for entities whose unit key proves one logical row.
pub trait SingletonEntity: EntityValue + singleton::Sealed {}

impl<E> SingletonEntity for E where E: EntityValue + singleton::Sealed {}
