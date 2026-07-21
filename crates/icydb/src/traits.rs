//! Module: traits
//!
//! Responsibility: public trait facade and application entity contract.
//! Does not own: core trait implementation semantics.
//! Boundary: re-exports stable trait names and narrows facade-only contracts.

pub use icydb_core::db::{EntityKey, EntityKeyBytes, EntityKeyBytesError};
pub use icydb_core::entity::{
    EntityCreateInput, EntityCreateType, EntityDeclaration, EntityKind, EntityPlacement,
    SingletonEntity,
};
pub use icydb_core::traits::{CanisterKind, FieldTypeMeta, Inner, Kind, Path, StoreKind, TypeKind};
pub use icydb_core::types::NumericValue;
pub use icydb_core::value::{Collection, MapCollection};
pub use icydb_core::visitor::{
    Sanitize, SanitizeAuto, SanitizeCustom, Sanitizer, Validate, ValidateAuto, ValidateCustom,
    Validator, Visitable,
};

///
/// Entity
///
/// Public semantic entity contract for application and repository helpers.
///
/// This trait intentionally hides the lower-level persisted-row and
/// value-projection traits from normal generic bounds. Runtime/session internals
/// still use those storage contracts directly where they encode, decode, plan,
/// or execute persisted rows.
///

pub trait Entity: icydb_core::db::PersistedRow {}

impl<T> Entity for T where T: icydb_core::db::PersistedRow {}

///
/// EntityFor
///
/// Session-local entity contract for APIs that must prove an entity is wired to
/// the current canister's generated store registry.
///
/// Application helpers should prefer `Entity` unless they are themselves
/// generic over a concrete `DbSession<C>`.
///

pub trait EntityFor<C: CanisterKind>: Entity<Canister = C> {}

impl<T, C> EntityFor<C> for T
where
    T: Entity<Canister = C>,
    C: CanisterKind,
{
}

///
/// CreateInput
///
/// Public semantic create-input contract for authored insert payloads.
///
/// Generated create input types implement the lower-level materialization
/// bridge; this trait is the application-facing name for that intent.
///

pub trait CreateInput: EntityCreateInput
where
    Self::Entity: Entity,
{
}

impl<T> CreateInput for T
where
    T: EntityCreateInput,
    T::Entity: Entity,
{
}

///
/// CreateInputFor
///
/// Session-local create-input contract for APIs that must prove the materialized
/// entity is wired to the current canister's generated store registry.
///

pub trait CreateInputFor<C: CanisterKind>: CreateInput
where
    Self::Entity: EntityFor<C>,
{
}

impl<T, C> CreateInputFor<C> for T
where
    T: CreateInput,
    T::Entity: EntityFor<C>,
    C: CanisterKind,
{
}
