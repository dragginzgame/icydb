pub use icydb_core::traits::{
    Add, AddAssign, CanisterKind, Collection, Debug, Default, Deserialize, DeserializeOwned, Div,
    DivAssign, EntityCreateInput, EntityCreateMaterialization, EntityCreateType, EntityKey,
    EntityKeyBytes, EntityKind, EntityPlacement, EntitySchema, EntityValue, Eq, FieldTypeMeta,
    From, Hash, Inner, Kind, MapCollection, Mul, MulAssign, NumericValue, Ordering, PartialEq,
    Path, Rem, Sanitize, SanitizeAuto, SanitizeCustom, Sanitizer, Serialize, SingletonEntity,
    Storable, StoreKind, Sub, SubAssign, TypeKind, Validate, ValidateAuto, ValidateCustom,
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

pub trait Entity: icydb_core::db::PersistedRow + EntityValue {}

impl<T> Entity for T where T: icydb_core::db::PersistedRow + EntityValue {}

///
/// EntityFor
///
/// Session-local entity contract for APIs that must prove an entity is wired to
/// the current canister's generated store registry.
///
/// Application helpers should prefer `Entity` unless they are themselves
/// generic over a concrete `DbSession<C>`.
///

pub trait EntityFor<C: CanisterKind>: Entity + EntityKind<Canister = C> {}

impl<T, C> EntityFor<C> for T
where
    T: Entity + EntityKind<Canister = C>,
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
