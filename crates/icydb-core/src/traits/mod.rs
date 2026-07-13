//! Module: traits
//!
//! Responsibility: core field and entity contracts awaiting narrower domain
//! ownership.
//! Does not own: key taxonomy, runtime value conversion, visitor traversal,
//! executor policy, or public facade DTO behavior.
//! Boundary: remaining reusable contracts consumed throughout `icydb-core`.

mod numeric_value;

use crate::{
    db::EntityKey,
    error::InternalError,
    model::field::{FieldKind, FieldModel, FieldStorageDecode},
    prelude::*,
    types::{EntityTag, Id},
    value::{InputValue, Value},
    visitor::Visitable,
};
use std::collections::{BTreeMap, BTreeSet};

pub use numeric_value::*;

// -----------------------------------------------------------------------------
// Standard re-exports for `traits::X` ergonomics
// -----------------------------------------------------------------------------

pub use ic_memory::stable_structures::storable::Storable;
pub use serde::{Deserialize, Serialize, de::DeserializeOwned};
pub use std::{
    cmp::{Eq, Ordering, PartialEq},
    convert::From,
    default::Default,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign, Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
};

// ============================================================================
// FOUNDATIONAL KINDS
// ============================================================================
//
// These traits define *where* something lives in the system,
// not what data it contains.
//

///
/// Path
/// Fully-qualified schema path.
///

pub trait Path {
    const PATH: &'static str;
}

///
/// Kind
/// Marker for all schema/runtime nodes.
///

pub trait Kind: Path + 'static {}
impl<T> Kind for T where T: Path + 'static {}

///
/// CanisterKind
/// Marker for canister namespaces
///

pub trait CanisterKind: Kind {
    /// Stable memory slot used for commit marker storage.
    const COMMIT_MEMORY_ID: u8;

    /// Durable stable-memory allocation key for commit marker storage.
    const COMMIT_STABLE_KEY: &'static str;
}

///
/// StoreKind
/// Marker for data stores bound to a canister
///

pub trait StoreKind: Kind {
    type Canister: CanisterKind;
}

// ============================================================================
// FIELD & ENTITY SCHEMA
// ============================================================================
//
// These traits describe *what an entity is*, not how it is stored
// or manipulated at runtime.
//

///
/// PersistedByKindCodec
///
/// PersistedByKindCodec lets one field type own the stricter schema-selected
/// `ByKind` persisted-row storage contract.
/// This contract is persistence-only and MUST NOT depend on runtime `Value`
/// conversion, generic fallback bridges, or the runtime value-surface traits.
///

pub trait PersistedByKindCodec: Sized {
    /// Encode one field payload through the explicit `ByKind` storage lane.
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError>;

    /// Decode one optional field payload through the explicit `ByKind`
    /// storage lane, preserving the null sentinel for wrapper-owned optional
    /// handling.
    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError>;
}

///
/// PersistedStructuredFieldCodec
///
/// Direct persisted payload codec for structured field values.
/// This trait owns only the typed field <-> persisted structured payload bytes
/// boundary used by persisted-row storage helpers.
/// It is persistence-only and MUST NOT mention runtime `Value`, rely on
/// generic fallback bridges, or widen into a general structural storage
/// authority.
///

pub trait PersistedStructuredFieldCodec {
    /// Encode this typed structured field into persisted structured payload bytes.
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError>;

    /// Decode this typed structured field from persisted structured payload bytes.
    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError>
    where
        Self: Sized;
}

///
/// EntitySchema
///
/// Declared runtime schema facts for an entity.
///
/// `NAME` seeds self-referential model construction for relation metadata.
/// `MODEL` remains the authoritative runtime authority for field, primary-key,
/// and index metadata consumed by planning and execution.
///

pub trait EntitySchema: EntityKey {
    const NAME: &'static str;
    const MODEL: &'static EntityModel;
}

// ============================================================================
// ENTITY RUNTIME COMPOSITION
// ============================================================================
//
// These traits bind schema-defined entities into runtime placement.
//

///
/// EntityPlacement
///
/// Runtime placement of an entity
///

pub trait EntityPlacement {
    type Store: StoreKind<Canister = Self::Canister>;
    type Canister: CanisterKind;
}

///
/// EntityKind
///
/// Schema- and placement-bound entity model.
///
/// This contract is sufficient for model-only planning and does not imply that
/// `Self` is a materializable entity value. Stored runtime entities prove that
/// additional capability through `PersistedRow`.
///

pub trait EntityKind: EntitySchema + EntityPlacement + TypeKind {
    const ENTITY_TAG: EntityTag;
}

// ============================================================================
// ENTITY VALUES
// ============================================================================
//
// These traits describe *instances* of entities.
//

///
/// EntityValue
///
/// A concrete entity value that can present a typed identity at boundaries.
///
/// Implementors store primitive key material internally.
/// `id()` constructs a typed `Id<Self>` view on demand.
/// The returned `Id<Self>` is a public identifier, not proof of authority.
///

pub trait EntityValue: EntityKey + AuthoredFieldProjection + FieldProjection + Sized {
    fn id(&self) -> Id<Self>;
}

///
/// EntityCreateMaterialization
///
/// Materialized authored create payload produced by one generated create input.
/// Carries both the fully-typed entity after-image and the authored field-slot
/// list so save preflight can still distinguish omission from authorship.
///

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

///
/// EntityCreateInput
///
/// Create-authored typed input for one entity.
/// This is intentionally distinct from the readable entity shape so generated
/// and managed fields can stay structurally un-authorable on typed creates.
///

pub trait EntityCreateInput: Sized {
    type Entity: EntityValue;

    /// Materialize one typed create payload plus authored-slot provenance.
    fn materialize_create(self)
    -> Result<EntityCreateMaterialization<Self::Entity>, InternalError>;
}

///
/// EntityCreateType
///
/// Entity-owned association from one entity type to its generated create
/// input shape.
/// This keeps the public create-input surface generic at the facade boundary
/// while generated code remains free to pick any concrete backing type name.
///

pub trait EntityCreateType: EntityValue {
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

/// Marker for entities whose unit key proves they have exactly one logical row.
pub trait SingletonEntity: EntityValue + singleton::Sealed {}

impl<E> SingletonEntity for E where E: EntityValue + singleton::Sealed {}

///
// ============================================================================
// TYPE SYSTEM CONTRACTS
// ============================================================================
//
// These traits define behavioral expectations for schema-defined types.
//

///
/// TypeKind
///
/// Any schema-defined data type.
///
/// This is a *strong* contract and should only be required
/// where full lifecycle semantics are needed.
///

pub trait TypeKind: Kind + Clone + DeserializeOwned + Visitable + PartialEq {}

impl<T> TypeKind for T where T: Kind + Clone + DeserializeOwned + PartialEq + Visitable {}

///
/// FieldTypeMeta
///
/// Static runtime field metadata for one schema-facing value type.
/// This is the single authority for generated field kind and storage-decode
/// metadata, so callers do not need per-type inherent constants.
///

pub trait FieldTypeMeta {
    /// Semantic field kind used for runtime planning and validation.
    const KIND: FieldKind;

    /// Persisted decode contract used by row and payload decoding.
    const STORAGE_DECODE: FieldStorageDecode;

    /// Known nested fields for generated structured records.
    const NESTED_FIELDS: &'static [FieldModel] = &[];
}

impl<T> FieldTypeMeta for Option<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = T::KIND;
    const STORAGE_DECODE: FieldStorageDecode = T::STORAGE_DECODE;
    const NESTED_FIELDS: &'static [FieldModel] = T::NESTED_FIELDS;
}

impl<T> FieldTypeMeta for Box<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = T::KIND;
    const STORAGE_DECODE: FieldStorageDecode = T::STORAGE_DECODE;
    const NESTED_FIELDS: &'static [FieldModel] = T::NESTED_FIELDS;
}

// Standard containers mirror the generated collection-wrapper contract: their
// semantic kind remains structural, but persisted decode routes through the
// shared structural `Value` storage seam instead of leaf-by-leaf scalar decode.
impl<T> FieldTypeMeta for Vec<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = FieldKind::List(&T::KIND);
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl<T> FieldTypeMeta for BTreeSet<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = FieldKind::Set(&T::KIND);
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl<K, V> FieldTypeMeta for BTreeMap<K, V>
where
    K: FieldTypeMeta,
    V: FieldTypeMeta,
{
    const KIND: FieldKind = FieldKind::Map {
        key: &K::KIND,
        value: &V::KIND,
    };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

/// ============================================================================
/// QUERY VALUE BOUNDARIES
/// ============================================================================

/// Name-based field input projection used before accepted-catalog admission.
pub trait AuthoredFieldProjection {
    /// Resolve one authored field value by stable field slot index.
    fn get_input_value_by_index(&self, index: usize) -> Option<InputValue>;
}

pub trait FieldProjection {
    /// Resolve one field value by stable field slot index.
    fn get_value_by_index(&self, index: usize) -> Option<Value>;
}

/// ============================================================================
/// MISC HELPERS
/// ============================================================================

///
/// Inner
///
/// For newtypes to expose their innermost value.
///

pub trait Inner<T> {
    fn inner(&self) -> &T;
    fn into_inner(self) -> T;
}

///
/// Repr
///
/// Internal representation boundary for scalar wrapper types.
///

pub trait Repr {
    type Inner;

    fn repr(&self) -> Self::Inner;
    fn from_repr(inner: Self::Inner) -> Self;
}
