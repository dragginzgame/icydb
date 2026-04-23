//! Module: traits
//!
//! Responsibility: core trait surface shared across values, entities, and visitors.
//! Does not own: executor/runtime policy or public facade DTO behavior.
//! Boundary: reusable domain contracts consumed throughout `icydb-core`.

#[macro_use]
mod macros;
mod atomic;
mod numeric_value;
mod visitor;

use crate::{
    error::InternalError,
    model::field::{FieldKind, FieldStorageDecode},
    prelude::*,
    types::{EntityTag, Id},
    value::{Value, ValueEnum},
    visitor::VisitorContext,
};
use std::collections::{BTreeMap, BTreeSet};

pub use atomic::*;
pub use numeric_value::*;
pub use visitor::*;

// -----------------------------------------------------------------------------
// Standard re-exports for `traits::X` ergonomics
// -----------------------------------------------------------------------------

pub use canic_cdk::structures::storable::Storable;
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
}

///
/// StoreKind
/// Marker for data stores bound to a canister
///

pub trait StoreKind: Kind {
    type Canister: CanisterKind;
}

// ============================================================================
// ENTITY IDENTITY & SCHEMA
// ============================================================================
//
// These traits describe *what an entity is*, not how it is stored
// or manipulated at runtime.
//

///
/// EntityKey
///
/// Associates an entity with the primitive type used as its primary key.
///
/// ## Semantics
/// - Implemented for entity types
/// - `Self::Key` is the *storage representation* of the primary key
/// - Keys are plain values (Ulid, u64, Principal, …)
/// - Typed identity is provided by `Id<Self>`, not by the key itself
/// - Keys are public identifiers and are never authority-bearing capabilities
///

pub trait EntityKey {
    type Key: Copy + Debug + Eq + Ord + KeyValueCodec + EntityKeyBytes + 'static;
}

///
/// EntityKeyBytes
///

pub trait EntityKeyBytes {
    /// Exact number of bytes produced.
    const BYTE_LEN: usize;

    /// Write bytes into the provided buffer.
    fn write_bytes(&self, out: &mut [u8]);
}

macro_rules! impl_entity_key_bytes_numeric {
    ($($ty:ty),* $(,)?) => {
        $(
            impl EntityKeyBytes for $ty {
                const BYTE_LEN: usize = ::core::mem::size_of::<Self>();

                fn write_bytes(&self, out: &mut [u8]) {
                    assert_eq!(out.len(), Self::BYTE_LEN);
                    out.copy_from_slice(&self.to_be_bytes());
                }
            }
        )*
    };
}

impl_entity_key_bytes_numeric!(i8, i16, i32, i64, u8, u16, u32, u64);

impl EntityKeyBytes for () {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
    }
}

///
/// KeyValueCodec
///
/// Narrow runtime `Value` codec for typed primary keys and key-only access
/// surfaces. This exists to keep cursor, access, and key-routing contracts off
/// the wider structured-value conversion surface used by persisted-field
/// codecs and planner queryability metadata.
///

pub trait KeyValueCodec {
    fn to_key_value(&self) -> Value;

    #[must_use]
    fn from_key_value(value: &Value) -> Option<Self>
    where
        Self: Sized;
}

impl<T> KeyValueCodec for T
where
    T: ValueSurfaceDecode + ValueSurfaceEncode,
{
    fn to_key_value(&self) -> Value {
        self.to_value()
    }

    fn from_key_value(value: &Value) -> Option<Self> {
        Self::from_value(value)
    }
}

///
///
/// ValueSurfaceEncode
///
/// Narrow runtime lowering boundary for typed value surfaces that can be
/// projected into the internal `Value` union.
/// This is the encode-side owner used by generated wrappers and shared helper
/// paths that only need one-way lowering.
///
pub trait ValueSurfaceEncode {
    fn to_value(&self) -> Value;
}

///
/// ValueSurfaceDecode
///
/// Narrow runtime reconstruction boundary for typed value surfaces that can be
/// rebuilt from the internal `Value` union.
/// This is the decode-side owner used by generated wrappers and shared helper
/// paths that only need one-way typed reconstruction.
///
pub trait ValueSurfaceDecode {
    #[must_use]
    fn from_value(value: &Value) -> Option<Self>
    where
        Self: Sized;
}

///
/// value_surface_to_value
///
/// Hidden runtime lowering helper for generated code and other encode-only
/// call sites that should not spell the encode trait directly.
///
pub fn value_surface_to_value<T>(value: &T) -> Value
where
    T: ?Sized + ValueSurfaceEncode,
{
    value.to_value()
}

///
/// value_surface_from_value
///
/// Hidden runtime reconstruction helper for generated code and other decode
/// call sites that should not spell the decode trait directly.
///
#[must_use]
pub fn value_surface_from_value<T>(value: &Value) -> Option<T>
where
    T: ValueSurfaceDecode,
{
    T::from_value(value)
}

///
/// PersistedByKindCodec
///
/// PersistedByKindCodec lets one field type own the stricter schema-selected
/// `ByKind` persisted-row storage contract.
/// This keeps the persisted-row helper boundary off the wider runtime
/// value-surface conversion seam even when the current implementation still delegates to
/// runtime `Value` conversion internally.
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
/// Direct persisted payload codec for custom structured field values.
/// This trait owns only the typed field <-> persisted custom payload bytes
/// boundary used by persisted-row storage helpers.
///

pub trait PersistedStructuredFieldCodec {
    /// Encode this typed structured field into persisted custom payload bytes.
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError>;

    /// Decode this typed structured field from persisted custom payload bytes.
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
    type Store: StoreKind;
    type Canister: CanisterKind;
}

///
/// EntityKind
///
/// Fully runtime-bound entity.
///
/// This is the *maximum* entity contract and should only be
/// required by code that actually touches storage or execution.
///

pub trait EntityKind: EntitySchema + EntityPlacement + Kind + TypeKind {
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

pub trait EntityValue: EntityKey + FieldProjection + Sized {
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
    type Entity: EntityValue + Default;

    /// Materialize one typed create payload plus authored-slot provenance.
    fn materialize_create(self) -> EntityCreateMaterialization<Self::Entity>;
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

/// Marker for entities with exactly one logical row.
pub trait SingletonEntity: EntityValue {}

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

pub trait TypeKind:
    Kind + Clone + Default + DeserializeOwned + Sanitize + Validate + Visitable + PartialEq
{
}

impl<T> TypeKind for T where
    T: Kind + Clone + Default + DeserializeOwned + PartialEq + Sanitize + Validate + Visitable
{
}

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
}

///
/// PersistedFieldMetaCodec
///
/// PersistedFieldMetaCodec lets one field type own the persisted-row
/// encode/decode contract selected by its `FieldTypeMeta`.
/// This keeps the meta-hinted persisted-row path on the field-type owner
/// instead of forcing row helpers to require both the by-kind and direct
/// structured codec traits at once.
///

pub trait PersistedFieldMetaCodec: FieldTypeMeta + Sized {
    /// Encode one non-optional field payload through the type's own
    /// `FieldTypeMeta` storage contract.
    fn encode_persisted_slot_payload_by_meta(
        &self,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError>;

    /// Decode one non-optional field payload through the type's own
    /// `FieldTypeMeta` storage contract.
    fn decode_persisted_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError>;

    /// Encode one optional field payload through the inner type's own
    /// `FieldTypeMeta` storage contract.
    fn encode_persisted_option_slot_payload_by_meta(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError>;

    /// Decode one optional field payload through the inner type's own
    /// `FieldTypeMeta` storage contract.
    fn decode_persisted_option_slot_payload_by_meta(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError>;
}

impl<T> FieldTypeMeta for Option<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = T::KIND;
    const STORAGE_DECODE: FieldStorageDecode = T::STORAGE_DECODE;
}

impl<T> FieldTypeMeta for Box<T>
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = T::KIND;
    const STORAGE_DECODE: FieldStorageDecode = T::STORAGE_DECODE;
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

///
/// Collection
///
/// Explicit iteration contract for list/set wrapper types.
/// Keeps generic collection code on one stable boundary even when concrete
/// wrapper types opt into direct container ergonomics.
///

pub trait Collection {
    type Item;

    /// Iterator over the collection's items, tied to the borrow of `self`.
    type Iter<'a>: Iterator<Item = &'a Self::Item> + 'a
    where
        Self: 'a;

    /// Returns an iterator over the collection's items.
    fn iter(&self) -> Self::Iter<'_>;

    /// Returns the number of items in the collection.
    fn len(&self) -> usize;

    /// Returns true if the collection contains no items.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

///
/// MapCollection
///
/// Explicit iteration contract for map wrapper types.
/// Keeps generic map code on one stable boundary even when concrete wrapper
/// types opt into direct container ergonomics.
///

pub trait MapCollection {
    type Key;
    type Value;

    /// Iterator over the map's key/value pairs, tied to the borrow of `self`.
    type Iter<'a>: Iterator<Item = (&'a Self::Key, &'a Self::Value)> + 'a
    where
        Self: 'a;

    /// Returns an iterator over the map's key/value pairs.
    fn iter(&self) -> Self::Iter<'_>;

    /// Returns the number of entries in the map.
    fn len(&self) -> usize;

    /// Returns true if the map contains no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Collection for Vec<T> {
    type Item = T;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }
}

impl<T> Collection for BTreeSet<T> {
    type Item = T;
    type Iter<'a>
        = std::collections::btree_set::Iter<'a, T>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<K, V> MapCollection for BTreeMap<K, V> {
    type Key = K;
    type Value = V;
    type Iter<'a>
        = std::collections::btree_map::Iter<'a, K, V>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

pub trait EnumValue {
    fn to_value_enum(&self) -> ValueEnum;
}

pub trait FieldProjection {
    /// Resolve one field value by stable field slot index.
    fn get_value_by_index(&self, index: usize) -> Option<Value>;
}

///
/// ValueSurfaceKind
///
/// Schema affordance classification for query planning and validation.
/// Describes whether a field is planner-addressable and predicate-queryable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueSurfaceKind {
    /// Planner-addressable atomic value.
    Atomic,

    /// Structured value with known internal fields that the planner
    /// does not reason about as an addressable query target.
    Structured {
        /// Whether predicates may be expressed against this field.
        queryable: bool,
    },
}

impl ValueSurfaceKind {
    #[must_use]
    pub const fn is_queryable(self) -> bool {
        match self {
            Self::Atomic => true,
            Self::Structured { queryable } => queryable,
        }
    }
}

///
/// ValueSurfaceMeta
///
/// Schema/queryability metadata for one typed field value surface.
/// This stays separate from encode/decode conversion so metadata-only callers do not need
/// to depend on runtime `Value` conversion.
///

pub trait ValueSurfaceMeta {
    fn kind() -> ValueSurfaceKind
    where
        Self: Sized;
}

///
/// value_surface_collection_to_value
///
/// Shared collection-to-`Value::List` lowering for generated wrapper types.
/// This keeps list and set value-surface impls from re-emitting the same item
/// iteration body for every generated schema type.
///

pub fn value_surface_collection_to_value<C>(collection: &C) -> Value
where
    C: Collection,
    C::Item: ValueSurfaceEncode,
{
    Value::List(
        collection
            .iter()
            .map(ValueSurfaceEncode::to_value)
            .collect(),
    )
}

///
/// value_surface_vec_from_value
///
/// Shared `Value::List` decode for generated list wrapper types.
/// This preserves typed value-surface decoding while avoiding one repeated loop
/// body per generated list schema type.
///

#[must_use]
pub fn value_surface_vec_from_value<T>(value: &Value) -> Option<Vec<T>>
where
    T: ValueSurfaceDecode,
{
    let Value::List(values) = value else {
        return None;
    };

    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(T::from_value(value)?);
    }

    Some(out)
}

///
/// value_surface_btree_set_from_value
///
/// Shared `Value::List` decode for generated set wrapper types.
/// This preserves duplicate rejection while avoiding one repeated loop body
/// per generated set schema type.
///

#[must_use]
pub fn value_surface_btree_set_from_value<T>(value: &Value) -> Option<BTreeSet<T>>
where
    T: Ord + ValueSurfaceDecode,
{
    let Value::List(values) = value else {
        return None;
    };

    let mut out = BTreeSet::new();
    for value in values {
        let item = T::from_value(value)?;
        if !out.insert(item) {
            return None;
        }
    }

    Some(out)
}

///
/// value_surface_map_collection_to_value
///
/// Shared map-to-`Value::Map` lowering for generated map wrapper types.
/// This keeps canonicalization and duplicate-key checks in one runtime helper
/// instead of re-emitting the same map conversion body per generated schema
/// type.
///

pub fn value_surface_map_collection_to_value<M>(map: &M, path: &'static str) -> Value
where
    M: MapCollection,
    M::Key: ValueSurfaceEncode,
    M::Value: ValueSurfaceEncode,
{
    let mut entries: Vec<(Value, Value)> = map
        .iter()
        .map(|(key, value)| {
            (
                ValueSurfaceEncode::to_value(key),
                ValueSurfaceEncode::to_value(value),
            )
        })
        .collect();

    if let Err(err) = Value::validate_map_entries(entries.as_slice()) {
        debug_assert!(false, "invalid map field value for {path}: {err}");
        return Value::Map(entries);
    }

    Value::sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == Ordering::Equal {
            debug_assert!(
                false,
                "duplicate map key in {path} after value-surface canonicalization",
            );
            break;
        }
    }

    Value::Map(entries)
}

///
/// value_surface_btree_map_from_value
///
/// Shared `Value::Map` decode for generated map wrapper types.
/// This keeps canonical-entry normalization in one runtime helper instead of
/// re-emitting the same decode body per generated schema type.
///

#[must_use]
pub fn value_surface_btree_map_from_value<K, V>(value: &Value) -> Option<BTreeMap<K, V>>
where
    K: Ord + ValueSurfaceDecode,
    V: ValueSurfaceDecode,
{
    let Value::Map(entries) = value else {
        return None;
    };

    let normalized = Value::normalize_map_entries(entries.clone()).ok()?;
    if normalized.as_slice() != entries.as_slice() {
        return None;
    }

    let mut map = BTreeMap::new();
    for (entry_key, entry_value) in normalized {
        let key = K::from_value(&entry_key)?;
        let value = V::from_value(&entry_value)?;
        map.insert(key, value);
    }

    Some(map)
}

///
/// value_surface_from_vec_into
///
/// Shared `Vec<I> -> Vec<T>` conversion for generated wrapper `From<Vec<I>>`
/// impls. This keeps list wrappers from re-emitting the same `into_iter` /
/// `map(Into::into)` collection body for every generated schema type.
///

#[must_use]
pub fn value_surface_from_vec_into<T, I>(entries: Vec<I>) -> Vec<T>
where
    I: Into<T>,
{
    entries.into_iter().map(Into::into).collect()
}

///
/// value_surface_from_vec_into_btree_set
///
/// Shared `Vec<I> -> BTreeSet<T>` conversion for generated set wrapper
/// `From<Vec<I>>` impls. This keeps set wrappers from re-emitting the same
/// collection conversion body for every generated schema type.
///

#[must_use]
pub fn value_surface_from_vec_into_btree_set<T, I>(entries: Vec<I>) -> BTreeSet<T>
where
    I: Into<T>,
    T: Ord,
{
    entries.into_iter().map(Into::into).collect()
}

///
/// value_surface_from_vec_into_btree_map
///
/// Shared `Vec<(IK, IV)> -> BTreeMap<K, V>` conversion for generated map
/// wrapper `From<Vec<(IK, IV)>>` impls. This keeps map wrappers from
/// re-emitting the same pair-conversion body for every generated schema type.
///

#[must_use]
pub fn value_surface_from_vec_into_btree_map<K, V, IK, IV>(entries: Vec<(IK, IV)>) -> BTreeMap<K, V>
where
    IK: Into<K>,
    IV: Into<V>,
    K: Ord,
{
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
}

///
/// value_surface_into
///
/// Shared `Into<T>` lowering for generated newtype `From<U>` impls.
/// This keeps newtype wrappers from re-emitting the same single-field
/// conversion body for every generated schema type.
///

#[must_use]
pub fn value_surface_into<T, U>(value: U) -> T
where
    U: Into<T>,
{
    value.into()
}

impl ValueSurfaceMeta for &str {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueSurfaceEncode for &str {
    fn to_value(&self) -> Value {
        Value::Text((*self).to_string())
    }
}

impl ValueSurfaceDecode for &str {
    fn from_value(_value: &Value) -> Option<Self> {
        None
    }
}

impl ValueSurfaceMeta for String {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueSurfaceEncode for String {
    fn to_value(&self) -> Value {
        Value::Text(self.clone())
    }
}

impl ValueSurfaceDecode for String {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Text(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl<T: ValueSurfaceMeta> ValueSurfaceMeta for Option<T> {
    fn kind() -> ValueSurfaceKind {
        T::kind()
    }
}

impl<T: ValueSurfaceEncode> ValueSurfaceEncode for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }
}

impl<T: ValueSurfaceDecode> ValueSurfaceDecode for Option<T> {
    fn from_value(value: &Value) -> Option<Self> {
        if matches!(value, Value::Null) {
            return Some(None);
        }

        T::from_value(value).map(Some)
    }
}

impl<T: ValueSurfaceMeta> ValueSurfaceMeta for Box<T> {
    fn kind() -> ValueSurfaceKind {
        T::kind()
    }
}

impl<T: ValueSurfaceEncode> ValueSurfaceEncode for Box<T> {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

impl<T: ValueSurfaceDecode> ValueSurfaceDecode for Box<T> {
    fn from_value(value: &Value) -> Option<Self> {
        T::from_value(value).map(Self::new)
    }
}

impl<T> ValueSurfaceMeta for Vec<T> {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Structured { queryable: true }
    }
}

impl<T: ValueSurfaceEncode> ValueSurfaceEncode for Vec<T> {
    fn to_value(&self) -> Value {
        value_surface_collection_to_value(self)
    }
}

impl<T: ValueSurfaceDecode> ValueSurfaceDecode for Vec<T> {
    fn from_value(value: &Value) -> Option<Self> {
        value_surface_vec_from_value(value)
    }
}

impl<T> ValueSurfaceMeta for BTreeSet<T>
where
    T: Ord,
{
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Structured { queryable: true }
    }
}

impl<T> ValueSurfaceEncode for BTreeSet<T>
where
    T: Ord + ValueSurfaceEncode,
{
    fn to_value(&self) -> Value {
        value_surface_collection_to_value(self)
    }
}

impl<T> ValueSurfaceDecode for BTreeSet<T>
where
    T: Ord + ValueSurfaceDecode,
{
    fn from_value(value: &Value) -> Option<Self> {
        value_surface_btree_set_from_value(value)
    }
}

impl<K, V> ValueSurfaceMeta for BTreeMap<K, V>
where
    K: Ord,
{
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Structured { queryable: true }
    }
}

impl<K, V> ValueSurfaceEncode for BTreeMap<K, V>
where
    K: Ord + ValueSurfaceEncode,
    V: ValueSurfaceEncode,
{
    fn to_value(&self) -> Value {
        value_surface_map_collection_to_value(self, std::any::type_name::<Self>())
    }
}

impl<K, V> ValueSurfaceDecode for BTreeMap<K, V>
where
    K: Ord + ValueSurfaceDecode,
    V: ValueSurfaceDecode,
{
    fn from_value(value: &Value) -> Option<Self> {
        value_surface_btree_map_from_value(value)
    }
}

// impl_field_value
#[macro_export]
macro_rules! impl_field_value {
    ( $( $type:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl ValueSurfaceMeta for $type {
                fn kind() -> ValueSurfaceKind {
                    ValueSurfaceKind::Atomic
                }
            }

            impl ValueSurfaceEncode for $type {
                fn to_value(&self) -> Value {
                    Value::$variant((*self).into())
                }
            }

            impl ValueSurfaceDecode for $type {
                fn from_value(value: &Value) -> Option<Self> {
                    match value {
                        Value::$variant(v) => (*v).try_into().ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_field_value!(
    i8 => Int,
    i16 => Int,
    i32 => Int,
    i64 => Int,
    u8 => Uint,
    u16 => Uint,
    u32 => Uint,
    u64 => Uint,
    bool => Bool,
);

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

impl<T> Inner<T> for T
where
    T: Atomic,
{
    fn inner(&self) -> &T {
        self
    }

    fn into_inner(self) -> T {
        self
    }
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

/// ============================================================================
/// SANITIZATION / VALIDATION
/// ============================================================================

///
/// Sanitizer
///
/// Transforms a value into a sanitized version.
///

pub trait Sanitizer<T> {
    fn sanitize(&self, value: &mut T) -> Result<(), String>;

    fn sanitize_with_context(
        &self,
        value: &mut T,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), String> {
        let _ = ctx;

        self.sanitize(value)
    }
}

///
/// Validator
///
/// Allows a node to validate values.
///

pub trait Validator<T: ?Sized> {
    fn validate(&self, value: &T, ctx: &mut dyn VisitorContext);
}
