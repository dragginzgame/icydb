//! Module: traits
//!
//! Responsibility: foundational kind, field metadata, projection, and wrapper
//! contracts awaiting narrower domain ownership.
//! Does not own: entity composition, key taxonomy, runtime value conversion,
//! visitor traversal, executor policy, or public facade DTO behavior.
//! Boundary: remaining reusable contracts consumed throughout `icydb-core`.

use crate::{
    model::field::{FieldKind, FieldModel, FieldStorageDecode},
    value::{InputValue, Value},
    visitor::Visitable,
};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

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
