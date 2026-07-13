//! Module: value::runtime
//!
//! Responsibility: typed runtime conversion contracts and shared collection
//! lowering/reconstruction helpers for the canonical [`Value`] domain.
//! Does not own: primary-key encoding or persisted field codecs.
//! Boundary: runtime-only typed value conversion used by generated wrappers,
//! query values, and accepted-catalog enum reconstruction.

use super::{Value, ValueEnum};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

/// Schema affordance classification for query planning and validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeValueKind {
    /// Planner-addressable atomic value.
    Atomic,

    /// Structured value whose internal fields are not planner-addressable.
    Structured {
        /// Whether predicates may be expressed against this field.
        queryable: bool,
    },
}

impl RuntimeValueKind {
    /// Return whether this runtime value kind may be used in predicates.
    #[must_use]
    pub const fn is_queryable(self) -> bool {
        match self {
            Self::Atomic => true,
            Self::Structured { queryable } => queryable,
        }
    }
}

/// Schema/queryability metadata for one typed runtime value surface.
pub trait RuntimeValueMeta {
    /// Return the planner affordance category for this value type.
    fn kind() -> RuntimeValueKind
    where
        Self: Sized;
}

/// Runtime-only lowering from a typed value into the canonical [`Value`] union.
///
/// This contract must not be used as a persisted-row or primary-key codec.
pub trait RuntimeValueEncode {
    /// Lower this typed value into its canonical runtime representation.
    fn to_value(&self) -> Value;
}

/// Runtime-only reconstruction of a typed value from the canonical [`Value`]
/// union.
///
/// This contract must not be used as a persisted-row or primary-key codec.
pub trait RuntimeValueDecode {
    /// Reconstruct a typed value when the runtime representation matches.
    #[must_use]
    fn from_value(value: &Value) -> Option<Self>
    where
        Self: Sized;

    /// Reconstruct through accepted catalog authority when the value graph may
    /// contain store-local enum IDs.
    #[doc(hidden)]
    fn from_value_with_enum_context(
        value: &Value,
        _context: &dyn RuntimeEnumContext,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        Self::from_value(value)
    }
}

/// Catalog-resolved view of one canonical runtime enum value.
#[doc(hidden)]
pub struct RuntimeEnumSelection<'a> {
    pub path: &'a str,
    pub variant: &'a str,
    pub payload: Option<&'a Value>,
}

/// Opaque accepted-catalog resolver used by generated typed decode.
#[doc(hidden)]
pub trait RuntimeEnumContext {
    fn resolve_enum<'a>(&'a self, value: &'a ValueEnum) -> Option<RuntimeEnumSelection<'a>>;
}

/// Explicit iteration contract for generated list and set wrapper types.
pub trait Collection {
    /// Element type exposed by the collection.
    type Item;

    /// Iterator tied to the borrow of this collection.
    type Iter<'a>: Iterator<Item = &'a Self::Item> + 'a
    where
        Self: 'a;

    /// Iterate over the collection's items.
    fn iter(&self) -> Self::Iter<'_>;

    /// Return the number of items in the collection.
    fn len(&self) -> usize;

    /// Return whether the collection contains no items.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Explicit iteration contract for generated map wrapper types.
pub trait MapCollection {
    /// Key type exposed by the map.
    type Key;

    /// Value type exposed by the map.
    type Value;

    /// Iterator tied to the borrow of this map.
    type Iter<'a>: Iterator<Item = (&'a Self::Key, &'a Self::Value)> + 'a
    where
        Self: 'a;

    /// Iterate over the map's entries.
    fn iter(&self) -> Self::Iter<'_>;

    /// Return the number of entries in the map.
    fn len(&self) -> usize;

    /// Return whether the map contains no entries.
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

/// Lower one typed runtime value without naming its encode trait at the call
/// site.
pub fn runtime_value_to_value<T>(value: &T) -> Value
where
    T: ?Sized + RuntimeValueEncode,
{
    value.to_value()
}

/// Reconstruct one typed runtime value without naming its decode trait at the
/// call site.
#[must_use]
pub fn runtime_value_from_value<T>(value: &Value) -> Option<T>
where
    T: RuntimeValueDecode,
{
    T::from_value(value)
}

/// Reconstruct one generated enum graph through accepted catalog authority.
#[doc(hidden)]
pub fn runtime_value_from_value_with_enum_context<T>(
    value: &Value,
    context: &dyn RuntimeEnumContext,
) -> Option<T>
where
    T: RuntimeValueDecode,
{
    T::from_value_with_enum_context(value, context)
}

/// Reconstruct with accepted enum authority when the row reader carries it.
#[doc(hidden)]
#[must_use]
pub fn runtime_value_from_value_with_optional_enum_context<T>(
    value: &Value,
    context: Option<&dyn RuntimeEnumContext>,
) -> Option<T>
where
    T: RuntimeValueDecode,
{
    match context {
        Some(context) => T::from_value_with_enum_context(value, context),
        None => T::from_value(value),
    }
}

/// Lower a generated collection wrapper into a runtime list value.
pub fn runtime_value_collection_to_value<C>(collection: &C) -> Value
where
    C: Collection,
    C::Item: RuntimeValueEncode,
{
    Value::List(
        collection
            .iter()
            .map(RuntimeValueEncode::to_value)
            .collect(),
    )
}

/// Decode a runtime list into a typed vector.
#[must_use]
pub fn runtime_value_vec_from_value<T>(value: &Value) -> Option<Vec<T>>
where
    T: RuntimeValueDecode,
{
    decode_runtime_value_list(value, T::from_value)
}

fn decode_runtime_value_list<T>(
    value: &Value,
    mut decode: impl FnMut(&Value) -> Option<T>,
) -> Option<Vec<T>> {
    let Value::List(values) = value else {
        return None;
    };

    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(decode(value)?);
    }

    Some(out)
}

/// Decode a runtime list into a typed set, rejecting decoded duplicates.
#[must_use]
pub fn runtime_value_btree_set_from_value<T>(value: &Value) -> Option<BTreeSet<T>>
where
    T: Ord + RuntimeValueDecode,
{
    decode_runtime_value_set(value, T::from_value)
}

fn decode_runtime_value_set<T>(
    value: &Value,
    mut decode: impl FnMut(&Value) -> Option<T>,
) -> Option<BTreeSet<T>>
where
    T: Ord,
{
    let Value::List(values) = value else {
        return None;
    };

    let mut out = BTreeSet::new();
    for value in values {
        let item = decode(value)?;
        if !out.insert(item) {
            return None;
        }
    }

    Some(out)
}

/// Lower a generated map wrapper into a canonical runtime map value.
pub fn runtime_value_map_collection_to_value<M>(map: &M, path: &'static str) -> Value
where
    M: MapCollection,
    M::Key: RuntimeValueEncode,
    M::Value: RuntimeValueEncode,
{
    let mut entries: Vec<(Value, Value)> = map
        .iter()
        .map(|(key, value)| {
            (
                RuntimeValueEncode::to_value(key),
                RuntimeValueEncode::to_value(value),
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

/// Decode a canonical runtime map into a typed map, rejecting decoded key
/// collisions.
#[must_use]
pub fn runtime_value_btree_map_from_value<K, V>(value: &Value) -> Option<BTreeMap<K, V>>
where
    K: Ord + RuntimeValueDecode,
    V: RuntimeValueDecode,
{
    decode_runtime_value_map(value, K::from_value, V::from_value)
}

fn decode_runtime_value_map<K, V>(
    value: &Value,
    mut decode_key: impl FnMut(&Value) -> Option<K>,
    mut decode_value: impl FnMut(&Value) -> Option<V>,
) -> Option<BTreeMap<K, V>>
where
    K: Ord,
{
    let Value::Map(entries) = value else {
        return None;
    };

    Value::validate_map_entries(entries).ok()?;
    if !Value::map_entries_are_strictly_canonical(entries) {
        return None;
    }

    let mut map = BTreeMap::new();
    for (entry_key, entry_value) in entries {
        let key = decode_key(entry_key)?;
        let value = decode_value(entry_value)?;
        match map.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(_) => return None,
        }
    }

    Some(map)
}

/// Convert generated list wrapper input entries into their stored item type.
#[must_use]
pub fn runtime_value_from_vec_into<T, I>(entries: Vec<I>) -> Vec<T>
where
    I: Into<T>,
{
    entries.into_iter().map(Into::into).collect()
}

/// Convert generated set wrapper input entries into their stored item type.
#[must_use]
pub fn runtime_value_from_vec_into_btree_set<T, I>(entries: Vec<I>) -> BTreeSet<T>
where
    I: Into<T>,
    T: Ord,
{
    entries.into_iter().map(Into::into).collect()
}

/// Convert generated map wrapper input entries into their stored key/value
/// types.
#[must_use]
pub fn runtime_value_from_vec_into_btree_map<K, V, IK, IV>(entries: Vec<(IK, IV)>) -> BTreeMap<K, V>
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

/// Apply one generated newtype input conversion.
#[must_use]
pub fn runtime_value_into<T, U>(value: U) -> T
where
    U: Into<T>,
{
    value.into()
}

impl RuntimeValueMeta for Value {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl RuntimeValueDecode for Value {
    fn from_value(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

impl RuntimeValueMeta for &str {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for &str {
    fn to_value(&self) -> Value {
        Value::Text((*self).to_string())
    }
}

impl RuntimeValueMeta for String {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for String {
    fn to_value(&self) -> Value {
        Value::Text(self.clone())
    }
}

impl RuntimeValueDecode for String {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Text(value) => Some(value.clone()),
            _ => None,
        }
    }
}

impl<T: RuntimeValueMeta> RuntimeValueMeta for Option<T> {
    fn kind() -> RuntimeValueKind {
        T::kind()
    }
}

impl<T: RuntimeValueEncode> RuntimeValueEncode for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(value) => value.to_value(),
            None => Value::Null,
        }
    }
}

impl<T: RuntimeValueDecode> RuntimeValueDecode for Option<T> {
    fn from_value(value: &Value) -> Option<Self> {
        if matches!(value, Value::Null) {
            return Some(None);
        }

        T::from_value(value).map(Some)
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        if matches!(value, Value::Null) {
            return Some(None);
        }

        T::from_value_with_enum_context(value, context).map(Some)
    }
}

impl<T: RuntimeValueMeta> RuntimeValueMeta for Box<T> {
    fn kind() -> RuntimeValueKind {
        T::kind()
    }
}

impl<T: RuntimeValueEncode> RuntimeValueEncode for Box<T> {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

impl<T: RuntimeValueDecode> RuntimeValueDecode for Box<T> {
    fn from_value(value: &Value) -> Option<Self> {
        T::from_value(value).map(Self::new)
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        T::from_value_with_enum_context(value, context).map(Self::new)
    }
}

impl<T> RuntimeValueMeta for Vec<T> {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Structured { queryable: true }
    }
}

impl<T: RuntimeValueEncode> RuntimeValueEncode for Vec<T> {
    fn to_value(&self) -> Value {
        runtime_value_collection_to_value(self)
    }
}

impl<T: RuntimeValueDecode> RuntimeValueDecode for Vec<T> {
    fn from_value(value: &Value) -> Option<Self> {
        runtime_value_vec_from_value(value)
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        decode_runtime_value_list(value, |value| {
            T::from_value_with_enum_context(value, context)
        })
    }
}

impl<T> RuntimeValueMeta for BTreeSet<T>
where
    T: Ord,
{
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Structured { queryable: true }
    }
}

impl<T> RuntimeValueEncode for BTreeSet<T>
where
    T: Ord + RuntimeValueEncode,
{
    fn to_value(&self) -> Value {
        runtime_value_collection_to_value(self)
    }
}

impl<T> RuntimeValueDecode for BTreeSet<T>
where
    T: Ord + RuntimeValueDecode,
{
    fn from_value(value: &Value) -> Option<Self> {
        runtime_value_btree_set_from_value(value)
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        decode_runtime_value_set(value, |value| {
            T::from_value_with_enum_context(value, context)
        })
    }
}

impl<K, V> RuntimeValueMeta for BTreeMap<K, V>
where
    K: Ord,
{
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Structured { queryable: true }
    }
}

impl<K, V> RuntimeValueEncode for BTreeMap<K, V>
where
    K: Ord + RuntimeValueEncode,
    V: RuntimeValueEncode,
{
    fn to_value(&self) -> Value {
        runtime_value_map_collection_to_value(self, std::any::type_name::<Self>())
    }
}

impl<K, V> RuntimeValueDecode for BTreeMap<K, V>
where
    K: Ord + RuntimeValueDecode,
    V: RuntimeValueDecode,
{
    fn from_value(value: &Value) -> Option<Self> {
        runtime_value_btree_map_from_value(value)
    }

    fn from_value_with_enum_context(
        value: &Value,
        context: &dyn RuntimeEnumContext,
    ) -> Option<Self> {
        decode_runtime_value_map(
            value,
            |key| K::from_value_with_enum_context(key, context),
            |value| V::from_value_with_enum_context(value, context),
        )
    }
}

macro_rules! impl_runtime_value {
    ( $( $type:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl RuntimeValueMeta for $type {
                fn kind() -> RuntimeValueKind {
                    RuntimeValueKind::Atomic
                }
            }

            impl RuntimeValueEncode for $type {
                fn to_value(&self) -> Value {
                    Value::$variant((*self).into())
                }
            }

            impl RuntimeValueDecode for $type {
                fn from_value(value: &Value) -> Option<Self> {
                    match value {
                        Value::$variant(value) => (*value).try_into().ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_runtime_value!(
    i8 => Int64,
    i16 => Int64,
    i32 => Int64,
    i64 => Int64,
    i128 => Int128,
    u8 => Nat64,
    u16 => Nat64,
    u32 => Nat64,
    u64 => Nat64,
    u128 => Nat128,
    bool => Bool,
);
