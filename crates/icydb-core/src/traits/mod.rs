#[macro_use]
mod macros;
mod view;
mod visitor;

pub use view::*;
pub use visitor::*;

// re-exports of other traits
// for the standard traits::X pattern
pub use canic_cdk::structures::storable::Storable;
pub use num_traits::{FromPrimitive as NumFromPrimitive, NumCast, ToPrimitive as NumToPrimitive};
pub use serde::{Deserialize, Serialize, de::DeserializeOwned};
pub use std::{
    cmp::{Eq, Ordering, PartialEq},
    convert::{AsRef, From, Into},
    default::Default,
    fmt::{Debug, Display},
    hash::Hash,
    iter::IntoIterator,
    ops::{Add, AddAssign, Deref, DerefMut, Mul, MulAssign, Sub, SubAssign},
    str::FromStr,
};

use crate::{
    IndexSpec, Key, Value,
    db::primitives::{
        BoolEqualityFilterKind, BoolListFilterKind, FilterKind, Int64RangeFilterKind,
        IntListFilterKind, Nat64RangeFilterKind, NatListFilterKind, TextFilterKind,
        TextListFilterKind,
    },
    visitor::VisitorContext,
};

/// ------------------------
/// KIND TRAITS
/// the Schema uses the term "Node" but when they're built it's "Kind"
/// ------------------------

///
/// Kind
///

pub trait Kind: Path + 'static {}

impl<T> Kind for T where T: Path + 'static {}

///
/// CanisterKind
///

pub trait CanisterKind: Kind {}

///
/// EntityKind
///

pub trait EntityKind: Kind + TypeKind + FieldValues {
    type PrimaryKey: Copy + Into<Key>;
    type Store: StoreKind;
    type Canister: CanisterKind; // Self::Store::Canister shortcut

    const ENTITY_ID: u64;
    const PRIMARY_KEY: &'static str;
    const FIELDS: &'static [&'static str];
    const INDEXES: &'static [&'static IndexSpec];

    fn key(&self) -> Key;
    fn primary_key(&self) -> Self::PrimaryKey;
    fn set_primary_key(&mut self, key: Self::PrimaryKey);
}

///
/// StoreKind
///

pub trait StoreKind: Kind {
    type Canister: CanisterKind;
}

/// ------------------------
/// TYPE TRAITS
/// ------------------------

///
/// TypeKind
/// any data type
///

pub trait TypeKind:
    Kind
    + View
    + Clone
    + Default
    + Serialize
    + DeserializeOwned
    + Sanitize
    + Validate
    + Visitable
    + PartialEq
{
}

impl<T> TypeKind for T where
    T: Kind
        + View
        + Clone
        + Default
        + DeserializeOwned
        + PartialEq
        + Serialize
        + Sanitize
        + Validate
        + Visitable
{
}

/// ------------------------
/// OTHER TRAITS
/// ------------------------

///
/// FieldValues
///

pub trait FieldValues {
    fn get_value(&self, field: &str) -> Option<Value>;
}

///
/// FieldValue
///

pub trait FieldValue {
    fn to_value(&self) -> Value {
        Value::Unsupported
    }
}

impl FieldValue for &str {
    fn to_value(&self) -> Value {
        Value::Text((*self).to_string())
    }
}

impl FieldValue for String {
    fn to_value(&self) -> Value {
        Value::Text(self.clone())
    }
}

impl<T: FieldValue + Clone> FieldValue for &T {
    fn to_value(&self) -> Value {
        (*self).clone().to_value()
    }
}

impl<T: FieldValue> FieldValue for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::None,
        }
    }
}

impl<T: FieldValue> FieldValue for Vec<T> {
    fn to_value(&self) -> Value {
        Value::List(self.iter().map(FieldValue::to_value).collect())
    }
}

impl<T: FieldValue> FieldValue for Box<T> {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

// impl_field_value
#[macro_export]
macro_rules! impl_field_value {
    ( $( $type:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl FieldValue for $type {
                fn to_value(&self) -> Value {
                    Value::$variant((*self).into())
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

///
/// Filterable
///

pub trait Filterable {
    type Filter: FilterKind;
    type ListFilter: FilterKind;
}

macro_rules! impl_filterable {
    // Case 1: type => scalar_filter, list_filter
    ( $( $type:ty => $filter:path, $list_filter:path );* $(;)? ) => {
        $(
            impl Filterable for $type {
                type Filter = $filter;
                type ListFilter = $list_filter;
            }
        )*
    };
}

impl_filterable! {
    bool    => BoolEqualityFilterKind, BoolListFilterKind;
    i8      => Int64RangeFilterKind, IntListFilterKind;
    i16     => Int64RangeFilterKind, IntListFilterKind;
    i32     => Int64RangeFilterKind, IntListFilterKind;
    i64     => Int64RangeFilterKind, IntListFilterKind;

    u8      => Nat64RangeFilterKind, NatListFilterKind;
    u16     => Nat64RangeFilterKind, NatListFilterKind;
    u32     => Nat64RangeFilterKind, NatListFilterKind;
    u64     => Nat64RangeFilterKind, NatListFilterKind;

    String  => TextFilterKind, TextListFilterKind;
}

///
/// FromKey
/// Convert a stored [`Key`] into a concrete type.
/// Returns `None` if the key cannot represent this type.
///

pub trait FromKey: Copy {
    fn try_from_key(key: Key) -> Option<Self>;
}

#[macro_export]
macro_rules! impl_from_key_int {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl FromKey for $ty {
                fn try_from_key(key: Key) -> Option<Self> {
                    match key {
                        Key::Int(v) => Self::try_from(v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! impl_from_key_uint {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl FromKey for $ty {
                fn try_from_key(key: Key) -> Option<Self> {
                    match key {
                        Key::Uint(v) => Self::try_from(v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_from_key_int!(i8, i16, i32, i64);
impl_from_key_uint!(u8, u16, u32, u64);

///
/// Inner
/// for Newtypes to get the innermost value
///
/// DO NOT REMOVE - its been added and removed twice already, NumCast
/// is a pain to use and won't work for half our types
///

pub trait Inner<T> {
    fn inner(&self) -> &T;
    fn into_inner(self) -> T;
}

// impl_inner
#[macro_export]
macro_rules! impl_inner {
    ($($type:ty),*) => {
        $(
            impl Inner<$type> for $type {
                fn inner(&self) -> &$type {
                    &self
                }
                fn into_inner(self) -> $type {
                    self
                }
            }
        )*
    };
}

impl_inner!(
    bool, f32, f64, i8, i16, i32, i64, i128, String, u8, u16, u32, u64, u128
);

///
/// Path
///
/// any node created via a macro has a Path
/// ie. design::game::rarity::Rarity
///

pub trait Path {
    const PATH: &'static str;
}

///
/// Sanitizer
/// transforms a value into a sanitized version
///

pub trait Sanitizer<T> {
    /// Apply in-place sanitization.
    ///
    /// - `Ok(())` means success (possibly with issues recorded by the caller)
    /// - `Err(String)` means a fatal sanitization failure
    fn sanitize(&self, value: &mut T) -> Result<(), String>;
}

///
/// Validator
/// allows a node to validate different types of primitives
/// ?Sized so we can operate on str
///

pub trait Validator<T: ?Sized> {
    fn validate(&self, value: &T, ctx: &mut dyn VisitorContext);
}
