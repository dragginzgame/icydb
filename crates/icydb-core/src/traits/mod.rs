#[macro_use]
mod macros;
mod view;
mod visitor;

pub use view::*;
pub use visitor::*;

// -----------------------------------------------------------------------------
// Standard re-exports for `traits::X` ergonomics
// -----------------------------------------------------------------------------

pub use canic_cdk::structures::storable::Storable;
pub use num_traits::{FromPrimitive as NumFromPrimitive, NumCast, ToPrimitive as NumToPrimitive};
pub use serde::{Deserialize, Serialize, de::DeserializeOwned};
pub use std::{
    cmp::{Eq, Ordering, PartialEq},
    convert::From,
    default::Default,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign, Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
};

use crate::{prelude::*, value::ValueEnum, visitor::VisitorContext};

/// ============================================================================
/// KIND HIERARCHY
/// ============================================================================
///
/// In schema terminology these are "nodes".
/// In runtime terminology these are "kinds".
///

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
/// DataStoreKind
///
pub trait DataStoreKind: Kind {
    type Canister: CanisterKind;
}

///
/// IndexStoreKind
///
pub trait IndexStoreKind: Kind {
    type Canister: CanisterKind;
}

/// ============================================================================
/// IDENTITY TRAITS
/// ============================================================================
///
/// Identity traits describe *who* an entity is, not how it is stored.
/// No persistence assumptions are allowed here.
///

///
/// EntityKey
///
/// Marker trait for entity identity types.
///
/// Identity types must be:
/// - Copy
/// - Comparable
///
/// They are NOT required to be persistable.
///
pub trait EntityKey: Copy + Debug + Eq + Ord + FieldValue + 'static {}

impl<T> EntityKey for T where T: Copy + Debug + Eq + Ord + FieldValue + 'static {}

///
/// EntityKind
///
/// Describes a concrete entity type.
///
/// This trait binds together:
/// - Identity (`Id`)
/// - Schema metadata
/// - Store and canister placement
///
/// It intentionally does NOT imply how the ID is stored.
///

pub trait EntityKind: Kind + TypeKind {
    /// Entity primary key type.
    ///
    /// Invariants:
    /// - Must be representable as a `Value`
    /// - Must be totally ordered for range validation
    /// - Must be schema-compatible with the declared primary key field
    type Id: EntityKey;
    type DataStore: DataStoreKind;
    type Canister: CanisterKind;

    const ENTITY_NAME: &'static str;
    const PRIMARY_KEY: &'static str;
    const FIELDS: &'static [&'static str];
    const INDEXES: &'static [&'static IndexModel];
    const MODEL: &'static EntityModel;
}

///
/// EntityValue
///

pub trait EntityValue: EntityKind + FieldValues {
    fn id(&self) -> Self::Id;
    fn set_id(&mut self, id: Self::Id);
}

/// ============================================================================
/// PERSISTENCE CAPABILITIES
/// ============================================================================
///
/// These traits explicitly grant permission to cross into storage concerns.
///

///
/// SingletonEntity
/// Entity with exactly one logical row.
///

pub trait SingletonEntity: EntityValue {}

/// ============================================================================
/// TYPE SYSTEM
/// ============================================================================

///
/// TypeKind
///
/// Any schema-defined data type.
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

/// ============================================================================
/// QUERY VALUE BOUNDARIES
/// ============================================================================

///
/// EnumValue
///
/// Explicit conversion boundary for domain enums.
///

pub trait EnumValue {
    fn to_value_enum(&self) -> ValueEnum;
}

///
/// FieldValues
///
/// Read access to entity fields by name.
///
pub trait FieldValues {
    fn get_value(&self, field: &str) -> Option<Value>;
}

///
/// CollectionValue
///
/// Explicit iteration contract for list/set wrapper types.
/// Avoids implicit deref-based access to inner collections.
///
pub trait CollectionValue {
    type Item;

    fn iter(&self) -> impl Iterator<Item = &Self::Item>;
    fn len(&self) -> usize;
}

///
/// FieldValue
///
/// Conversion boundary for values used in query predicates.
///
/// Represents values that can appear on the *right-hand side* of predicates.
///
pub trait FieldValue {
    fn to_value(&self) -> Value {
        Value::Unsupported
    }

    #[must_use]
    fn from_value(_value: &Value) -> Option<Self>
    where
        Self: Sized,
    {
        None
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
/// Fully-qualified schema path.
///
pub trait Path {
    const PATH: &'static str;
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
}

///
/// Validator
///
/// Allows a node to validate values.
///
pub trait Validator<T: ?Sized> {
    fn validate(&self, value: &T, ctx: &mut dyn VisitorContext);
}
