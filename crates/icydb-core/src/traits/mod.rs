#[macro_use]
mod macros;
mod atomic;
mod view;
mod visitor;

pub use atomic::*;
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

use crate::{prelude::*, types::Id, value::ValueEnum, visitor::VisitorContext};

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

/// Marker for all schema/runtime nodes.
pub trait Kind: Path + 'static {}
impl<T> Kind for T where T: Path + 'static {}

/// Marker for canister namespaces.
pub trait CanisterKind: Kind {}

/// Marker for data stores bound to a canister.
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
    type Key: Copy + Debug + Eq + Ord + FieldValue + EntityKeyBytes + 'static;
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
/// EntityIdentity
///
/// Semantic primary-key metadata about an entity.
///
/// These constants name identity metadata only. They do not imply trust, ownership,
/// authorization, or existence.
///

pub trait EntityIdentity: EntityKey {
    const ENTITY_NAME: &'static str;
    const PRIMARY_KEY: &'static str;
}

///
/// EntitySchema
///
/// Declared schema facts for an entity.
///

pub trait EntitySchema: EntityIdentity {
    const MODEL: &'static EntityModel;
    const FIELDS: &'static [&'static str];
    const INDEXES: &'static [&'static IndexModel];
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

pub trait EntityKind: EntitySchema + EntityPlacement + Kind + TypeKind {}

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

pub trait EntityValue: EntityIdentity + FieldProjection + Sized {
    fn id(&self) -> Id<Self>;
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
    Kind
    + AsView
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
        + AsView
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
/// Collection
///
/// Explicit iteration contract for list/set wrapper types.
/// Avoids implicit deref-based access to inner collections.
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
/// Avoids implicit deref-based access to inner collections.
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

pub trait EnumValue {
    fn to_value_enum(&self) -> ValueEnum;
}

pub trait FieldProjection {
    /// Resolve one field value by stable field slot index.
    fn get_value_by_index(&self, index: usize) -> Option<Value>;
}

///
/// FieldValueKind
///
/// Schema affordance classification for query planning and validation.
/// Describes whether a field is planner-addressable and predicate-queryable.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldValueKind {
    /// Planner-addressable atomic value.
    Atomic,

    /// Structured value with known internal fields that the planner
    /// does not reason about as an addressable query target.
    Structured {
        /// Whether predicates may be expressed against this field.
        queryable: bool,
    },
}

impl FieldValueKind {
    #[must_use]
    pub const fn is_queryable(self) -> bool {
        match self {
            Self::Atomic => true,
            Self::Structured { queryable } => queryable,
        }
    }
}

///
/// FieldValue
///
/// Conversion boundary for values used in query predicates.
///
/// Represents values that can appear on the *right-hand side* of predicates.
///

pub trait FieldValue {
    fn kind() -> FieldValueKind
    where
        Self: Sized;

    fn to_value(&self) -> Value;

    #[must_use]
    fn from_value(value: &Value) -> Option<Self>
    where
        Self: Sized;
}

impl FieldValue for &str {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Text((*self).to_string())
    }

    fn from_value(_value: &Value) -> Option<Self> {
        None
    }
}

impl FieldValue for String {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Text(self.clone())
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Text(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl<T: FieldValue> FieldValue for Option<T> {
    fn kind() -> FieldValueKind {
        T::kind()
    }

    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }

    fn from_value(value: &Value) -> Option<Self> {
        if matches!(value, Value::Null) {
            return Some(None);
        }

        T::from_value(value).map(Some)
    }
}

impl<T: FieldValue> FieldValue for Box<T> {
    fn kind() -> FieldValueKind {
        T::kind()
    }

    fn to_value(&self) -> Value {
        (**self).to_value()
    }

    fn from_value(value: &Value) -> Option<Self> {
        T::from_value(value).map(Self::new)
    }
}

impl<T: FieldValue> FieldValue for Vec<Box<T>> {
    fn kind() -> FieldValueKind {
        FieldValueKind::Structured { queryable: true }
    }

    fn to_value(&self) -> Value {
        Value::List(self.iter().map(FieldValue::to_value).collect())
    }

    fn from_value(value: &Value) -> Option<Self> {
        let Value::List(items) = value else {
            return None;
        };

        let mut out = Self::with_capacity(items.len());
        for item in items {
            out.push(Box::new(T::from_value(item)?));
        }

        Some(out)
    }
}

// impl_field_value
#[macro_export]
macro_rules! impl_field_value {
    ( $( $type:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl FieldValue for $type {
                fn kind() -> FieldValueKind {
                    FieldValueKind::Atomic
                }

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
