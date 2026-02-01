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
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::EntityFieldKind,
    prelude::*,
    types::Unit,
    value::ValueEnum,
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
/// DataStoreKind
///

pub trait DataStoreKind: Kind {
    type Canister: CanisterKind;
}

///
/// EntityKind
///

pub trait EntityKind: Kind + TypeKind + FieldValues {
    type PrimaryKey: Copy + Into<Key>;
    type DataStore: DataStoreKind;
    type Canister: CanisterKind; // Self::Store::Canister shortcut

    const ENTITY_NAME: &'static str;
    const PRIMARY_KEY: &'static str;
    const FIELDS: &'static [&'static str];
    const INDEXES: &'static [&'static IndexModel];
    const MODEL: &'static crate::model::entity::EntityModel;

    fn key(&self) -> Key;
    fn primary_key(&self) -> Self::PrimaryKey;
    fn set_primary_key(&mut self, key: Self::PrimaryKey);
}

///
/// IndexStoreKind
///

pub trait IndexStoreKind: Kind {
    type Canister: CanisterKind;
}

///
/// UnitKey
/// Marker trait for unit-valued primary keys used by singleton entities.
///

pub trait UnitKey: Copy + Into<Key> + unit_key::Sealed {}

impl UnitKey for () {}
impl UnitKey for Unit {}

mod unit_key {
    use crate::types::Unit;

    // Seal UnitKey so only unit-equivalent key types can implement it.
    pub trait Sealed {}

    impl Sealed for () {}
    impl Sealed for Unit {}
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
/// EntityRef
///
/// Concrete reference extracted from an entity instance.
/// Carries the target entity path and the referenced key value.
/// Produced by [`EntityReferences`] during pre-commit planning.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityRef {
    pub target_path: &'static str,
    pub key: Key,
}

///
/// EntityReferences
///
/// Extract typed entity references from a concrete entity instance.
/// This is a pure helper for pre-commit planning and RI checks.
/// Only direct `Ref<T>` and `Option<Ref<T>>` fields are strong in 0.6.
/// Nested and collection references are treated as weak and ignored.
/// This is a shallow walk over entity fields only; no recursive traversal occurs.
///
pub trait EntityReferences {
    /// Return all concrete references currently present on this entity.
    fn entity_refs(&self) -> Result<Vec<EntityRef>, InternalError>;
}

impl<E> EntityReferences for E
where
    E: EntityKind,
{
    fn entity_refs(&self) -> Result<Vec<EntityRef>, InternalError> {
        let mut refs = Vec::with_capacity(E::MODEL.fields.len());

        for field in E::MODEL.fields {
            // Phase 1: identify strong reference fields; weak shapes are ignored.
            let target_path = match &field.kind {
                &EntityFieldKind::Ref { target_path, .. } => target_path,
                &EntityFieldKind::List(inner) | &EntityFieldKind::Set(inner) => {
                    if matches!(inner, &EntityFieldKind::Ref { .. }) {
                        // Weak references: collection refs are allowed but not validated in 0.6.
                        continue;
                    }
                    continue;
                }
                &EntityFieldKind::Map { key, value } => {
                    if matches!(key, &EntityFieldKind::Ref { .. })
                        || matches!(value, &EntityFieldKind::Ref { .. })
                    {
                        // Weak references: map refs are allowed but not validated in 0.6.
                        continue;
                    }
                    continue;
                }
                _ => continue,
            };

            // Phase 2: fetch the field value and skip absent references.
            let Some(value) = self.get_value(field.name) else {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("reference field missing: {} field={}", E::PATH, field.name),
                ));
            };

            if matches!(value, Value::None) {
                continue;
            }

            if matches!(value, Value::Unsupported) {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "reference field value is unsupported: {} field={}",
                        E::PATH,
                        field.name
                    ),
                ));
            }

            // Phase 3: normalize into a concrete key and record the reference.
            let Some(key) = value.as_key() else {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "reference field value is not a key: {} field={}",
                        E::PATH,
                        field.name
                    ),
                ));
            };

            refs.push(EntityRef { target_path, key });
        }

        Ok(refs)
    }
}

///
/// FieldValue
///
/// Conversion boundary for values used in query predicates.
///
/// `FieldValue` represents any value that can appear on the *right-hand side*
/// of a predicate (e.g. `field == value`, `field IN values`). Implementations
/// convert Rust values into owned [`Value`] instances that are stored inside
/// query plans and executed later.
///

pub trait FieldValue {
    fn to_value(&self) -> Value {
        Value::Unsupported
    }
}

///
/// EnumValue
/// Explicit conversion boundary for domain enums used in query values.
///

pub trait EnumValue {
    /// Convert this enum into a strict [`ValueEnum`] with its canonical path.
    fn to_value_enum(&self) -> ValueEnum;
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

impl<T> FieldValue for &'_ T
where
    T: FieldValue + Copy,
{
    fn to_value(&self) -> Value {
        (*self).to_value()
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

#[cfg(test)]
mod tests;
