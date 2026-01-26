use crate::{
    key::Key,
    traits::FieldValue,
    types::{Account, Principal, Subaccount, Timestamp, Ulid, Unit},
    value::Value,
};
use candid::Principal as WrappedPrincipal;

impl FieldValue for Key {
    fn to_value(&self) -> Value {
        match self {
            Self::Account(v) => Value::Account(*v),
            Self::Int(v) => Value::Int(*v),
            Self::Principal(v) => Value::Principal(*v),
            Self::Subaccount(v) => Value::Subaccount(*v),
            Self::Timestamp(v) => Value::Timestamp(*v),
            Self::Uint(v) => Value::Uint(*v),
            Self::Ulid(v) => Value::Ulid(*v),
            Self::Unit => Value::Unit,
        }
    }
}

impl From<()> for Key {
    fn from((): ()) -> Self {
        Self::Unit
    }
}

impl From<Unit> for Key {
    fn from(_: Unit) -> Self {
        Self::Unit
    }
}

impl PartialEq<()> for Key {
    fn eq(&self, (): &()) -> bool {
        matches!(self, Self::Unit)
    }
}

impl PartialEq<Key> for () {
    fn eq(&self, other: &Key) -> bool {
        other == self
    }
}

/// Implements `From<T> for Key` for simple conversions.
macro_rules! impl_from_key {
    ( $( $ty:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl From<$ty> for Key {
                fn from(v: $ty) -> Self {
                    Self::$variant(v.into())
                }
            }
        )*
    }
}

/// Implements symmetric PartialEq between Key and another type.
macro_rules! impl_eq_key {
    ( $( $ty:ty => $variant:ident ),* $(,)? ) => {
        $(
            impl PartialEq<$ty> for Key {
                fn eq(&self, other: &$ty) -> bool {
                    matches!(self, Self::$variant(val) if val == other)
                }
            }

            impl PartialEq<Key> for $ty {
                fn eq(&self, other: &Key) -> bool {
                    other == self
                }
            }
        )*
    }
}

impl_from_key! {
    Account => Account,
    i8  => Int,
    i16 => Int,
    i32 => Int,
    i64 => Int,
    Principal => Principal,
    WrappedPrincipal => Principal,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u8  => Uint,
    u16 => Uint,
    u32 => Uint,
    u64 => Uint,
    Ulid => Ulid,
}

impl_eq_key! {
    Account => Account,
    i64 => Int,
    Principal => Principal,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u64  => Uint,
    Ulid => Ulid,
}
