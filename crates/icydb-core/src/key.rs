use crate::{
    traits::FieldValue,
    types::{Account, Principal, Subaccount, Timestamp, Ulid, Unit},
    value::Value,
};
use candid::{CandidType, Principal as WrappedPrincipal};
use canic::impl_storable_bounded;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

///
/// Key
///
/// Treating IndexKey as the atomic, normalized unit of the keyspace
/// Backing primary keys and secondary indexes with the same value representation
/// Planning to enforce Copy semantics (i.e., fast, clean, safe)
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Display, Eq, Hash, PartialEq, Serialize)]
pub enum Key {
    Account(Account),
    Int(i64),
    Principal(Principal),
    Subaccount(Subaccount),
    Timestamp(Timestamp),
    Uint(u64),
    Ulid(Ulid),
    Unit,
}

impl Key {
    pub const STORABLE_MAX_SIZE: u32 = 128;

    #[must_use]
    pub fn max_storable() -> Self {
        Self::Account(Account::max_storable())
    }

    #[must_use]
    pub const fn lower_bound() -> Self {
        Self::Int(i64::MIN)
    }

    #[must_use]
    pub const fn upper_bound() -> Self {
        Self::Unit
    }

    const fn variant_rank(&self) -> u8 {
        match self {
            Self::Account(_) => 0,
            Self::Int(_) => 1,
            Self::Principal(_) => 2,
            Self::Subaccount(_) => 3,
            Self::Timestamp(_) => 4,
            Self::Uint(_) => 5,
            Self::Ulid(_) => 6,
            Self::Unit => 7,
        }
    }
}

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

/// Implements `From<T> for Key` for simple conversions
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

/// Implements symmetric PartialEq between Key and another type
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

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Account(a), Self::Account(b)) => Ord::cmp(a, b),
            (Self::Int(a), Self::Int(b)) => Ord::cmp(a, b),
            (Self::Principal(a), Self::Principal(b)) => Ord::cmp(a, b),
            (Self::Uint(a), Self::Uint(b)) => Ord::cmp(a, b),
            (Self::Ulid(a), Self::Ulid(b)) => Ord::cmp(a, b),
            (Self::Subaccount(a), Self::Subaccount(b)) => Ord::cmp(a, b),
            (Self::Timestamp(a), Self::Timestamp(b)) => Ord::cmp(a, b),

            _ => Ord::cmp(&self.variant_rank(), &other.variant_rank()), // fallback for cross-type comparison
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl_storable_bounded!(Key, Self::STORABLE_MAX_SIZE, false);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Storable;

    #[test]
    fn key_max_size_is_bounded() {
        let key = Key::max_storable();
        let size = Storable::to_bytes(&key).len();

        assert!(
            size <= Key::STORABLE_MAX_SIZE as usize,
            "serialized Key too large: got {size} bytes (limit {})",
            Key::STORABLE_MAX_SIZE
        );
    }
}
