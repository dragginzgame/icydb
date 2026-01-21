use crate::{
    db::primitives::{
        BoolEqualityFilterKind, BoolListFilterKind, FilterKind, Int64RangeFilterKind,
        IntListFilterKind, IntoFilterExpr, Nat64RangeFilterKind, NatListFilterKind, TextFilterKind,
        TextListFilterKind,
    },
    key::Key,
};

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
/// FilterView
///

pub trait FilterView {
    type FilterViewType: Default + IntoFilterExpr;
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
            impl $crate::db::traits::FromKey for $ty {
                fn try_from_key(key: $crate::key::Key) -> Option<Self> {
                    match key {
                        $crate::key::Key::Int(v) => Self::try_from(v).ok(),
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
            impl $crate::db::traits::FromKey for $ty {
                fn try_from_key(key: $crate::key::Key) -> Option<Self> {
                    match key {
                        $crate::key::Key::Uint(v) => Self::try_from(v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_from_key_int!(i8, i16, i32, i64);
impl_from_key_uint!(u8, u16, u32, u64);
