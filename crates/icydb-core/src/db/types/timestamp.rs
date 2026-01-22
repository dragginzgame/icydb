use crate::{
    db::{
        primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
        traits::FromKey,
    },
    key::Key,
    traits::Filterable,
    types::Timestamp,
};

///
/// Timestamp
///

impl Filterable for Timestamp {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}

impl FromKey for Timestamp {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Timestamp(v) => Some(v),
            _ => None,
        }
    }
}
