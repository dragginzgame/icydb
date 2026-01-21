use crate::{
    db::primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
    traits::Filterable,
    types::Timestamp,
};

impl Filterable for Timestamp {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}
