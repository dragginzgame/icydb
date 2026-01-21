use crate::{
    db::primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
    traits::Filterable,
    types::E8s,
};

impl Filterable for E8s {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}
