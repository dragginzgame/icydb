use crate::{
    db::primitives::{NatListFilterKind, NatRangeFilterKind},
    traits::Filterable,
    types::Nat,
};

impl Filterable for Nat {
    type Filter = NatRangeFilterKind;
    type ListFilter = NatListFilterKind;
}
