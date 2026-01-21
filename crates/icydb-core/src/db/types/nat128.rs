use crate::{
    db::primitives::{NatListFilterKind, NatRangeFilterKind},
    traits::Filterable,
    types::Nat128,
};

impl Filterable for Nat128 {
    type Filter = NatRangeFilterKind;
    type ListFilter = NatListFilterKind;
}
