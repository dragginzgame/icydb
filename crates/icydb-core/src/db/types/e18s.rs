use crate::{
    db::primitives::{NatListFilterKind, NatRangeFilterKind},
    traits::Filterable,
    types::E18s,
};

impl Filterable for E18s {
    type Filter = NatRangeFilterKind;
    type ListFilter = NatListFilterKind;
}
