use crate::{
    db::primitives::{IntListFilterKind, IntRangeFilterKind},
    traits::Filterable,
    types::Int,
};

impl Filterable for Int {
    type Filter = IntRangeFilterKind;
    type ListFilter = IntListFilterKind;
}
