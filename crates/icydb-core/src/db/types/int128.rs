use crate::{
    db::primitives::{IntListFilterKind, IntRangeFilterKind},
    traits::Filterable,
    types::Int128,
};

impl Filterable for Int128 {
    type Filter = IntRangeFilterKind;
    type ListFilter = IntListFilterKind;
}
