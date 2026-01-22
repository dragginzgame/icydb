use crate::{
    db::primitives::{IntListFilterKind, IntRangeFilterKind},
    traits::Filterable,
    types::Int,
};

///
/// Int
///

impl Filterable for Int {
    type Filter = IntRangeFilterKind;
    type ListFilter = IntListFilterKind;
}
