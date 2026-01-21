use crate::{
    db::primitives::{Int64ListFilterKind, Int64RangeFilterKind},
    traits::Filterable,
    types::Date,
};

impl Filterable for Date {
    type Filter = Int64RangeFilterKind;
    type ListFilter = Int64ListFilterKind;
}
