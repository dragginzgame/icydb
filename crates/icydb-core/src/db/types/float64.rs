use crate::{
    db::primitives::{DecimalListFilterKind, DecimalRangeFilterKind},
    traits::Filterable,
    types::Float64,
};

impl Filterable for Float64 {
    type Filter = DecimalRangeFilterKind;
    type ListFilter = DecimalListFilterKind;
}
