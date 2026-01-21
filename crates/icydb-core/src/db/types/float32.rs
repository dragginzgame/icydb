use crate::{
    db::primitives::{DecimalListFilterKind, DecimalRangeFilterKind},
    traits::Filterable,
    types::Float32,
};

impl Filterable for Float32 {
    type Filter = DecimalRangeFilterKind;
    type ListFilter = DecimalListFilterKind;
}
