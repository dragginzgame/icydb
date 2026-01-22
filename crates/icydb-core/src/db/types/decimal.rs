use crate::{
    db::primitives::{DecimalListFilterKind, DecimalRangeFilterKind},
    traits::Filterable,
    types::Decimal,
};

///
/// Decimal
///

impl Filterable for Decimal {
    type Filter = DecimalRangeFilterKind;
    type ListFilter = DecimalListFilterKind;
}
