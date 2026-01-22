use crate::{
    db::primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
    traits::Filterable,
    types::Duration,
};

///
/// Duration
///

impl Filterable for Duration {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}
