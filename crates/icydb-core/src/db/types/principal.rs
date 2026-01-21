use crate::{
    db::primitives::filter::{TextEqualityFilterKind, TextListFilterKind},
    traits::Filterable,
    types::Principal,
};

impl Filterable for Principal {
    type Filter = TextEqualityFilterKind;
    type ListFilter = TextListFilterKind;
}
