use crate::{db::primitives::NoFilterKind, traits::Filterable, types::Unit};

impl Filterable for Unit {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}
