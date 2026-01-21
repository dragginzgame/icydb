use crate::{db::primitives::NoFilterKind, traits::Filterable, types::Blob};

impl Filterable for Blob {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}
