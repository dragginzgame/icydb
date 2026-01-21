use crate::{db::primitives::NoFilterKind, traits::Filterable, types::Subaccount};

impl Filterable for Subaccount {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}
