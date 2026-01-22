use crate::{
    db::{primitives::NoFilterKind, traits::FromKey},
    key::Key,
    traits::Filterable,
    types::Subaccount,
};

///
/// Subaccount
///

impl Filterable for Subaccount {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}

impl FromKey for Subaccount {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Subaccount(v) => Some(v),
            _ => None,
        }
    }
}
