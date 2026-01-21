use crate::{
    db::{primitives::NoFilterKind, traits::FromKey},
    key::Key,
    traits::Filterable,
    types::Unit,
};

impl Filterable for Unit {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
}

impl FromKey for Unit {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Unit => Some(Self),
            _ => None,
        }
    }
}
