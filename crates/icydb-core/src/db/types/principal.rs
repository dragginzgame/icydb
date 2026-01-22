use crate::{
    db::{
        primitives::filter::{TextEqualityFilterKind, TextListFilterKind},
        traits::FromKey,
    },
    key::Key,
    traits::Filterable,
    types::Principal,
};

///
/// Principal
///

impl Filterable for Principal {
    type Filter = TextEqualityFilterKind;
    type ListFilter = TextListFilterKind;
}

impl FromKey for Principal {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Principal(v) => Some(v),
            _ => None,
        }
    }
}
