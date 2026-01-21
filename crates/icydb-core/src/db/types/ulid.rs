use crate::{
    db::{
        primitives::{TextFilterKind, TextListFilterKind},
        traits::FromKey,
    },
    key::Key,
    traits::Filterable,
    types::Ulid,
};

impl Filterable for Ulid {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
}

impl FromKey for Ulid {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Ulid(v) => Some(v),
            _ => None,
        }
    }
}
