use crate::{
    db::{
        primitives::{TextFilterKind, TextListFilterKind},
        traits::FromKey,
    },
    key::Key,
    traits::Filterable,
    types::Account,
};

///
/// Account
///

impl Filterable for Account {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
}

impl FromKey for Account {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Account(v) => Some(v),
            _ => None,
        }
    }
}
