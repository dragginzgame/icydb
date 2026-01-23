use crate::{db::traits::FromKey, key::Key, types::Account};

///
/// Account
///

impl FromKey for Account {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Account(v) => Some(v),
            _ => None,
        }
    }
}
