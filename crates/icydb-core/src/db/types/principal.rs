use crate::{db::traits::FromKey, key::Key, types::Principal};

///
/// Principal
///

impl FromKey for Principal {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Principal(v) => Some(v),
            _ => None,
        }
    }
}
