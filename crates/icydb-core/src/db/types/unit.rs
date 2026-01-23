use crate::{db::traits::FromKey, key::Key, types::Unit};

///
/// Unit
///

impl FromKey for Unit {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Unit => Some(Self),
            _ => None,
        }
    }
}
