use crate::{db::traits::FromKey, key::Key, types::Ulid};

///
/// Ulid
///

impl FromKey for Ulid {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Ulid(v) => Some(v),
            _ => None,
        }
    }
}
