use crate::{db::traits::FromKey, key::Key, types::Timestamp};

///
/// Timestamp
///

impl FromKey for Timestamp {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Timestamp(v) => Some(v),
            _ => None,
        }
    }
}
