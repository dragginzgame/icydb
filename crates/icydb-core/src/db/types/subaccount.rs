use crate::{db::traits::FromKey, key::Key, types::Subaccount};

///
/// Subaccount
///

impl FromKey for Subaccount {
    fn try_from_key(key: Key) -> Option<Self> {
        match key {
            Key::Subaccount(v) => Some(v),
            _ => None,
        }
    }
}
