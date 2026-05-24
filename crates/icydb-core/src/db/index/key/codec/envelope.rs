//! Module: index::key::codec::envelope
//! Responsibility: raw-key storable boundary helpers.
//! Does not own: index-key parsing rules.
//! Boundary: stable-memory storage adapter for `RawIndexStoreKey`.

use crate::{
    db::index::key::codec::{IndexKey, RawIndexStoreKey},
    traits::Storable,
};
use ic_memory::stable_structures::storable::Bound;
use std::borrow::Cow;

impl Storable for RawIndexStoreKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self::from_persisted_bytes(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }

    #[expect(clippy::cast_possible_truncation)]
    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::MAX_STORED_SIZE_BYTES as u32,
        is_fixed_size: false,
    };
}
