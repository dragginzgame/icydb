//! Module: index::key::codec::envelope
//! Responsibility: raw-key storable boundary helpers.
//! Does not own: index-key parsing rules.
//! Boundary: stable-memory storage adapter for `RawIndexKey`.

use crate::{
    db::index::key::codec::{IndexKey, RawIndexKey},
    traits::Storable,
};
use canic_cdk::structures::storable::Bound;
use std::borrow::Cow;

impl RawIndexKey {
    /// Borrow the raw byte representation.
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Storable for RawIndexKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    #[expect(clippy::cast_possible_truncation)]
    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::STORED_SIZE_BYTES as u32,
        is_fixed_size: false,
    };
}
