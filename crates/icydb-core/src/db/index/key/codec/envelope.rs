//! Module: index::key::codec::envelope
//! Responsibility: raw-key storable boundary helpers.
//! Does not own: index-key parsing rules.
//! Boundary: stable-memory storage adapter for `RawIndexStoreKey`.

use crate::db::index::key::codec::{IndexKey, RawIndexStoreKey};
use ic_storage::adapter::{Storable, storable::Bound};
use std::borrow::Cow;

impl Storable for RawIndexStoreKey {
    fn to_bytes(&self) -> Result<Cow<'_, [u8]>, ic_storage::storable::CodecError> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Result<Self, ic_storage::storable::CodecError> {
        Self::check_bounds(bytes.as_ref())?;
        Ok(Self::from_persisted_bytes(bytes.into_owned()))
    }

    fn into_bytes(self) -> Result<Vec<u8>, ic_storage::storable::CodecError> {
        Ok(self.into_bytes())
    }

    #[expect(clippy::cast_possible_truncation)]
    const BOUND: Bound = Bound::Bounded {
        max_size: IndexKey::MAX_STORED_SIZE_BYTES as u32,
        is_fixed_size: false,
    };
}
