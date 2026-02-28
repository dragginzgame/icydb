//! Module: index::key::codec::scalar
//! Responsibility: scalar tag encoding for index key namespaces.
//! Does not own: full key framing.
//! Boundary: consumed by codec encode/decode.

use crate::db::index::key::codec::error::ERR_INVALID_KEY_KIND;

///
/// IndexKeyKind
///
/// Encoded discriminator for index key families.
///
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(crate) enum IndexKeyKind {
    User = 0,
    System = 1,
}

impl IndexKeyKind {
    const USER_TAG: u8 = 0;
    const SYSTEM_TAG: u8 = 1;

    #[must_use]
    pub(super) const fn tag(self) -> u8 {
        self as u8
    }

    pub(super) const fn from_tag(tag: u8) -> Result<Self, &'static str> {
        match tag {
            Self::USER_TAG => Ok(Self::User),
            Self::SYSTEM_TAG => Ok(Self::System),
            _ => Err(ERR_INVALID_KEY_KIND),
        }
    }
}
