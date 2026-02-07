//! Identity projection utilities.
//!
//! WARNING:
//! Identity projections MUST be one-way.
//! No API in this module may expose raw storage keys
//! or allow reconstruction of `Id<T>`.

use crate::{
    traits::{EntityIdentity, EntityStorageKey},
    types::{Id, Ulid, Unit},
};
use sha2::{Digest, Sha256};

mod sealed {
    pub trait Sealed {}
}

/// One-way projection of an entity identity into stable bytes.
///
/// This is intended for:
/// - ledger subaccounts
/// - partition keys
/// - deterministic external identifiers
///
/// It MUST NOT be used to reconstitute identity.
pub trait IdentityProjection: sealed::Sealed {
    /// Project identity into a stable, opaque byte representation.
    fn project_bytes(&self) -> [u8; 32];
}

impl<E> sealed::Sealed for Id<E> where E: EntityStorageKey {}

impl<E> IdentityProjection for Id<E>
where
    E: EntityIdentity,
    E::Key: StorageKeyBytes,
{
    fn project_bytes(&self) -> [u8; 32] {
        // Domain-separate identity projection by entity name to avoid
        // cross-entity collisions for identical storage key bytes.
        const DOMAIN_TAG: &[u8] = b"icydb:id-projection:v1";

        // INTERNAL: raw key access is allowed only at this boundary.
        let key = self.storage_key();

        let mut hasher = Sha256::new();
        hasher.update(DOMAIN_TAG);
        hasher.update([0x00]);
        hasher.update(E::ENTITY_NAME.as_bytes());
        hasher.update([0x00]);
        key.with_stable_bytes(|bytes| hasher.update(bytes));

        hasher.finalize().into()
    }
}

/// Internal trait for stable byte access to storage keys.
///
/// This is NOT a general-purpose conversion trait.
/// It exists only for identity projection internals.
pub trait StorageKeyBytes {
    fn with_stable_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R;
}

impl StorageKeyBytes for Ulid {
    fn with_stable_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        let bytes = self.to_bytes();
        f(&bytes)
    }
}

impl StorageKeyBytes for Unit {
    fn with_stable_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&[])
    }
}

impl StorageKeyBytes for () {
    fn with_stable_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&[])
    }
}
