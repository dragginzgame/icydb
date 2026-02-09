//! One-way identity projection for external systems.
//!
//! ## Purpose
//! This module defines a **one-way, non-reversible identity projection**
//! for deterministic external identifier mapping.
//!
//! It is **not** a storage abstraction and does **not** establish trust.
//! Projection is a mechanical derivation from canonical key bytes. It provides:
//!
//! - an opaque, stable external identifier
//! - domain separation via the projection protocol tag
//! - deterministic projection from canonical key bytes
//!
//! Projection does **not** provide:
//! - secrecy
//! - authentication
//! - authorization
//! - proof of ownership or existence
//!
//! Projected values are public identifiers and must be treated as untrusted input until verified
//! in context.

use crate::{
    traits::{EntityIdentity, EntityKeyBytes},
    types::Id,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

// -----------------------------------------------------------------------------
// Projection constants
// -----------------------------------------------------------------------------

/// Domain separator for the projection protocol.
///
/// Bump this if the projection envelope changes incompatibly.
const PROJECTION_DOMAIN_TAG: &[u8] = b"icydb:identity-projection:v2";

// -----------------------------------------------------------------------------
// Projected identity
// -----------------------------------------------------------------------------

///
/// ProjectedIdentity
///
/// Stable, opaque output of one-way identity projection.
///
/// ## Guarantees
/// - Deterministic
/// - Non-reversible
/// - Safe to expose externally
///
/// ## Non-goals
/// - Does NOT provide secrecy, authentication, or authorization
/// - Does NOT imply ownership
/// - Does NOT preserve ordering
/// - Does NOT permit identity reconstruction
/// - Does NOT imply entity existence
///

#[repr(transparent)]
#[derive(
    CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct ProjectedIdentity([u8; 32]);

impl ProjectedIdentity {
    /// Borrow the projected identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consume and return the projected identity bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Display for ProjectedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Projection implementation
// -----------------------------------------------------------------------------

impl<E> Id<E>
where
    E: EntityIdentity,
    E::Key: EntityKeyBytes,
{
    /// Derive a deterministic, one-way external identifier from canonical key bytes.
    ///
    /// This method is a mechanical mapping only. It does not verify authorization, ownership,
    /// or entity existence.
    pub fn project(&self) -> ProjectedIdentity {
        let mut hasher = Sha256::new();
        hasher.update(PROJECTION_DOMAIN_TAG);

        // Canonical key bytes (ULID, UUID, etc.)
        let mut key_buf = vec![0u8; E::Key::BYTE_LEN];
        self.key().write_bytes(&mut key_buf);
        hasher.update(&key_buf);

        ProjectedIdentity(hasher.finalize().into())
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::{
        traits::{EntityIdentity, EntityKey},
        types::{Id, Ulid},
    };

    struct VectorEntity;
    impl EntityKey for VectorEntity {
        type Key = Ulid;
    }
    impl EntityIdentity for VectorEntity {
        const ENTITY_NAME: &'static str = "VectorEntity";
        const PRIMARY_KEY: &'static str = "id";
    }

    struct OtherEntity;
    impl EntityKey for OtherEntity {
        type Key = Ulid;
    }
    impl EntityIdentity for OtherEntity {
        const ENTITY_NAME: &'static str = "OtherEntity";
        const PRIMARY_KEY: &'static str = "id";
    }

    #[test]
    fn projection_is_deterministic_for_known_vector() {
        let id = Id::<VectorEntity>::from_key(Ulid::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]));

        let projected = id.project();
        assert_eq!(
            projected.to_string(),
            "aff2dcfa48e7868d2177d30d963d8926b82ffd7c822e22d6c4e42514d6ffa890"
        );
    }

    #[test]
    fn projection_is_stable_across_entities_for_same_key_bytes() {
        let key = Ulid::from_bytes([0x42; 16]);
        let a = Id::<VectorEntity>::from_key(key).project();
        let b = Id::<OtherEntity>::from_key(key).project();

        assert_eq!(a, b);
    }
}
