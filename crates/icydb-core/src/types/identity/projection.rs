//! Module: types::identity::projection
//! Defines the deterministic public identity projection derived from
//! canonical entity key bytes.

//! Deterministic identity projection for external systems.
//!
//! ## Purpose
//! This module maps canonical key bytes to a public, unkeyed SHA-256 digest.
//! Candidate keys can be matched by projecting them, so enumerable or public
//! key domains do not become secret. Equal canonical key bytes produce equal
//! projections across entities: the protocol tag does not include entity identity.
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
    db::{EntityKey, EntityKeyBytes, EntityKeyBytesError},
    types::Id,
};
use candid::CandidType;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt;

// -----------------------------------------------------------------------------
// Projection constants
// -----------------------------------------------------------------------------

// Domain separator for the projection protocol.
//
// Pre-1.0.0 protocol changes hard-cut the active tag instead of preserving
// multiple internal projection versions in parallel.
const PROJECTION_DOMAIN_TAG: &[u8] = b"icydb:identity-projection:v1";

// -----------------------------------------------------------------------------
// Projected identity
// -----------------------------------------------------------------------------

//
// ProjectedIdentity
//
// Stable output of deterministic identity projection.
//
// ## Guarantees
// - Deterministic
// - Equal canonical key bytes produce equal projections across entities
//
// ## Non-goals
// - Does NOT provide secrecy, authentication, or authorization
// - Does NOT imply ownership
// - Does NOT preserve ordering
// - Guessable source keys can be recovered by projecting candidates
// - Does NOT imply entity existence
//

/// Public SHA-256 projection of the protocol tag and canonical key bytes.
///
/// This unkeyed mapping preserves cross-entity correlation for equal key bytes.
/// Enumerable source keys can be matched by projecting candidates.
#[repr(transparent)]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    E: EntityKey,
    E::Key: EntityKeyBytes,
{
    /// Derive a deterministic external identifier from canonical key bytes.
    ///
    /// This method is a mechanical mapping only. It does not verify authorization, ownership,
    /// or entity existence.
    /// The unkeyed digest includes the protocol tag and key bytes, with no entity
    /// identity. Guessable keys can be matched by projecting candidate values.
    ///
    /// # Errors
    ///
    /// Returns [`EntityKeyBytesError`] when the key cannot produce its
    /// declared fixed-width representation.
    pub fn project(&self) -> Result<ProjectedIdentity, EntityKeyBytesError> {
        let mut hasher = Sha256::new();
        hasher.update(PROJECTION_DOMAIN_TAG);

        // Canonical key bytes (ULID, UUID, etc.)
        let mut key_buf = vec![0u8; E::Key::BYTE_LEN];
        self.key().write_bytes(&mut key_buf)?;
        hasher.update(&key_buf);

        Ok(ProjectedIdentity(hasher.finalize().into()))
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::{
        db::EntityKey,
        types::{Id, Ulid},
    };

    struct VectorEntity;
    impl EntityKey for VectorEntity {
        type Key = Ulid;
    }

    struct OtherEntity;
    impl EntityKey for OtherEntity {
        type Key = Ulid;
    }

    #[test]
    fn projection_is_deterministic_for_known_vector() {
        let id = Id::<VectorEntity>::from_key(Ulid::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]));

        let projected = id.project().expect("valid ULID key should project");
        assert_eq!(
            projected.to_string(),
            "2da507e94f2d3dcb66fad5f74338d3fdf31f7b8a0e56e5888078f0c91a5b643f"
        );
    }

    #[test]
    fn projection_is_stable_across_entities_for_same_key_bytes() {
        let key = Ulid::from_bytes([0x42; 16]);
        let a = Id::<VectorEntity>::from_key(key)
            .project()
            .expect("valid ULID key should project");
        let b = Id::<OtherEntity>::from_key(key)
            .project()
            .expect("valid ULID key should project");

        assert_eq!(a, b);
    }
}
