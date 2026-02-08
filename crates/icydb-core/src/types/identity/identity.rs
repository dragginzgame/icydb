//! One-way identity projection for external systems.
//!
//! ## Purpose
//! This module defines a **one-way, non-reversible identity projection**
//! intended for use *outside* IcyDB trust boundaries.
//!
//! It is **not** a storage abstraction and does **not** attempt to hide
//! raw primary-key values internally. Instead, it provides:
//!
//! - an opaque, stable external identifier
//! - domain separation across entities and namespaces
//! - protection against accidental key reuse or correlation
//!
//! No API here permits reconstruction of `Id<E>` or recovery of storage keys.

use crate::{
    traits::{EntityIdentity, FieldValue},
    types::Id,
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error as ThisError;

// -----------------------------------------------------------------------------
// Projection framing constants
// -----------------------------------------------------------------------------

/// Domain separator for the projection protocol.
///
/// Bump this if the projection envelope changes incompatibly.
const PROJECTION_DOMAIN_TAG: &[u8] = b"icydb:identity-projection:v2";

/// Framing labels used inside the hash envelope.
///
/// These labels make the hash forward-compatible and unambiguous.
const FRAME_NAMESPACE: &[u8] = b"entity-namespace";
const FRAME_PRIMARY_KEY: &[u8] = b"primary-key";
const FRAME_STORAGE_KEY: &[u8] = b"storage-key-v1";

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

///
/// IdentityProjectionError
///
/// Errors emitted when projecting `Id<E>` into an external identity.
/// These are *boundary errors*, not storage or validation failures.
///
#[derive(Debug, ThisError)]
pub enum IdentityProjectionError {
    /// The entity does not define a stable external namespace.
    #[error("identity namespace is empty for entity '{entity}'")]
    EmptyNamespace { entity: &'static str },

    /// The entity primary key cannot be encoded as a canonical storage key.
    #[error(
        "primary key '{primary_key}' for entity '{entity}' is not storage-key encodable: {value:?}"
    )]
    UnsupportedPrimaryKey {
        entity: &'static str,
        primary_key: &'static str,
        value: Value,
    },

    /// Canonical key-to-bytes encoding failed.
    #[error("failed to encode storage key during identity projection: {reason}")]
    StorageKeyEncoding { reason: String },
}

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
{
    ///
    /// Project this typed identity into deterministic, one-way external bytes.
    ///
    /// ## Envelope structure
    /// The hash input is domain-separated and framed as:
    ///
    /// - projection protocol version
    /// - entity identity namespace
    /// - primary key field name
    /// - canonical storage-key byte encoding
    ///
    /// ## Notes
    /// - This API is intentionally **one-way**
    /// - Projected identities MUST NOT be treated as reversible identifiers
    /// - Different entities or namespaces will never collide
    ///
    pub fn project(&self) -> Result<ProjectedIdentity, IdentityProjectionError> {
        // Phase 1: validate external namespace stability.
        if E::IDENTITY_NAMESPACE.is_empty() {
            return Err(IdentityProjectionError::EmptyNamespace {
                entity: E::ENTITY_NAME,
            });
        }

        // Phase 2: normalize the key through the canonical storage-key boundary.
        let key_value = FieldValue::to_value(&self.key());
        let Some(storage_key) = key_value.as_storage_key() else {
            return Err(IdentityProjectionError::UnsupportedPrimaryKey {
                entity: E::ENTITY_NAME,
                primary_key: E::PRIMARY_KEY,
                value: key_value,
            });
        };

        let key_bytes =
            storage_key
                .to_bytes()
                .map_err(|err| IdentityProjectionError::StorageKeyEncoding {
                    reason: err.to_string(),
                })?;

        // Phase 3: hash a framed envelope for deterministic forward compatibility.
        let mut hasher = Sha256::new();
        hasher.update(PROJECTION_DOMAIN_TAG);

        write_framed(
            &mut hasher,
            FRAME_NAMESPACE,
            E::IDENTITY_NAMESPACE.as_bytes(),
        );
        write_framed(&mut hasher, FRAME_PRIMARY_KEY, E::PRIMARY_KEY.as_bytes());
        write_framed(&mut hasher, FRAME_STORAGE_KEY, &key_bytes);

        Ok(ProjectedIdentity(hasher.finalize().into()))
    }
}

// -----------------------------------------------------------------------------
// Framing helper
// -----------------------------------------------------------------------------

fn write_framed(hasher: &mut Sha256, label: &[u8], bytes: &[u8]) {
    let label_len = u32::try_from(label.len()).unwrap_or(u32::MAX);
    hasher.update(label_len.to_be_bytes());
    hasher.update(label);

    let bytes_len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    hasher.update(bytes_len.to_be_bytes());
    hasher.update(bytes);
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
        const IDENTITY_NAMESPACE: &'static str = "vector/entity/v1";
    }

    struct NamespaceEntity;
    impl EntityKey for NamespaceEntity {
        type Key = Ulid;
    }
    impl EntityIdentity for NamespaceEntity {
        const ENTITY_NAME: &'static str = "NamespaceEntity";
        const PRIMARY_KEY: &'static str = "id";
        const IDENTITY_NAMESPACE: &'static str = "vector/entity/v2";
    }

    struct UnsupportedKeyEntity;
    impl EntityKey for UnsupportedKeyEntity {
        type Key = bool;
    }
    impl EntityIdentity for UnsupportedKeyEntity {
        const ENTITY_NAME: &'static str = "UnsupportedKeyEntity";
        const PRIMARY_KEY: &'static str = "id";
        const IDENTITY_NAMESPACE: &'static str = "unsupported/key/v1";
    }

    #[test]
    fn projection_is_deterministic_for_known_vector() {
        let id = Id::<VectorEntity>::from_storage_key(Ulid::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]));

        let projected = id.project().expect("projection should succeed");
        assert_eq!(
            projected.to_string(),
            "4f4930477e209d911976a1da366ec18ba2d2633845b9b6451fc32e522f8ae619"
        );
    }

    #[test]
    fn projection_changes_when_namespace_changes() {
        let key = Ulid::from_bytes([0x42; 16]);
        let a = Id::<VectorEntity>::from_storage_key(key)
            .project()
            .expect("projection should succeed");
        let b = Id::<NamespaceEntity>::from_storage_key(key)
            .project()
            .expect("projection should succeed");

        assert_ne!(a, b);
    }

    #[test]
    fn projection_rejects_non_keyable_primary_keys() {
        let id = Id::<UnsupportedKeyEntity>::from_storage_key(true);
        let err = id.project().expect_err("bool key should not project");

        assert!(matches!(
            err,
            super::IdentityProjectionError::UnsupportedPrimaryKey { .. }
        ));
    }
}
