//! Schema mutation audit and runtime identity fingerprints.

use super::{SchemaMutationRunnerInput, SchemaMutationStoreVisibility};
use crate::{
    db::{
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u32,
        },
        schema::{PersistedSchemaSnapshot, SchemaVersion, encode_persisted_schema_snapshot},
    },
    error::InternalError,
};
use sha2::Digest;

const SCHEMA_MUTATION_RUNTIME_EPOCH_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-runtime-epoch:v1";

pub(in crate::db::schema::mutation) fn runtime_epoch_fingerprint(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<[u8; 16], InternalError> {
    let encoded_snapshot = encode_persisted_schema_snapshot(snapshot)?;
    let mut hasher = new_hash_sha256_prefixed(SCHEMA_MUTATION_RUNTIME_EPOCH_PROFILE_TAG);
    write_hash_str_u32(&mut hasher, snapshot.entity_path());
    write_hash_u32(&mut hasher, snapshot.version().get());
    write_hash_u32(
        &mut hasher,
        u32::try_from(encoded_snapshot.len()).unwrap_or(u32::MAX),
    );
    hasher.update(encoded_snapshot);
    let digest = finalize_hash_sha256(hasher);
    let mut fingerprint = [0u8; 16];
    fingerprint.copy_from_slice(&digest[..16]);

    Ok(fingerprint)
}

///
/// SchemaMutationRuntimeEpoch
///
/// Runtime schema identity derived from one accepted persisted snapshot. The
/// physical runner uses it as the publication/invalidation token; staged work
/// must not advance visible runtime identity.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRuntimeEpoch {
    entity_path: String,
    schema_version: SchemaVersion,
    snapshot_fingerprint: [u8; 16],
}

impl SchemaMutationRuntimeEpoch {
    pub(in crate::db::schema) fn from_snapshot(
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            entity_path: snapshot.entity_path().to_string(),
            schema_version: snapshot.version(),
            snapshot_fingerprint: runtime_epoch_fingerprint(snapshot)?,
        })
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn snapshot_fingerprint(&self) -> [u8; 16] {
        self.snapshot_fingerprint
    }
}

///
/// SchemaMutationPublicationIdentity
///
/// Publication identity for one checked runner input. `StagedOnly` keeps the
/// previous epoch visible; only `Published` exposes the accepted-after epoch to
/// runtime caches and planners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationPublicationIdentity {
    before: SchemaMutationRuntimeEpoch,
    after: SchemaMutationRuntimeEpoch,
    store_visibility: SchemaMutationStoreVisibility,
}

impl SchemaMutationPublicationIdentity {
    pub(in crate::db::schema) fn from_input(
        input: &SchemaMutationRunnerInput<'_>,
        store_visibility: SchemaMutationStoreVisibility,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            before: SchemaMutationRuntimeEpoch::from_snapshot(input.accepted_before())?,
            after: SchemaMutationRuntimeEpoch::from_snapshot(input.accepted_after())?,
            store_visibility,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn before_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        &self.before
    }

    #[must_use]
    pub(in crate::db::schema) const fn after_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        &self.after
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn visible_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        match self.store_visibility {
            SchemaMutationStoreVisibility::StagedOnly => &self.before,
            SchemaMutationStoreVisibility::Published => &self.after,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn published_epoch(
        &self,
    ) -> Option<&SchemaMutationRuntimeEpoch> {
        match self.store_visibility {
            SchemaMutationStoreVisibility::StagedOnly => None,
            SchemaMutationStoreVisibility::Published => Some(&self.after),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn changes_epoch(&self) -> bool {
        self.before != self.after
    }
}
