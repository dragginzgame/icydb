//! Schema mutation audit and runtime identity fingerprints.

#[cfg(test)]
use super::{MutationCompatibility, MutationPlan, RebuildRequirement, SchemaMutation};
use super::{SchemaMutationRunnerInput, SchemaMutationStoreVisibility};
#[cfg(test)]
use crate::db::{
    codec::write_hash_tag_u8,
    schema::{FieldId, SchemaFieldSlot},
};
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

#[cfg(test)]
const SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-plan:v1";

const SCHEMA_MUTATION_RUNTIME_EPOCH_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-runtime-epoch:v1";

#[cfg(test)]
impl MutationPlan {
    /// Compute a deterministic plan fingerprint. This is not a cache key yet;
    /// it is a stable audit identity for mutation semantics.
    pub(in crate::db::schema) fn fingerprint(&self) -> [u8; 16] {
        let mut hasher = new_hash_sha256_prefixed(SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG);
        write_hash_tag_u8(&mut hasher, self.compatibility.tag());
        write_hash_tag_u8(&mut hasher, self.rebuild.tag());
        write_hash_u32(
            &mut hasher,
            u32::try_from(self.mutations.len()).unwrap_or(u32::MAX),
        );

        for mutation in &self.mutations {
            mutation.hash_into(&mut hasher);
        }

        let digest = finalize_hash_sha256(hasher);
        let mut fingerprint = [0u8; 16];
        fingerprint.copy_from_slice(&digest[..16]);
        fingerprint
    }
}

#[cfg(test)]
impl SchemaMutation {
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            Self::AddNullableField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 1);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddDefaultedField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 2);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddFieldPathIndex { target } => {
                write_hash_tag_u8(hasher, 3);
                target.hash_into(hasher);
            }
            Self::AddExpressionIndex { target } => {
                write_hash_tag_u8(hasher, 4);
                target.hash_into(hasher);
            }
            Self::DropNonRequiredSecondaryIndex { target } => {
                write_hash_tag_u8(hasher, 5);
                target.hash_into(hasher);
            }
            Self::AlterNullability { field_id } => {
                write_hash_tag_u8(hasher, 6);
                write_hash_u32(hasher, field_id.get());
            }
        }
    }
}

#[cfg(test)]
impl MutationCompatibility {
    const fn tag(self) -> u8 {
        match self {
            Self::MetadataOnlySafe => 1,
            Self::RequiresRebuild => 2,
            Self::UnsupportedPreOne => 3,
            Self::Incompatible => 4,
        }
    }
}

#[cfg(test)]
impl RebuildRequirement {
    const fn tag(self) -> u8 {
        match self {
            Self::NoRebuildRequired => 1,
            Self::IndexRebuildRequired => 2,
            Self::FullDataRewriteRequired => 3,
            Self::Unsupported => 4,
        }
    }
}

#[cfg(test)]
fn hash_field_identity(
    hasher: &mut sha2::Sha256,
    field_id: FieldId,
    name: &str,
    slot: SchemaFieldSlot,
) {
    write_hash_u32(hasher, field_id.get());
    write_hash_str_u32(hasher, name);
    write_hash_u32(hasher, u32::from(slot.get()));
}

#[cfg(test)]
pub(in crate::db::schema::mutation) fn write_hash_bool(hasher: &mut sha2::Sha256, value: bool) {
    write_hash_tag_u8(hasher, u8::from(value));
}

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
/// Runtime schema identity derived from one accepted persisted snapshot. Future
/// runners use this as the publication/invalidation token; staged physical work
/// must not advance visible runtime identity.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRuntimeEpoch {
    entity_path: String,
    schema_version: SchemaVersion,
    snapshot_fingerprint: [u8; 16],
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "0.153 stages runtime epoch identity before physical runners publish snapshots"
    )
)]
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
    pub(in crate::db::schema) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
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

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "0.153 stages runtime publication identity before physical runners publish snapshots"
    )
)]
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
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn visible_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        match self.store_visibility {
            SchemaMutationStoreVisibility::StagedOnly => &self.before,
            SchemaMutationStoreVisibility::Published => &self.after,
        }
    }

    #[must_use]
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
