//! Module: db::schema::application
//! Responsibility: issue database/store proposal targets and one exact database-wide accepted head.
//! Does not own: proposal lowering, schema mutation, durable receipts, or activation progress.
//! Boundary: recovered runtime registry plus accepted store roots -> opaque public application target.

use crate::{
    db::{
        Db,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_str_u32,
            write_hash_tag_u8, write_hash_u64,
        },
        commit::{database_incarnation_id, ensure_recovered},
        registry::{
            StoreAllocationIdentity, StoreAllocationIdentityCapability, StoreCommitParticipation,
            StoreDurability, StoreHandle, StoreRecoveryCapability, StoreRelationSourceCapability,
            StoreRelationTargetCapability, StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use candid::CandidType;
use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, TargetDatabaseIdentity, TargetStoreIdentity,
};
use serde::Deserialize;
use sha2::Digest;

const DATABASE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.database.v1";
const STORE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.store.v1";
const ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE: &[u8] = b"icydb.accepted-schema.database-head.v1";

///
/// SchemaApplicationStore
///
/// One registered store path paired with the opaque routing token accepted by
/// the current database incarnation.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaApplicationStore {
    path: String,
    identity: TargetStoreIdentity,
}

impl SchemaApplicationStore {
    /// Borrow the registered store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return the opaque routing identity for this store.
    #[must_use]
    pub const fn identity(&self) -> TargetStoreIdentity {
        self.identity
    }
}

///
/// SchemaApplicationTarget
///
/// Point-in-time optimistic application context issued from recovered runtime
/// authority. Callers compose proposals against these opaque identities and
/// this exact database-wide accepted head.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SchemaApplicationTarget {
    database_identity: TargetDatabaseIdentity,
    accepted_head: ExpectedAcceptedHead,
    stores: Vec<SchemaApplicationStore>,
}

impl SchemaApplicationTarget {
    /// Return the opaque current database identity.
    #[must_use]
    pub const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    /// Borrow the exact optimistic accepted head.
    #[must_use]
    pub const fn accepted_head(&self) -> &ExpectedAcceptedHead {
        &self.accepted_head
    }

    /// Borrow registered stores in canonical path order.
    #[must_use]
    pub const fn stores(&self) -> &[SchemaApplicationStore] {
        self.stores.as_slice()
    }
}

///
/// StoreApplicationAuthority
///
/// Canonically ordered registry facts used to derive opaque proposal routing
/// identities without exposing physical allocation details.
///

#[derive(Clone, Copy)]
struct StoreApplicationAuthority {
    path: &'static str,
    handle: StoreHandle,
}

///
/// AcceptedStoreHead
///
/// Exact store-local root facts contributing to the database-wide optimistic
/// accepted head. Absence is represented explicitly by the enclosing option.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AcceptedStoreHead {
    revision: u64,
    fingerprint: [u8; 32],
}

/// Issue the current proposal-application target from recovered authority.
pub(in crate::db) fn schema_application_target<C: CanisterKind>(
    db: &Db<C>,
) -> Result<SchemaApplicationTarget, InternalError> {
    ensure_recovered(db)?;
    let incarnation = database_incarnation_id()?;
    let mut stores = db.with_store_registry(|registry| {
        registry
            .iter()
            .map(|(path, handle)| StoreApplicationAuthority { path, handle })
            .collect::<Vec<_>>()
    });
    stores.sort_by(|left, right| left.path.cmp(right.path));

    let database_identity = derive_database_identity(incarnation.to_bytes(), stores.as_slice());
    let mut accepted_heads = Vec::with_capacity(stores.len());
    let mut application_stores = Vec::with_capacity(stores.len());
    for store in &stores {
        let root = store
            .handle
            .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_root)?
            .map(|selection| AcceptedStoreHead {
                revision: selection.root().revision().get(),
                fingerprint: selection.root().fingerprint().as_bytes(),
            });
        accepted_heads.push((store.path, root));
        application_stores.push(SchemaApplicationStore {
            path: store.path.to_string(),
            identity: derive_store_identity(database_identity, store),
        });
    }

    Ok(SchemaApplicationTarget {
        database_identity,
        accepted_head: derive_accepted_head(accepted_heads.as_slice()),
        stores: application_stores,
    })
}

fn derive_database_identity(
    incarnation: [u8; 16],
    stores: &[StoreApplicationAuthority],
) -> TargetDatabaseIdentity {
    let mut hasher = new_hash_sha256_prefixed(DATABASE_TARGET_FINGERPRINT_PROFILE);
    hasher.update(incarnation);
    write_hash_len_u32(&mut hasher, stores.len());
    for store in stores {
        write_store_authority(&mut hasher, store);
    }
    TargetDatabaseIdentity::from_bytes(finalize_hash_sha256(hasher))
}

fn derive_store_identity(
    database_identity: TargetDatabaseIdentity,
    store: &StoreApplicationAuthority,
) -> TargetStoreIdentity {
    let mut hasher = new_hash_sha256_prefixed(STORE_TARGET_FINGERPRINT_PROFILE);
    hasher.update(database_identity.to_bytes());
    write_store_authority(&mut hasher, store);
    TargetStoreIdentity::from_bytes(finalize_hash_sha256(hasher))
}

fn derive_accepted_head(stores: &[(&str, Option<AcceptedStoreHead>)]) -> ExpectedAcceptedHead {
    let Some(revision) = stores
        .iter()
        .filter_map(|(_, head)| head.map(|head| head.revision))
        .max()
    else {
        return ExpectedAcceptedHead::Empty;
    };

    let mut hasher = new_hash_sha256_prefixed(ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE);
    write_hash_len_u32(&mut hasher, stores.len());
    for (path, head) in stores {
        write_hash_str_u32(&mut hasher, path);
        match head {
            None => write_hash_tag_u8(&mut hasher, 0),
            Some(head) => {
                write_hash_tag_u8(&mut hasher, 1);
                write_hash_u64(&mut hasher, head.revision);
                hasher.update(head.fingerprint);
            }
        }
    }

    ExpectedAcceptedHead::Exact {
        revision,
        fingerprint: ExpectedSchemaFingerprint::from_bytes(finalize_hash_sha256(hasher)),
    }
}

fn write_store_authority(hasher: &mut sha2::Sha256, store: &StoreApplicationAuthority) {
    write_hash_str_u32(hasher, store.path);
    write_storage_capabilities(hasher, store.handle);
    for allocation in [
        store.handle.data_allocation(),
        store.handle.index_allocation(),
        store.handle.schema_allocation(),
        store.handle.journal_allocation(),
    ] {
        write_allocation_identity(hasher, allocation);
    }
}

fn write_storage_capabilities(hasher: &mut sha2::Sha256, store: StoreHandle) {
    let capabilities = store.storage_capabilities();
    write_hash_tag_u8(
        hasher,
        match capabilities.storage_mode() {
            StoreRuntimeStorageMode::Heap => 0,
            StoreRuntimeStorageMode::Journaled => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.allocation_identity() {
            StoreAllocationIdentityCapability::Present => 0,
            StoreAllocationIdentityCapability::Absent => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.durability() {
            StoreDurability::Durable => 0,
            StoreDurability::Volatile => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.recovery() {
            StoreRecoveryCapability::StableBasePlusJournalReplay => 0,
            StoreRecoveryCapability::None => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.commit_participation() {
            StoreCommitParticipation::Durable => 0,
            StoreCommitParticipation::LiveOnly => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.schema_metadata() {
            StoreSchemaMetadataCapability::LiveRebuiltMetadata => 0,
            StoreSchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.relation_source() {
            StoreRelationSourceCapability::DurableSource => 0,
            StoreRelationSourceCapability::LiveSource => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.relation_target() {
            StoreRelationTargetCapability::DurableTarget => 0,
            StoreRelationTargetCapability::VolatileTarget => 1,
        },
    );
}

fn write_allocation_identity(
    hasher: &mut sha2::Sha256,
    allocation: Option<StoreAllocationIdentity>,
) {
    match allocation {
        None => write_hash_tag_u8(hasher, 0),
        Some(allocation) => {
            write_hash_tag_u8(hasher, 1);
            write_hash_tag_u8(hasher, allocation.memory_id());
            write_hash_str_u32(hasher, allocation.stable_key());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptedStoreHead, derive_accepted_head};
    use icydb_schema::ExpectedAcceptedHead;

    #[test]
    fn database_head_is_empty_only_when_every_store_root_is_absent() {
        assert_eq!(
            derive_accepted_head(&[("test::A", None), ("test::B", None)]),
            ExpectedAcceptedHead::Empty,
        );
    }

    #[test]
    fn database_head_covers_store_path_revision_fingerprint_and_absence() {
        let first = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x11; 32],
                }),
            ),
            ("test::B", None),
        ]);
        let changed_fingerprint = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x12; 32],
                }),
            ),
            ("test::B", None),
        ]);
        let changed_absence = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x11; 32],
                }),
            ),
            (
                "test::B",
                Some(AcceptedStoreHead {
                    revision: 1,
                    fingerprint: [0x22; 32],
                }),
            ),
        ]);

        assert_ne!(first, changed_fingerprint);
        assert_ne!(first, changed_absence);
        assert!(matches!(
            first,
            ExpectedAcceptedHead::Exact { revision: 3, .. }
        ));
    }
}
