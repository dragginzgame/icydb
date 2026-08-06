//! Module: db::schema::runtime_root
//! Responsibility: canonical database-wide accepted runtime-root identity.
//! Does not own: runtime entity construction, cache publication, or query planning.
//! Boundary: accepted store roots -> one incarnation-bound runtime cache identity.

use crate::{
    db::{
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_str_u32,
            write_hash_tag_u8, write_hash_u64,
        },
        integrity::DatabaseIncarnationId,
        schema::{AcceptedSchemaRevision, enum_catalog::AcceptedSchemaRoot},
    },
    error::InternalError,
};
use sha2::Digest;

const ACCEPTED_RUNTIME_ROOT_FINGERPRINT_DOMAIN: &[u8] = b"icydb.accepted-runtime-root.v1";
const ACCEPTED_RUNTIME_ROOT_FINGERPRINT_METHOD_VERSION: u8 = 1;

///
/// AcceptedSchemaRuntimeStoreRoot
///
/// One registered store's exact contribution to a database-wide accepted
/// runtime root. Root absence is explicit so first publication invalidates an
/// already-captured empty-store authority.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRuntimeStoreRoot {
    store_path: &'static str,
    root: Option<AcceptedSchemaRoot>,
}

impl AcceptedSchemaRuntimeStoreRoot {
    /// Capture one registered store's current accepted root.
    #[must_use]
    pub(in crate::db) const fn new(
        store_path: &'static str,
        root: Option<AcceptedSchemaRoot>,
    ) -> Self {
        Self { store_path, root }
    }

    /// Borrow the canonical registered store path.
    #[must_use]
    pub(in crate::db) const fn store_path(self) -> &'static str {
        self.store_path
    }

    /// Borrow the selected store-local root, when present.
    #[must_use]
    pub(in crate::db) const fn root(self) -> Option<AcceptedSchemaRoot> {
        self.root
    }
}

///
/// AcceptedSchemaRuntimeRootIdentity
///
/// Database-wide identity for one immutable accepted runtime publication.
/// It binds the durable database incarnation and every registered store's
/// current accepted root, including explicit root absence.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRuntimeRootIdentity {
    database_incarnation: DatabaseIncarnationId,
    accepted_root_revision: AcceptedSchemaRevision,
    fingerprint_method_version: u8,
    fingerprint: [u8; 32],
}

impl AcceptedSchemaRuntimeRootIdentity {
    /// Derive one runtime-root identity from a canonical store-path ordering.
    pub(in crate::db) fn from_store_roots(
        database_incarnation: DatabaseIncarnationId,
        store_roots: &[AcceptedSchemaRuntimeStoreRoot],
    ) -> Result<Self, InternalError> {
        if store_roots
            .windows(2)
            .any(|pair| pair[0].store_path() >= pair[1].store_path())
        {
            return Err(InternalError::store_invariant());
        }

        let accepted_root_revision = store_roots
            .iter()
            .filter_map(|store| store.root().map(AcceptedSchemaRoot::revision))
            .max()
            .unwrap_or(AcceptedSchemaRevision::NONE);
        let mut hasher = new_hash_sha256_prefixed(ACCEPTED_RUNTIME_ROOT_FINGERPRINT_DOMAIN);
        write_hash_len_u32(&mut hasher, store_roots.len());
        for store in store_roots {
            write_hash_str_u32(&mut hasher, store.store_path());
            match store.root() {
                None => write_hash_tag_u8(&mut hasher, 0),
                Some(root) => {
                    write_hash_tag_u8(&mut hasher, 1);
                    write_hash_u64(&mut hasher, root.revision().get());
                    hasher.update(root.fingerprint().as_bytes());
                }
            }
        }

        Ok(Self {
            database_incarnation,
            accepted_root_revision,
            fingerprint_method_version: ACCEPTED_RUNTIME_ROOT_FINGERPRINT_METHOD_VERSION,
            fingerprint: finalize_hash_sha256(hasher),
        })
    }

    /// Return the durable database lifecycle identity.
    #[must_use]
    pub(in crate::db) const fn database_incarnation(self) -> DatabaseIncarnationId {
        self.database_incarnation
    }

    /// Return the durable accepted-root generation represented by this runtime.
    #[must_use]
    pub(in crate::db) const fn accepted_root_revision(self) -> AcceptedSchemaRevision {
        self.accepted_root_revision
    }

    /// Return the method-qualified database-wide accepted-root fingerprint.
    #[must_use]
    pub(in crate::db) const fn fingerprint(self) -> (u8, [u8; 32]) {
        (self.fingerprint_method_version, self.fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaRuntimeStoreRoot};
    use crate::db::{
        integrity::DatabaseIncarnationId,
        schema::{AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests},
    };

    fn store_root(
        store_path: &'static str,
        revision: AcceptedSchemaRevision,
    ) -> AcceptedSchemaRuntimeStoreRoot {
        let candidate = empty_accepted_schema_candidate_for_tests(store_path, revision);
        AcceptedSchemaRuntimeStoreRoot::new(store_path, Some(candidate.root()))
    }

    #[test]
    fn runtime_root_identity_binds_every_store_root_and_database_incarnation() {
        let first = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            DatabaseIncarnationId::for_tests(1),
            &[
                store_root("test::First", AcceptedSchemaRevision::INITIAL),
                store_root("test::Second", AcceptedSchemaRevision::INITIAL),
            ],
        )
        .expect("canonical accepted roots should admit");
        let changed_store = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            DatabaseIncarnationId::for_tests(1),
            &[
                store_root("test::First", AcceptedSchemaRevision::INITIAL),
                store_root("test::Second", AcceptedSchemaRevision::new(2)),
            ],
        )
        .expect("changed accepted root should admit");
        let changed_incarnation = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            DatabaseIncarnationId::for_tests(2),
            &[
                store_root("test::First", AcceptedSchemaRevision::INITIAL),
                store_root("test::Second", AcceptedSchemaRevision::INITIAL),
            ],
        )
        .expect("changed database incarnation should admit");

        assert_ne!(first, changed_store);
        assert_ne!(first, changed_incarnation);
    }

    #[test]
    fn runtime_root_identity_rejects_noncanonical_store_order() {
        let stores = [
            AcceptedSchemaRuntimeStoreRoot::new("test::Second", None),
            AcceptedSchemaRuntimeStoreRoot::new("test::First", None),
        ];

        assert!(
            AcceptedSchemaRuntimeRootIdentity::from_store_roots(
                DatabaseIncarnationId::for_tests(1),
                &stores,
            )
            .is_err(),
        );
    }
}
