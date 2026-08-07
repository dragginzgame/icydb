//! Module: db::read_set
//! Responsibility: bounded canonical source-revision proof vocabulary.
//! Does not own: query planning, cursor traversal, or application job state.
//! Boundary: registered physical stores + accepted runtime root -> public proof.

use crate::{
    db::{
        QueryError,
        codec::{finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32},
        integrity::DatabaseIncarnationId,
        schema::AcceptedSchemaRuntimeRootIdentity,
    },
    error::InternalError,
};
use candid::CandidType;
use serde::Deserialize;
use std::{error::Error as StdError, fmt};

const READ_SET_STORE_IDENTITY_DOMAIN: &[u8] = b"icydb.read-set.store.v1";
const READ_SET_PROOF_FIXED_BYTES: usize = 16 + 8 + 1 + 32 + 4;
const READ_SET_STORE_ENTRY_BYTES: usize = 32 + 8 + 8;

/// Maximum physical stores admitted by one exhaustive source proof.
pub const MAX_READ_SET_PROOF_STORES: usize = 64;
const MAX_READ_SET_PROOF_STORES_U32: u32 = 64;
/// Maximum canonical binary bytes admitted by one exhaustive source proof.
pub const MAX_READ_SET_PROOF_BYTES: usize = 8 * 1024;
const MAX_READ_SET_PROOF_BYTES_U32: u32 = 8 * 1024;

/// Opaque canonical identity of one registered physical store.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReadSetStoreIdentity([u8; 32]);

impl ReadSetStoreIdentity {
    pub(in crate::db) fn for_store_path(store_path: &str) -> Self {
        let mut hasher = new_hash_sha256_prefixed(READ_SET_STORE_IDENTITY_DOMAIN);
        write_hash_str_u32(&mut hasher, store_path);
        Self(finalize_hash_sha256(hasher))
    }

    pub(in crate::db) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the opaque canonical bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// One physical store's row and access-state revisions.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct ReadSetStoreRevision {
    store: ReadSetStoreIdentity,
    data_revision: u64,
    access_state_revision: u64,
}

impl ReadSetStoreRevision {
    pub(in crate::db) const fn new(
        store: ReadSetStoreIdentity,
        data_revision: u64,
        access_state_revision: u64,
    ) -> Self {
        Self {
            store,
            data_revision,
            access_state_revision,
        }
    }

    /// Return the physical store identity.
    #[must_use]
    pub const fn store(&self) -> ReadSetStoreIdentity {
        self.store
    }

    /// Return the logical row-mutation revision.
    #[must_use]
    pub const fn data_revision(&self) -> u64 {
        self.data_revision
    }

    /// Return the physical access-readiness revision.
    #[must_use]
    pub const fn access_state_revision(&self) -> u64 {
        self.access_state_revision
    }
}

/// Canonical bounded proof for every physical source store in one exhaustive job.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ReadSetRevisionProof {
    database_incarnation: [u8; 16],
    accepted_root_revision: u64,
    accepted_root_fingerprint_method: u8,
    accepted_root_fingerprint: [u8; 32],
    stores: Vec<ReadSetStoreRevision>,
}

impl ReadSetRevisionProof {
    pub(in crate::db) fn new(
        root: AcceptedSchemaRuntimeRootIdentity,
        stores: Vec<ReadSetStoreRevision>,
    ) -> Result<Self, ReadSetRevisionError> {
        let (accepted_root_fingerprint_method, accepted_root_fingerprint) = root.fingerprint();
        let proof = Self {
            database_incarnation: root.database_incarnation().to_bytes(),
            accepted_root_revision: root.accepted_root_revision().get(),
            accepted_root_fingerprint_method,
            accepted_root_fingerprint,
            stores,
        };
        proof.validate()?;
        Ok(proof)
    }

    pub(in crate::db) fn from_parts(
        database_incarnation: [u8; 16],
        accepted_root_revision: u64,
        accepted_root_fingerprint_method: u8,
        accepted_root_fingerprint: [u8; 32],
        stores: Vec<ReadSetStoreRevision>,
    ) -> Result<Self, ReadSetRevisionError> {
        let proof = Self {
            database_incarnation,
            accepted_root_revision,
            accepted_root_fingerprint_method,
            accepted_root_fingerprint,
            stores,
        };
        proof.validate()?;
        Ok(proof)
    }

    /// Return the durable database lifecycle identity.
    #[must_use]
    pub const fn database_incarnation(&self) -> [u8; 16] {
        self.database_incarnation
    }

    /// Return the accepted runtime-root revision.
    #[must_use]
    pub const fn accepted_root_revision(&self) -> u64 {
        self.accepted_root_revision
    }

    /// Return the accepted runtime-root fingerprint method.
    #[must_use]
    pub const fn accepted_root_fingerprint_method(&self) -> u8 {
        self.accepted_root_fingerprint_method
    }

    /// Return the accepted runtime-root fingerprint.
    #[must_use]
    pub const fn accepted_root_fingerprint(&self) -> [u8; 32] {
        self.accepted_root_fingerprint
    }

    /// Borrow canonically sorted participating stores.
    #[must_use]
    pub const fn stores(&self) -> &[ReadSetStoreRevision] {
        self.stores.as_slice()
    }

    /// Return the exact current canonical binary size.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        READ_SET_PROOF_FIXED_BYTES
            .saturating_add(self.stores.len().saturating_mul(READ_SET_STORE_ENTRY_BYTES))
    }

    /// Validate bounds, nonzero revisions, and canonical store ordering.
    pub fn validate(&self) -> Result<(), ReadSetRevisionError> {
        if self.stores.is_empty() {
            return Err(ReadSetRevisionError::Empty);
        }
        if self.stores.len() > MAX_READ_SET_PROOF_STORES {
            return Err(ReadSetRevisionError::TooManyStores {
                limit: MAX_READ_SET_PROOF_STORES_U32,
                actual: u32::try_from(self.stores.len()).unwrap_or(u32::MAX),
            });
        }
        let encoded_len = self.encoded_len();
        if encoded_len > MAX_READ_SET_PROOF_BYTES {
            return Err(ReadSetRevisionError::EncodedBytesExceeded {
                limit: MAX_READ_SET_PROOF_BYTES_U32,
                actual: u32::try_from(encoded_len).unwrap_or(u32::MAX),
            });
        }
        if self.database_incarnation == [0; 16]
            || self.accepted_root_revision == 0
            || self.accepted_root_fingerprint_method == 0
            || self.accepted_root_fingerprint == [0; 32]
            || self.stores.iter().any(|store| {
                store.store.to_bytes() == [0; 32]
                    || store.data_revision == 0
                    || store.access_state_revision == 0
            })
            || self
                .stores
                .windows(2)
                .any(|pair| pair[0].store >= pair[1].store)
        {
            return Err(ReadSetRevisionError::NonCanonical);
        }
        Ok(())
    }

    pub(in crate::db) fn contains_store(&self, store: ReadSetStoreIdentity) -> bool {
        self.stores
            .binary_search_by_key(&store, ReadSetStoreRevision::store)
            .is_ok()
    }

    pub(in crate::db) fn signature_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&self.database_incarnation);
        bytes.extend_from_slice(&self.accepted_root_revision.to_be_bytes());
        bytes.push(self.accepted_root_fingerprint_method);
        bytes.extend_from_slice(&self.accepted_root_fingerprint);
        let store_count = u32::try_from(self.stores.len()).unwrap_or(u32::MAX);
        bytes.extend_from_slice(&store_count.to_be_bytes());
        for store in &self.stores {
            bytes.extend_from_slice(&store.store.to_bytes());
            bytes.extend_from_slice(&store.data_revision.to_be_bytes());
            bytes.extend_from_slice(&store.access_state_revision.to_be_bytes());
        }
        bytes
    }

    pub(in crate::db) fn root_matches(
        &self,
        incarnation: DatabaseIncarnationId,
        root: AcceptedSchemaRuntimeRootIdentity,
    ) -> bool {
        let (method, fingerprint) = root.fingerprint();
        self.database_incarnation == incarnation.to_bytes()
            && self.accepted_root_revision == root.accepted_root_revision().get()
            && self.accepted_root_fingerprint_method == method
            && self.accepted_root_fingerprint == fingerprint
    }
}

/// Typed failure while capturing or validating an exhaustive source proof.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum ReadSetRevisionError {
    /// No participating physical store was supplied.
    Empty,
    /// The proof exceeds the bounded participating-store count.
    TooManyStores { limit: u32, actual: u32 },
    /// The canonical proof encoding exceeds its byte ceiling.
    EncodedBytesExceeded { limit: u32, actual: u32 },
    /// Proof authority, revisions, or store ordering are not canonical.
    NonCanonical,
    /// One requested entity is absent from accepted runtime authority.
    UnknownEntity,
    /// The page's physical source was not declared in the initial proof.
    StoreMissingFromProof { store: ReadSetStoreIdentity },
    /// A continuation was supplied without its associated source proof.
    ResumeProofRequired,
    /// The database was recreated after the proof was captured.
    DatabaseIncarnationChanged,
    /// Accepted runtime authority changed after the proof was captured.
    AcceptedRootChanged,
    /// Rows in one participating physical store changed.
    StoreDataChanged { store: ReadSetStoreIdentity },
    /// Physical access readiness in one participating store changed.
    StoreAccessChanged { store: ReadSetStoreIdentity },
    /// A cross-call job attempted to use a volatile source store.
    DurableStoreRequired { store: ReadSetStoreIdentity },
}

impl fmt::Display for ReadSetRevisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exhaustive read source proof is invalid")
    }
}

impl StdError for ReadSetRevisionError {}

impl ReadSetRevisionError {
    pub(in crate::db) const fn is_source_change(&self) -> bool {
        matches!(
            self,
            Self::DatabaseIncarnationChanged
                | Self::AcceptedRootChanged
                | Self::StoreDataChanged { .. }
                | Self::StoreAccessChanged { .. }
        )
    }
}

/// Query/runtime or typed source-proof failure from an exhaustive operation.
#[derive(Debug)]
pub enum ExhaustiveReadError {
    /// Query planning, admission, execution, or database authority failed.
    Query(QueryError),
    /// The bounded source proof was invalid or changed.
    Revision(ReadSetRevisionError),
}

impl fmt::Display for ExhaustiveReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => error.fmt(formatter),
            Self::Revision(error) => error.fmt(formatter),
        }
    }
}

impl StdError for ExhaustiveReadError {}

impl From<QueryError> for ExhaustiveReadError {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

impl From<InternalError> for ExhaustiveReadError {
    fn from(error: InternalError) -> Self {
        Self::Query(QueryError::execute(error))
    }
}

impl From<ReadSetRevisionError> for ExhaustiveReadError {
    fn from(error: ReadSetRevisionError) -> Self {
        Self::Revision(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(byte: u8, data_revision: u64) -> ReadSetStoreRevision {
        ReadSetStoreRevision::new(
            ReadSetStoreIdentity::from_bytes([byte; 32]),
            data_revision,
            1,
        )
    }

    fn proof(
        stores: Vec<ReadSetStoreRevision>,
    ) -> Result<ReadSetRevisionProof, ReadSetRevisionError> {
        ReadSetRevisionProof::from_parts([1; 16], 1, 1, [2; 32], stores)
    }

    #[test]
    fn read_set_proof_requires_bounded_canonical_distinct_store_order() {
        assert_eq!(proof(Vec::new()), Err(ReadSetRevisionError::Empty));
        assert_eq!(
            proof(vec![store(2, 1), store(1, 1)]),
            Err(ReadSetRevisionError::NonCanonical),
        );
        assert_eq!(
            proof(vec![store(1, 1), store(1, 2)]),
            Err(ReadSetRevisionError::NonCanonical),
        );

        let too_many = (1..=MAX_READ_SET_PROOF_STORES + 1)
            .map(|index| {
                let mut identity = [0; 32];
                identity[24..].copy_from_slice(
                    &u64::try_from(index)
                        .expect("bounded test store count should fit u64")
                        .to_be_bytes(),
                );
                ReadSetStoreRevision::new(ReadSetStoreIdentity::from_bytes(identity), 1, 1)
            })
            .collect();
        assert_eq!(
            proof(too_many),
            Err(ReadSetRevisionError::TooManyStores {
                limit: MAX_READ_SET_PROOF_STORES_U32,
                actual: u32::try_from(MAX_READ_SET_PROOF_STORES + 1)
                    .expect("bounded test store count should fit u32"),
            }),
        );
    }

    #[test]
    fn read_set_proof_rejects_zero_authority_or_revision_components() {
        let valid = proof(vec![store(1, 1)]).expect("nonzero canonical proof should admit");
        assert_eq!(
            valid.encoded_len(),
            READ_SET_PROOF_FIXED_BYTES + READ_SET_STORE_ENTRY_BYTES
        );

        assert_eq!(
            ReadSetRevisionProof::from_parts([1; 16], 0, 1, [2; 32], vec![store(1, 1)]),
            Err(ReadSetRevisionError::NonCanonical),
        );
        assert_eq!(
            proof(vec![store(1, 0)]),
            Err(ReadSetRevisionError::NonCanonical),
        );
    }
}
