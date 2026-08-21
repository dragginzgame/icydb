//! Module: db::registry
//! Responsibility: thread-local store registry lifecycle and lookup boundary.
//! Does not own: store encode/decode semantics or query/executor planning behavior.
//! Boundary: manages registry state for named data/index stores and typed registry errors.

mod error;
mod handle;
mod readers;
mod registry;
#[cfg(test)]
mod tests;

pub(in crate::db::registry) use error::StoreRegistryError;
pub(crate) use handle::StoreHandle;
pub(in crate::db) use handle::{
    ExactPrefixCardinalityLifecycleStamp, ExactUserIndexPrefixEvidence,
};
pub use handle::{
    StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
    StoreCommitParticipation, StoreDurability, StoreRecoveryCapability,
    StoreRelationSourceCapability, StoreRelationTargetCapability, StoreRuntimeStorageCapabilities,
    StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
};
#[cfg(test)]
pub(in crate::db) use handle::{
    exact_prefix_evidence_call_counts_for_tests, reset_exact_prefix_evidence_call_counts_for_tests,
};
pub use registry::StoreRegistry;
