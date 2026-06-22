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
pub use handle::{
    StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
    StoreCommitParticipation, StoreDurability, StoreLiveValidationCapability,
    StoreRecoveryCapability, StoreRelationSourceCapability, StoreRelationTargetCapability,
    StoreRuntimeStorageCapabilities, StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
};
pub use registry::StoreRegistry;
