//! Module: diagnostics::memory_allocations
//! Responsibility: project bounded runtime allocation facts into Wire DTOs.
//! Does not own: manager validation, allocation policy, or payload accounting.
//! Boundary: ic-memory's owned report -> canister-wide storage diagnostics.

#[cfg(test)]
mod tests;

use crate::{db::memory_runtime, error::InternalError};

use ic_memory::MemoryAllocationSummary;
use serde::Deserialize;

/// Physical allocation accounting for the bound host memory runtime.
///
/// The ledger is already included in totals. Virtual extent is addressable
/// memory, not live payload. Unknown bindings still own physical allocations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct MemoryAllocations {
    /// Committed runtime generation, absent before allocation bootstrap.
    pub current_generation: Option<u64>,
    /// Validated manager layout version.
    pub manager_layout_version: u8,
    /// Actual persisted bucket size in Wasm pages.
    pub bucket_size_pages: u16,
    /// Actual persisted bucket size in bytes.
    pub bucket_size_bytes: u64,
    /// Total bucket-table capacity shared by all slots.
    pub bucket_capacity: u32,
    /// Number of assigned buckets.
    pub allocated_buckets: u16,
    /// Remaining bucket-table entries.
    pub remaining_buckets: u32,
    /// Table capacity in bytes, excluding metadata and backing limits.
    pub maximum_bucket_bytes: u64,
    /// Entire backing extent, including manager metadata and residuals.
    pub physical_extent: MemoryExtent,
    /// Sum of addressable virtual extents.
    pub virtual_extent: MemoryExtent,
    /// Complete manager metadata page, including padding.
    pub manager_metadata_bytes: u64,
    /// Manager header bytes.
    pub manager_header_bytes: u64,
    /// Manager bucket-table bytes.
    pub manager_bucket_table_bytes: u64,
    /// Padding inside the manager metadata page.
    pub manager_padding_bytes: u64,
    /// Sum of bytes assigned to all slots, including the ledger.
    pub allocated_bucket_bytes: u64,
    /// Assigned bucket bytes beyond virtual extents.
    pub bucket_slack_bytes: u64,
    /// Assigned bytes with a current binding, including the ledger.
    pub known_binding_bytes: u64,
    /// Assigned bytes without a current binding; not free space.
    pub unknown_binding_bytes: u64,
    /// Backing bytes outside the manager's assigned region.
    pub unmanaged_bytes: u64,
    /// Metadata bytes read by the bounded collector.
    pub metadata_bytes_read: u64,
    /// All 255 usable slots in ID order, including zero-size slots.
    pub memories: Vec<MemoryAllocation>,
}

impl MemoryAllocations {
    // Project physical facts and current host labels from the same runtime.
    // Unbootstrapped bindings stay unknown; retained ledger bytes grant no labels.
    fn from_runtime<M: ic_memory::Memory>(
        runtime: &ic_memory::MemoryRuntime<M>,
    ) -> Result<Self, InternalError> {
        let report = runtime
            .memory_allocations()
            .map_err(|_| InternalError::store_internal())?;
        let (bindings, pool) = if runtime.is_bootstrapped() {
            (
                runtime
                    .committed_allocations()
                    .map_err(|_| InternalError::store_internal())?,
                Some(
                    runtime
                        .allocation_pool()
                        .map_err(|_| InternalError::store_internal())?,
                ),
            )
        } else {
            (&[][..], None)
        };
        Ok(Self {
            current_generation: report.summary.current_generation,
            manager_layout_version: MemoryAllocationSummary::MANAGER_LAYOUT_VERSION,
            bucket_size_pages: report.summary.bucket_size_pages,
            bucket_size_bytes: report.summary.bucket_size_bytes(),
            bucket_capacity: MemoryAllocationSummary::BUCKET_CAPACITY,
            allocated_buckets: report.summary.allocated_buckets,
            remaining_buckets: report.summary.remaining_buckets(),
            maximum_bucket_bytes: report.summary.maximum_bucket_bytes(),
            physical_extent: report.summary.physical_extent.into(),
            virtual_extent: report.summary.virtual_extent.into(),
            manager_metadata_bytes: MemoryAllocationSummary::MANAGER_METADATA_BYTES,
            manager_header_bytes: MemoryAllocationSummary::MANAGER_HEADER_BYTES,
            manager_bucket_table_bytes: MemoryAllocationSummary::MANAGER_BUCKET_TABLE_BYTES,
            manager_padding_bytes: MemoryAllocationSummary::MANAGER_PADDING_BYTES,
            allocated_bucket_bytes: report.summary.allocated_bucket_bytes(),
            bucket_slack_bytes: report.summary.bucket_slack_bytes(),
            known_binding_bytes: report.summary.known_binding_bytes(),
            unknown_binding_bytes: report.summary.unknown_binding_bytes(),
            unmanaged_bytes: report.summary.unmanaged_bytes(),
            metadata_bytes_read: MemoryAllocationSummary::METADATA_BYTES_READ,
            memories: report
                .memories
                .into_iter()
                .map(|memory| {
                    MemoryAllocation::from_runtime(
                        memory,
                        report.summary.bucket_size_pages,
                        bindings,
                        pool,
                    )
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Measured extent in 64-KiB Wasm pages and bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct MemoryExtent {
    /// Extent in Wasm pages.
    pub wasm_pages: u64,
    /// Extent in bytes.
    pub bytes: u64,
}

impl From<ic_memory::DiagnosticMemorySize> for MemoryExtent {
    fn from(extent: ic_memory::DiagnosticMemorySize) -> Self {
        Self {
            wasm_pages: extent.wasm_pages,
            bytes: extent.bytes(),
        }
    }
}

/// Current binding of a physical slot; this grants no opening authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum MemoryAllocationBinding {
    /// A declaration bound by successful runtime bootstrap.
    Current {
        /// Durable allocation key.
        stable_key: String,
        /// Declaring authority.
        owner: String,
    },
    /// Substrate-reserved ledger slot, not evidence of a validated ledger payload.
    Ledger {
        /// Ledger allocation key.
        stable_key: String,
        /// Ledger authority.
        owner: String,
    },
    /// No current binding is available; historical ownership is not decoded.
    Unknown,
}

/// Physical accounting for one usable manager slot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct MemoryAllocation {
    /// Memory-manager slot ID.
    pub memory_manager_id: u8,
    /// Current allocation binding, including explicit unknown ownership.
    pub binding: MemoryAllocationBinding,
    /// Whether this slot belongs to the current host pool; absent before bootstrap.
    pub pool_eligible: Option<bool>,
    /// Addressable virtual extent, not payload occupancy.
    pub virtual_extent: MemoryExtent,
    /// Assigned bucket count.
    pub allocated_buckets: u16,
    /// Assigned bucket bytes.
    pub allocated_bytes: u64,
    /// Assigned capacity beyond the virtual extent.
    pub bucket_slack_bytes: u64,
    /// Unavailable: manager metadata cannot measure live payload occupancy.
    pub payload_bytes: Option<u64>,
}

impl MemoryAllocation {
    // Per-slot byte counts require the containing report's measured geometry.
    // Keep that context explicit instead of assuming a default bucket size.
    fn from_runtime(
        memory: ic_memory::MemoryAllocation,
        bucket_size_pages: u16,
        bindings: &[ic_memory::AllocationDeclaration],
        pool: Option<&ic_memory::MemoryAllocationPool>,
    ) -> Result<Self, InternalError> {
        let allocated_bytes = memory.allocated_bytes(bucket_size_pages);
        let bucket_slack_bytes = memory.bucket_slack_bytes(bucket_size_pages);
        let binding = if memory.memory_manager_id == ic_memory::MEMORY_MANAGER_LEDGER_ID {
            MemoryAllocationBinding::Ledger {
                stable_key: ic_memory::IC_MEMORY_LEDGER_STABLE_KEY.to_string(),
                owner: ic_memory::IC_MEMORY_AUTHORITY_OWNER.to_string(),
            }
        } else if let Some(declaration) = bindings
            .iter()
            .find(|binding| binding.slot().id() == memory.memory_manager_id)
        {
            let owner = pool
                .and_then(|pool| pool.authority_for_key(declaration.stable_key()))
                .ok_or_else(InternalError::store_invariant)?;
            MemoryAllocationBinding::Current {
                stable_key: declaration.stable_key().to_string(),
                owner: owner.to_string(),
            }
        } else {
            MemoryAllocationBinding::Unknown
        };
        Ok(Self {
            memory_manager_id: memory.memory_manager_id,
            binding,
            pool_eligible: pool.map(|pool| pool.contains(memory.memory_manager_id)),
            virtual_extent: memory.virtual_extent.into(),
            allocated_buckets: memory.allocated_buckets,
            allocated_bytes,
            bucket_slack_bytes,
            // The manager measures addressable capacity, never live payload.
            payload_bytes: None,
        })
    }
}

// Inspect the existing binding only. Heap-only/native callers can have no
// runtime; a corrupt or inaccessible runtime must not look like absence.
pub(super) fn collect_memory_allocations() -> Result<Option<MemoryAllocations>, InternalError> {
    let Some(runtime) = memory_runtime() else {
        return Ok(None);
    };
    MemoryAllocations::from_runtime(runtime.as_ref()).map(Some)
}
