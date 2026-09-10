//! Module: diagnostics::memory_allocations
//! Responsibility: project bounded runtime allocation facts into Candid DTOs.
//! Does not own: manager validation, allocation policy, or payload accounting.
//! Boundary: ic-memory's owned report -> canister-wide storage diagnostics.

#[cfg(test)]
mod tests;

use crate::error::InternalError;
use candid::CandidType;
use serde::Deserialize;

/// Physical allocation accounting for the entire default memory runtime.
///
/// The ledger is already included in totals. Virtual extent is addressable
/// memory, not live payload. Unknown bindings still own physical allocations.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

impl From<ic_memory::MemoryAllocations> for MemoryAllocations {
    fn from(report: ic_memory::MemoryAllocations) -> Self {
        Self {
            current_generation: report.current_generation,
            manager_layout_version: report.manager_layout_version,
            bucket_size_pages: report.bucket_size_pages,
            bucket_size_bytes: report.bucket_size_bytes,
            bucket_capacity: report.bucket_capacity,
            allocated_buckets: report.allocated_buckets,
            remaining_buckets: report.remaining_buckets,
            maximum_bucket_bytes: report.maximum_bucket_bytes,
            physical_extent: report.physical_extent.into(),
            virtual_extent: report.virtual_extent.into(),
            manager_metadata_bytes: report.manager_metadata_bytes,
            manager_header_bytes: report.manager_header_bytes,
            manager_bucket_table_bytes: report.manager_bucket_table_bytes,
            manager_padding_bytes: report.manager_padding_bytes,
            allocated_bucket_bytes: report.allocated_bucket_bytes,
            bucket_slack_bytes: report.bucket_slack_bytes,
            known_binding_bytes: report.known_binding_bytes,
            unknown_binding_bytes: report.unknown_binding_bytes,
            unmanaged_bytes: report.unmanaged_bytes,
            metadata_bytes_read: report.metadata_bytes_read,
            memories: report.memories.into_iter().map(Into::into).collect(),
        }
    }
}

/// Measured extent in 64-KiB Wasm pages and bytes.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
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
            bytes: extent.bytes,
        }
    }
}

/// Current binding of a physical slot; this grants no opening authority.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum MemoryAllocationBinding {
    /// A declaration bound by successful runtime bootstrap.
    Current {
        /// Durable allocation key.
        stable_key: String,
        /// Declaring authority.
        owner: String,
    },
    /// Reserved allocation ledger; its payload is not inspected here.
    Ledger {
        /// Ledger allocation key.
        stable_key: String,
        /// Ledger authority.
        owner: String,
    },
    /// No current binding is available; historical ownership is not decoded.
    Unknown,
}

/// Current range policy metadata, not proof of historical slot ownership.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MemoryAllocationRangeClaim {
    /// Declaring range authority.
    pub authority: String,
    /// True for reserved ranges; false for allowed application ranges.
    pub reserved: bool,
}

/// Physical accounting for one usable manager slot.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MemoryAllocation {
    /// Memory-manager slot ID.
    pub memory_manager_id: u8,
    /// Current allocation binding, including explicit unknown ownership.
    pub binding: MemoryAllocationBinding,
    /// Current range claim, if any.
    pub range_claim: Option<MemoryAllocationRangeClaim>,
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

impl From<ic_memory::MemoryAllocation> for MemoryAllocation {
    fn from(memory: ic_memory::MemoryAllocation) -> Self {
        Self {
            memory_manager_id: memory.memory_manager_id,
            binding: match memory.binding {
                ic_memory::AllocationBinding::Current { stable_key, owner } => {
                    MemoryAllocationBinding::Current { stable_key, owner }
                }
                ic_memory::AllocationBinding::Ledger { stable_key, owner } => {
                    MemoryAllocationBinding::Ledger { stable_key, owner }
                }
                ic_memory::AllocationBinding::Unknown => MemoryAllocationBinding::Unknown,
            },
            range_claim: memory.range_claim.map(|claim| MemoryAllocationRangeClaim {
                authority: claim.authority,
                reserved: match claim.mode {
                    ic_memory::MemoryManagerRangeMode::Reserved => true,
                    ic_memory::MemoryManagerRangeMode::Allowed => false,
                },
            }),
            virtual_extent: memory.virtual_extent.into(),
            allocated_buckets: memory.allocated_buckets,
            allocated_bytes: memory.allocated_bytes,
            bucket_slack_bytes: memory.bucket_slack_bytes,
            payload_bytes: memory.payload_bytes,
        }
    }
}

// Inspect existing TLS only. Heap-only/native callers can have no default
// runtime; a corrupt or inaccessible runtime must not look like absence.
pub(super) fn collect_memory_allocations() -> Result<Option<MemoryAllocations>, InternalError> {
    match ic_memory::default_memory_manager_memory_allocations() {
        Ok(report) => Ok(Some(report.into())),
        Err(ic_memory::RuntimeDiagnosticError::NotBootstrapped) => Ok(None),
        Err(_) => Err(InternalError::store_internal()),
    }
}
