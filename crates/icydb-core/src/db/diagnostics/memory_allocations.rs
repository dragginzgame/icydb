//! Project Storage's checked directory entries into passive diagnostic DTOs.
//! Logical extents are addressable bytes, not payload occupancy or physical usage.

#[cfg(test)]
mod tests;

use crate::{db::memory_runtime, error::InternalError};
use serde::Deserialize;

/// Checked named allocations in the host-owned Storage directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub struct MemoryAllocations {
    /// Durable directory generation; this does not confer current admission.
    pub current_generation: u64,
    /// Total addressable logical extent across retained allocations.
    pub virtual_extent: MemoryExtent,
    /// Retained names and opaque allocation identities.
    pub memories: Vec<MemoryAllocation>,
}

impl MemoryAllocations {
    fn from_runtime<M: ic_storage::Memory>(
        runtime: &ic_storage::MemoryRuntime<M>,
    ) -> Result<Self, InternalError> {
        let entries = runtime
            .memory_allocations()
            .map_err(|_| InternalError::store_internal())?;
        let mut pages = 0u64;
        let mut memories = Vec::with_capacity(entries.len());
        for entry in entries {
            pages = pages
                .checked_add(entry.pages)
                .ok_or_else(InternalError::store_internal)?;
            memories.push(MemoryAllocation {
                allocation_id: entry.id.get(),
                stable_key: entry.stable_key.to_string(),
                virtual_extent: MemoryExtent::from_pages(entry.pages)?,
            });
        }
        Ok(Self {
            current_generation: runtime
                .committed_generation()
                .map_err(|_| InternalError::store_internal())?,
            virtual_extent: MemoryExtent::from_pages(pages)?,
            memories,
        })
    }
}

/// Measured logical extent in 64-KiB Wasm pages and bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub struct MemoryExtent {
    /// Extent in Wasm pages.
    pub wasm_pages: u64,
    /// Extent in bytes.
    pub bytes: u64,
}

impl MemoryExtent {
    fn from_pages(wasm_pages: u64) -> Result<Self, InternalError> {
        Ok(Self {
            wasm_pages,
            bytes: wasm_pages
                .checked_mul(65_536)
                .ok_or_else(InternalError::store_internal)?,
        })
    }
}

/// A retained directory entry. Presence does not grant opening authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub struct MemoryAllocation {
    /// Opaque Storage identity, never a selectable memory-manager slot.
    pub allocation_id: u64,
    /// Durable retained allocation name.
    pub stable_key: String,
    /// Addressable logical extent, not payload occupancy.
    pub virtual_extent: MemoryExtent,
}

pub(super) fn collect_memory_allocations() -> Result<Option<MemoryAllocations>, InternalError> {
    memory_runtime()
        .as_deref()
        .map(MemoryAllocations::from_runtime)
        .transpose()
}
