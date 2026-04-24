mod cache;
mod core;
mod direct;
mod metrics;
mod primary_key;

#[cfg(test)]
pub(in crate::db::data::persisted_row) use cache::CachedSlotValue;
pub(in crate::db) use core::StructuralSlotReader;
pub(in crate::db) use direct::{
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
    decode_sparse_required_slot_with_contract_and_fields,
};
#[cfg(feature = "diagnostics")]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
pub use metrics::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use metrics::{StructuralReadMetrics, with_structural_read_metrics};
