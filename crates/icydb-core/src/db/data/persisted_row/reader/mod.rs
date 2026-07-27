mod cache;
#[cfg(any(test, feature = "sql"))]
mod direct;
mod metrics;
mod primary_key;
mod structural_slot_reader;

#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use direct::{
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_required_slot_with_contract,
};
#[cfg(feature = "diagnostics")]
#[cfg_attr(all(test, not(feature = "diagnostics")), expect(unreachable_pub))]
pub use metrics::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use metrics::{StructuralReadMetrics, with_structural_read_metrics};
pub(in crate::db) use structural_slot_reader::StructuralSlotReader;
