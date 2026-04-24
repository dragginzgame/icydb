//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: row envelope versions, typed entity materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.

mod codec;
mod contract;
mod patch;
mod reader;
mod types;
mod writer;

#[cfg(test)]
mod tests;

pub(in crate::db) use patch::{
    apply_serialized_update_patch_to_raw_row, canonical_row_from_complete_serialized_update_patch,
    canonical_row_from_entity, canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader, materialize_entity_from_serialized_update_patch,
    serialize_entity_slots_as_complete_serialized_patch, serialize_update_patch_fields,
};
#[cfg(feature = "diagnostics")]
pub use reader::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use reader::{StructuralReadMetrics, with_structural_read_metrics};
pub(in crate::db) use reader::{
    StructuralSlotReader, decode_dense_raw_row_with_contract,
    decode_sparse_indexed_raw_row_with_contract, decode_sparse_raw_row_with_contract,
    decode_sparse_required_slot_with_contract,
    decode_sparse_required_slot_with_contract_and_fields,
};
pub(in crate::db) use types::{CanonicalSlotReader, SerializedUpdatePatch};
pub use types::{PersistedRow, SlotReader, SlotWriter, UpdatePatch};
// These helpers remain public inside `icydb-core` because the cross-crate
// `icydb::__macro` boundary still needs a stable path for generated code.
pub use codec::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef, decode_persisted_custom_many_slot_payload,
    decode_persisted_custom_slot_payload, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload_by_kind, decode_persisted_option_slot_payload_by_meta,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload_by_kind,
    decode_persisted_slot_payload_by_meta, encode_persisted_custom_many_slot_payload,
    encode_persisted_custom_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_option_slot_payload_by_meta, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload_by_kind, encode_persisted_slot_payload_by_meta,
};
