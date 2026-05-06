//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: field persistence policy, row envelope versions, typed entity
//! materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.
//!
//! Runtime `Value` appears here only at outer row adapters for structural
//! writes, reads, projection, and patch replay. Persisted field storage remains
//! owned by field types through `PersistedFieldSlotCodec`.

mod codec;
mod contract;
mod patch;
mod reader;
mod types;
mod writer;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use contract::decode_runtime_value_from_accepted_field_contract;
#[doc(hidden)]
pub use contract::{decode_slot_into_runtime_value, encode_runtime_value_into_slot};
pub(in crate::db) use patch::{
    apply_serialized_structural_patch_to_raw_row,
    apply_serialized_structural_patch_to_raw_row_with_accepted_contract,
    canonical_row_from_complete_serialized_structural_patch, canonical_row_from_entity,
    canonical_row_from_raw_row_with_structural_contract, canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader, materialize_entity_from_serialized_structural_patch,
    serialize_entity_slots_as_complete_serialized_patch, serialize_structural_patch_fields,
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
pub(in crate::db) use types::{CanonicalSlotReader, FieldSlot, SerializedStructuralPatch};
pub use types::{PersistedRow, SlotReader, SlotWriter, StructuralPatch};
// These helpers remain public inside `icydb-core` because the cross-crate
// `icydb::__macro` boundary still needs a stable path for generated code.
pub use codec::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef,
    decode_persisted_many_slot_payload_by_meta, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload_by_kind, decode_persisted_option_slot_payload_by_meta,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload_by_kind,
    decode_persisted_slot_payload_by_meta, decode_persisted_structured_many_slot_payload,
    decode_persisted_structured_slot_payload, encode_persisted_many_slot_payload_by_meta,
    encode_persisted_option_scalar_slot_payload, encode_persisted_option_slot_payload_by_meta,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload_by_kind,
    encode_persisted_slot_payload_by_meta, encode_persisted_structured_many_slot_payload,
    encode_persisted_structured_slot_payload,
};
