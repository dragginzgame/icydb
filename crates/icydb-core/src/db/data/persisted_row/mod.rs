//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: field persistence policy, row envelope versions, typed entity
//! materialization, or query semantics.
//! Boundary: commit/index planning, row writes, and typed materialization all
//! consume the canonical slot-oriented persisted-row boundary here.
//!
//! Runtime `Value` appears here only at outer row adapters for structural
//! writes, reads, projection, and patch replay. Accepted field contracts own
//! persisted storage selection.

mod canonical;
mod codec;
mod contract;
mod patch;
mod reader;
mod types;

#[cfg(test)]
mod tests;

pub(in crate::db) use canonical::validate_default_payload_for_accepted_field_contract;
pub(in crate::db) use canonical::{
    encode_canonical_value_for_accepted_field_contract,
    encode_input_value_for_accepted_field_contract,
};
#[cfg(test)]
pub(in crate::db) use contract::encode_value_with_model_proposal_for_test;
pub(in crate::db) use contract::{
    decode_runtime_value_from_accepted_field_contract, decode_runtime_value_from_row_contract,
};
#[cfg(feature = "sql")]
pub(in crate::db) use patch::canonical_row_from_dense_slot_payloads;
pub(in crate::db) use patch::{
    apply_serialized_structural_patch_to_raw_row_with_accepted_contract,
    canonical_row_from_complete_serialized_structural_patch_with_accepted_contract,
    canonical_row_from_entity_with_accepted_contract,
    canonical_row_from_raw_row_with_accepted_decode_contract,
    canonical_row_from_raw_row_with_structural_contract, canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader_with_accepted_contract,
    merge_non_generated_slots_into_canonical_row_with_accepted_contract,
    serialize_complete_structural_patch_fields_with_accepted_contract,
    serialize_structural_patch_fields_with_accepted_contract,
};
#[cfg(test)]
pub(in crate::db) use patch::{
    canonical_row_from_complete_serialized_structural_patch_for_model_proposal_for_test,
    canonical_row_from_entity_for_model_proposal_for_test,
    materialize_entity_from_serialized_structural_patch_for_model_proposal_for_test,
};
#[cfg(feature = "diagnostics")]
pub use reader::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use reader::{StructuralReadMetrics, with_structural_read_metrics};
pub(in crate::db) use reader::{
    StructuralSlotReader, decode_dense_raw_row_with_contract,
    decode_sparse_indexed_raw_row_with_contract, decode_sparse_raw_row_with_contract,
    decode_sparse_required_slot_with_contract,
};
pub use types::{AuthoredStructuralPatch, PersistedRow, SlotReader};
pub(in crate::db) use types::{CanonicalSlotReader, FieldSlot, SerializedStructuralPatch};
// These helpers remain public inside `icydb-core` because the cross-crate
// `icydb::__macro` boundary still needs a stable path for generated code.
pub use codec::{
    PersistedByKindCodec, PersistedScalar, PersistedStructuralValueCodec, ScalarSlotValueRef,
    ScalarValueRef, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload_by_kind, decode_persisted_scalar_slot_payload,
    decode_persisted_slot_payload_by_kind, decode_persisted_structured_many_slot_payload,
    decode_persisted_structured_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload_by_kind,
    encode_persisted_structured_many_slot_payload, encode_persisted_structured_slot_payload,
};
