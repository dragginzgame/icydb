//! Module: data::persisted_row
//! Responsibility: slot-oriented persisted-row seams over runtime row bytes.
//! Does not own: field persistence policy, row envelope versions, typed entity
//! materialization, or query semantics.
//! Boundary: commit/index planning and structural row writes consume the
//! canonical slot-oriented persisted-row boundary here.
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

#[cfg(feature = "sql")]
pub(in crate::db) use canonical::encode_input_value_for_accepted_field_contract;
pub(in crate::db) use canonical::validate_default_payload_for_accepted_field_contract;
pub(in crate::db) use canonical::{
    decode_admitted_value_from_accepted_field_contract,
    encode_accepted_value_ref_for_accepted_field_contract,
    encode_canonical_value_for_accepted_field_contract,
};
pub(in crate::db) use canonical::{
    decode_validated_check_literal_payload, encode_input_value_for_candidate_field_contract,
};
pub(in crate::db) use codec::{ScalarSlotValueRef, ScalarValueRef};
#[cfg(any(test, feature = "migration"))]
pub(in crate::db) use contract::canonical_row_from_runtime_value_source_with_accepted_contract;
pub(in crate::db) use contract::{
    decode_runtime_value_from_accepted_field_contract, decode_runtime_value_from_row_contract,
};
#[cfg(feature = "sql")]
pub(in crate::db) use patch::AcceptedFixedUpdatePatch;
pub(in crate::db) use patch::{
    AcceptedFieldWriteProvenance, canonical_row_from_raw_row_with_accepted_decode_contract,
    canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader_with_accepted_contract,
    resolve_existing_replace_structural_patch_with_accepted_contract,
    resolve_insert_structural_patch_with_accepted_contract,
    resolve_update_structural_patch_with_accepted_contract,
};
pub(in crate::db) use reader::StructuralSlotReader;
pub(in crate::db) use reader::{
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_required_slot_with_contract,
};
pub(in crate::db) use types::SlotReader;
pub(in crate::db) use types::{
    AcceptedMutationIntentPatch, AcceptedPreKeyInsert, CanonicalSlotReader, FieldSlot,
};
