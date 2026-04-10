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
pub(in crate::db::data::persisted_row) use contract::decode_slot_value_by_contract;
#[cfg(test)]
pub(in crate::db::data::persisted_row) use contract::decode_slot_value_from_bytes;
#[cfg(test)]
pub(in crate::db::data::persisted_row) use contract::encode_slot_payload_from_parts;
#[cfg(test)]
pub(in crate::db::data::persisted_row) use contract::encode_slot_value_from_value;
#[cfg(test)]
pub(in crate::db) use patch::apply_update_patch_to_raw_row;
#[cfg(test)]
pub(in crate::db::data::persisted_row) use patch::canonical_row_from_raw_row;
pub(in crate::db) use patch::{
    apply_serialized_update_patch_to_raw_row, canonical_row_from_entity,
    canonical_row_from_serialized_update_patch, canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader, serialize_entity_slots_as_update_patch,
    serialize_update_patch_fields,
};
#[cfg(test)]
pub(in crate::db::data::persisted_row) use reader::CachedSlotValue;
#[cfg(feature = "structural-read-metrics")]
pub use reader::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "structural-read-metrics")))]
pub(crate) use reader::{StructuralReadMetrics, with_structural_read_metrics};
pub(in crate::db) use reader::{StructuralSlotReader, decode_dense_raw_row_with_contract};
#[cfg(test)]
pub(in crate::db::data::persisted_row) use types::FieldSlot;
pub(in crate::db) use types::{CanonicalSlotReader, SerializedUpdatePatch};
pub use types::{PersistedRow, SlotReader, SlotWriter, UpdatePatch};
#[cfg(test)]
pub(in crate::db::data::persisted_row) use writer::{SerializedPatchWriter, SlotBufferWriter};

#[cfg(test)]
pub(in crate::db::data::persisted_row) use codec::encode_scalar_slot_value;
pub use codec::{
    PersistedScalar, ScalarSlotValueRef, ScalarValueRef, decode_persisted_custom_many_slot_payload,
    decode_persisted_custom_slot_payload, decode_persisted_non_null_slot_payload,
    decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload,
    encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
    encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload,
};
#[cfg(test)]
pub(in crate::db::data::persisted_row) use types::SerializedFieldUpdate;

///
/// TESTS
///

#[cfg(test)]
mod tests;
