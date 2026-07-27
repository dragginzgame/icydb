//! Module: db::data::persisted_row::codec
//! Defines the persisted row-slot codec boundary and re-exports the leaf codec
//! families owned by this directory module.

mod scalar;

pub(crate) use scalar::{ScalarSlotValueRef, ScalarValueRef};
pub(super) use scalar::{decode_scalar_slot_value, encode_scalar_slot_value};
