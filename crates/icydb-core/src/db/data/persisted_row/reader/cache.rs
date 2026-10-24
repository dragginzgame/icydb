//! Module: db::data::persisted_row::reader::cache
//! Responsibility: lazy validated and materialized persisted-row slot state.
//! Does not own: field decode policy or row-envelope validation.
//! Boundary: `StructuralSlotReader` retains borrowed scalar views and runtime values here.

use crate::{db::data::ScalarSlotValueRef, value::Value};
use std::cell::OnceCell;

/// Validated scalar views borrow the immutable row payload, including text and
/// blobs. Materialization reuses that admission rather than decoding again.
/// Structural fields leave the scalar cell empty; their accepted codec is
/// resolved only when accessed, rather than once per slot on every row open.
#[derive(Debug, Default)]
pub(in crate::db::data::persisted_row) struct CachedSlotValue<'a> {
    pub(in crate::db::data::persisted_row) validated: OnceCell<ScalarSlotValueRef<'a>>,
    pub(in crate::db::data::persisted_row) materialized: OnceCell<Value>,
}

// Dense accepted slots all start empty; no field metadata or payload is needed.
pub(super) fn build_initial_slot_cache<'a>(field_count: usize) -> Vec<CachedSlotValue<'a>> {
    std::iter::repeat_with(CachedSlotValue::default)
        .take(field_count)
        .collect()
}
