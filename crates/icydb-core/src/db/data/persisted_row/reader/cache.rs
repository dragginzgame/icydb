//! Module: db::data::persisted_row::reader::cache
//! Responsibility: lazy validated and materialized persisted-row slot state.
//! Does not own: field decode policy or row-envelope validation.
//! Boundary: `StructuralSlotReader` retains borrowed scalar views and runtime values here.

use crate::{
    db::{
        data::{ScalarSlotValueRef, StructuralRowContract},
        schema::LeafCodec,
    },
    error::InternalError,
    value::Value,
};
use std::cell::OnceCell;

/// Validated scalar views borrow the immutable row payload, including text and
/// blobs. Materialization reuses that admission rather than decoding again.
#[derive(Debug)]
pub(in crate::db::data::persisted_row) enum CachedSlotValue<'a> {
    Scalar {
        validated: OnceCell<ScalarSlotValueRef<'a>>,
        materialized: OnceCell<Value>,
    },
    Deferred {
        materialized: OnceCell<Value>,
    },
}

// Select cache shape from the accepted contract without decoding any payload.
pub(super) fn build_initial_slot_cache<'a>(
    contract: &StructuralRowContract,
) -> Result<Vec<CachedSlotValue<'a>>, InternalError> {
    (0..contract.field_count())
        .map(|slot| {
            Ok(match contract.field_leaf_codec(slot)? {
                LeafCodec::Scalar(_) => CachedSlotValue::Scalar {
                    validated: OnceCell::new(),
                    materialized: OnceCell::new(),
                },
                LeafCodec::Structural => CachedSlotValue::Deferred {
                    materialized: OnceCell::new(),
                },
            })
        })
        .collect()
}
