//! Module: data::structural_row
//! Responsibility: canonical structural persisted-row decode helpers.
//! Does not own: typed entity reconstruction, slot layout planning, or query semantics.
//! Boundary: runtime paths use this module when they need persisted-row structure without `E`.

use crate::{
    db::{codec::deserialize_row, data::RawRow},
    error::InternalError,
    model::entity::EntityModel,
};
use serde_cbor::Value as CborValue;
use std::collections::BTreeMap;
use thiserror::Error as ThisError;

///
/// StructuralRowObject
///
/// StructuralRowObject is the canonical top-level structural persisted-row
/// representation for runtime paths that do not need typed entity
/// reconstruction.
///
/// It owns one decoded CBOR object map and provides stable field lookup by
/// persisted field name.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowObject {
    fields: BTreeMap<CborValue, CborValue>,
}

impl StructuralRowObject {
    /// Decode one raw persisted row into the canonical structural row object.
    pub(in crate::db) fn from_raw_row(raw_row: &RawRow) -> Result<Self, StructuralRowDecodeError> {
        let fields = decode_structural_row_fields(raw_row)?;

        Ok(Self { fields })
    }

    /// Borrow one field value by persisted field name.
    #[must_use]
    pub(in crate::db) fn field(&self, name: &str) -> Option<&CborValue> {
        self.fields.get(&CborValue::Text(name.to_string()))
    }

    /// Project this row object into model slot order without typed reconstruction.
    #[must_use]
    pub(in crate::db) fn slots_for_model(
        &self,
        model: &'static EntityModel,
    ) -> StructuralRowSlots<'_> {
        StructuralRowSlots::from_object(self, model)
    }
}

///
/// StructuralRowSlots
///
/// StructuralRowSlots is the slot-indexed structural view of one persisted
/// row.
/// It borrows the canonical top-level row object and projects fields into
/// model slot order so runtime paths can stay slot-driven without reconstructing
/// typed entities.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowSlots<'a> {
    slots: Vec<Option<&'a CborValue>>,
}

impl<'a> StructuralRowSlots<'a> {
    /// Build one slot-indexed structural row view for one entity model.
    #[must_use]
    pub(in crate::db) fn from_object(
        row_object: &'a StructuralRowObject,
        model: &'static EntityModel,
    ) -> Self {
        let slots = model
            .fields()
            .iter()
            .map(|field| row_object.field(field.name()))
            .collect();

        Self { slots }
    }

    /// Borrow one raw persisted field payload by stable slot index.
    #[must_use]
    pub(in crate::db) fn field(&self, slot: usize) -> Option<&'a CborValue> {
        self.slots.get(slot).copied().flatten()
    }
}

///
/// StructuralRowDecodeError
///
/// StructuralRowDecodeError captures shape failures after persisted-row bytes
/// have already decoded successfully through the shared structural CBOR path.
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum StructuralRowDecodeError {
    #[error(transparent)]
    Deserialize(#[from] InternalError),

    #[error("expected top-level CBOR map")]
    ExpectedTopLevelMap,
}

/// Decode one persisted row through the canonical structural CBOR path.
pub(in crate::db) fn decode_structural_row_cbor(
    raw_row: &RawRow,
) -> Result<CborValue, InternalError> {
    let decoded = deserialize_row::<CborValue>(raw_row.as_bytes())?;

    Ok(unwrap_structural_row_cbor_tags(decoded))
}

/// Strip transparent CBOR tags before structural row interpretation.
#[must_use]
pub(in crate::db) fn unwrap_structural_row_cbor_tags(mut value: CborValue) -> CborValue {
    while let CborValue::Tag(_, inner) = value {
        value = *inner;
    }

    value
}

// Decode the top-level persisted-row object map once for all structural callers.
fn decode_structural_row_fields(
    raw_row: &RawRow,
) -> Result<BTreeMap<CborValue, CborValue>, StructuralRowDecodeError> {
    let decoded = decode_structural_row_cbor(raw_row)?;
    let CborValue::Map(map) = decoded else {
        return Err(StructuralRowDecodeError::ExpectedTopLevelMap);
    };

    Ok(map)
}
