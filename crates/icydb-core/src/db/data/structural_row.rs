//! Module: data::structural_row
//! Responsibility: canonical structural persisted-row decode helpers.
//! Does not own: typed entity reconstruction, slot layout planning, or query semantics.
//! Boundary: runtime paths use this module when they need persisted-row structure without `E`.

use crate::{
    db::{codec::decode_row_payload_bytes, data::RawRow},
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
};
use std::borrow::Cow;
use thiserror::Error as ThisError;

type SlotSpan = Option<(usize, usize)>;
type SlotSpans = Vec<SlotSpan>;
type RowFieldSpans<'a> = (Cow<'a, [u8]>, SlotSpans);

///
/// StructuralRowContract
///
/// StructuralRowContract is the compact static row-shape authority used by
/// structural row readers that do not need the full semantic `EntityModel`.
/// It keeps only the entity path, field table, and primary-key slot required
/// to open canonical persisted rows through the data-layer decode boundary.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) struct StructuralRowContract {
    entity_path: &'static str,
    fields: &'static [FieldModel],
    primary_key_slot: usize,
}

impl StructuralRowContract {
    /// Build one structural row contract from the generated entity model.
    #[must_use]
    pub(in crate::db) const fn from_model(model: &'static EntityModel) -> Self {
        Self {
            entity_path: model.path(),
            fields: model.fields(),
            primary_key_slot: model.primary_key_slot(),
        }
    }

    /// Borrow the owning entity path for diagnostics.
    #[must_use]
    pub(in crate::db) const fn entity_path(self) -> &'static str {
        self.entity_path
    }

    /// Borrow the static field table for slot-indexed decode.
    #[must_use]
    pub(in crate::db) const fn fields(self) -> &'static [FieldModel] {
        self.fields
    }

    /// Return the declared structural field count.
    #[must_use]
    pub(in crate::db) const fn field_count(self) -> usize {
        self.fields.len()
    }

    /// Return the authoritative primary-key slot.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot(self) -> usize {
        self.primary_key_slot
    }
}

///
/// StructuralRowFieldBytes
///
/// StructuralRowFieldBytes is the top-level persisted-row field scanner for
/// slot-driven proof paths.
/// It keeps the original encoded field payload bytes and records one byte span
/// per model slot so callers can decode only the fields they actually need.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowFieldBytes<'a> {
    payload: Cow<'a, [u8]>,
    spans: SlotSpans,
}

impl<'a> StructuralRowFieldBytes<'a> {
    /// Decode one raw row payload into contract slot-aligned encoded field spans.
    pub(in crate::db) fn from_row_bytes_with_contract(
        row_bytes: &'a [u8],
        contract: StructuralRowContract,
    ) -> Result<Self, StructuralRowDecodeError> {
        let payload = decode_structural_row_payload_bytes(row_bytes)?;
        let (payload, spans) = decode_row_field_spans(payload, contract)?;

        Ok(Self { payload, spans })
    }

    /// Decode one raw row into model slot-aligned encoded field payload spans.
    pub(in crate::db) fn from_raw_row(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, StructuralRowDecodeError> {
        Self::from_raw_row_with_contract(raw_row, StructuralRowContract::from_model(model))
    }

    /// Decode one raw row into contract slot-aligned encoded field payload spans.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, StructuralRowDecodeError> {
        Self::from_row_bytes_with_contract(raw_row.as_bytes(), contract)
    }

    /// Borrow one encoded persisted field payload by stable slot index.
    #[must_use]
    pub(in crate::db) fn field(&self, slot: usize) -> Option<&[u8]> {
        let (start, end) = self.spans.get(slot).copied().flatten()?;

        Some(&self.payload[start..end])
    }
}

///
/// SparseRequiredRowFieldBytes
///
/// SparseRequiredRowFieldBytes carries the shared payload plus just the two
/// slot spans needed by the narrow sparse required-slot decode path.
/// Executor one-slot reads use this to preserve full row-table validation
/// without allocating one field-count-sized span vector on every row.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SparseRequiredRowFieldBytes<'a> {
    payload: Cow<'a, [u8]>,
    required_span: (usize, usize),
    primary_key_span: (usize, usize),
}

impl<'a> SparseRequiredRowFieldBytes<'a> {
    /// Decode one raw row into the selected and primary-key field spans needed
    /// by sparse direct slot reads.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
        required_slot: usize,
    ) -> Result<Self, StructuralRowDecodeError> {
        let payload = decode_structural_row_payload_bytes(raw_row.as_bytes())?;
        let (payload, required_span, primary_key_span) =
            decode_sparse_required_row_field_spans(payload, contract, required_slot)?;

        Ok(Self {
            payload,
            required_span,
            primary_key_span,
        })
    }

    /// Borrow the selected required field payload bytes.
    #[must_use]
    pub(in crate::db) fn required_field(&self) -> &[u8] {
        &self.payload[self.required_span.0..self.required_span.1]
    }

    /// Borrow the primary-key field payload bytes.
    #[must_use]
    pub(in crate::db) fn primary_key_field(&self) -> &[u8] {
        &self.payload[self.primary_key_span.0..self.primary_key_span.1]
    }
}

///
/// StructuralRowDecodeError
///
/// StructuralRowDecodeError captures shape failures after persisted-row bytes
/// have already decoded successfully through the shared structural path.
///

#[derive(Debug, ThisError)]
pub(in crate::db) enum StructuralRowDecodeError {
    #[error(transparent)]
    Deserialize(#[from] InternalError),
}

impl StructuralRowDecodeError {
    // Collapse the local structural decode wrapper back into the internal taxonomy.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Deserialize(err) => err,
        }
    }

    // Build one structural row corruption error at the manual decode boundary.
    fn corruption(message: impl Into<String>) -> Self {
        Self::Deserialize(InternalError::serialize_corruption(message.into()))
    }
}

/// Decode one persisted row through the structural row-envelope validation path.
///
/// The only supported persisted row shape is the slot-framed payload envelope,
/// so this helper returns the validated enclosed payload bytes directly.
pub(in crate::db) fn decode_structural_row_payload(
    raw_row: &RawRow,
) -> Result<Cow<'_, [u8]>, InternalError> {
    decode_structural_row_payload_bytes(raw_row.as_bytes())
        .map_err(StructuralRowDecodeError::into_internal_error)
}

// Decode one persisted row envelope into the enclosed slot payload bytes.
fn decode_structural_row_payload_bytes(
    bytes: &[u8],
) -> Result<Cow<'_, [u8]>, StructuralRowDecodeError> {
    decode_row_payload_bytes(bytes).map_err(StructuralRowDecodeError::from)
}

// Decode the canonical slot-container header into slot-aligned payload spans.
fn decode_row_field_spans(
    payload: Cow<'_, [u8]>,
    contract: StructuralRowContract,
) -> Result<RowFieldSpans<'_>, StructuralRowDecodeError> {
    let bytes = payload.as_ref();
    let field_count_bytes = bytes
        .get(..2)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot header"))?;
    let field_count = usize::from(u16::from_be_bytes([
        field_count_bytes[0],
        field_count_bytes[1],
    ]));
    if field_count != contract.field_count() {
        return Err(StructuralRowDecodeError::corruption(format!(
            "row decode: slot count mismatch: expected {}, found {}",
            contract.field_count(),
            field_count,
        )));
    }
    let table_len = field_count
        .checked_mul(8)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: slot table overflow"))?;
    let data_start = 2usize.checked_add(table_len).ok_or_else(|| {
        StructuralRowDecodeError::corruption("row decode: slot payload header overflow")
    })?;
    let table = bytes
        .get(2..data_start)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot table"))?;
    let data_section = bytes
        .get(data_start..)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: missing slot payloads"))?;
    let mut spans: SlotSpans = vec![None; contract.field_count()];

    for (slot, span) in spans.iter_mut().enumerate() {
        let entry_start = slot.checked_mul(8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot index overflow")
        })?;
        let entry = table.get(entry_start..entry_start + 8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: truncated slot table entry")
        })?;
        let start = usize::try_from(u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]))
            .map_err(|_| {
            StructuralRowDecodeError::corruption("row decode: slot start out of range")
        })?;
        let len = usize::try_from(u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]))
            .map_err(|_| {
                StructuralRowDecodeError::corruption("row decode: slot length out of range")
            })?;
        if len == 0 {
            return Err(StructuralRowDecodeError::corruption(format!(
                "row decode: missing slot payload: slot={slot}",
            )));
        }
        let end = start.checked_add(len).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot span overflow")
        })?;
        if end > data_section.len() {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: slot span exceeds payload length",
            ));
        }
        *span = Some((start, end));
    }

    let payload = match payload {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[data_start..]),
        Cow::Owned(bytes) => Cow::Owned(bytes[data_start..].to_vec()),
    };

    Ok((payload, spans))
}

// Decode the canonical slot-container header while retaining only one required
// slot span plus the primary-key span for sparse direct slot reads.
fn decode_sparse_required_row_field_spans(
    payload: Cow<'_, [u8]>,
    contract: StructuralRowContract,
    required_slot: usize,
) -> Result<(Cow<'_, [u8]>, (usize, usize), (usize, usize)), StructuralRowDecodeError> {
    let bytes = payload.as_ref();
    let field_count_bytes = bytes
        .get(..2)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot header"))?;
    let field_count = usize::from(u16::from_be_bytes([
        field_count_bytes[0],
        field_count_bytes[1],
    ]));
    if field_count != contract.field_count() {
        return Err(StructuralRowDecodeError::corruption(format!(
            "row decode: slot count mismatch: expected {}, found {}",
            contract.field_count(),
            field_count,
        )));
    }
    let table_len = field_count
        .checked_mul(8)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: slot table overflow"))?;
    let data_start = 2usize.checked_add(table_len).ok_or_else(|| {
        StructuralRowDecodeError::corruption("row decode: slot payload header overflow")
    })?;
    let table = bytes
        .get(2..data_start)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: truncated slot table"))?;
    let data_section = bytes
        .get(data_start..)
        .ok_or_else(|| StructuralRowDecodeError::corruption("row decode: missing slot payloads"))?;
    let primary_key_slot = contract.primary_key_slot();
    let mut required_span = None;
    let mut primary_key_span = None;

    for slot in 0..contract.field_count() {
        let entry_start = slot.checked_mul(8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot index overflow")
        })?;
        let entry = table.get(entry_start..entry_start + 8).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: truncated slot table entry")
        })?;
        let start = usize::try_from(u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]))
            .map_err(|_| {
            StructuralRowDecodeError::corruption("row decode: slot start out of range")
        })?;
        let len = usize::try_from(u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]))
            .map_err(|_| {
                StructuralRowDecodeError::corruption("row decode: slot length out of range")
            })?;
        if len == 0 {
            return Err(StructuralRowDecodeError::corruption(format!(
                "row decode: missing slot payload: slot={slot}",
            )));
        }
        let end = start.checked_add(len).ok_or_else(|| {
            StructuralRowDecodeError::corruption("row decode: slot span overflow")
        })?;
        if end > data_section.len() {
            return Err(StructuralRowDecodeError::corruption(
                "row decode: slot span exceeds payload length",
            ));
        }
        if slot == required_slot {
            required_span = Some((start, end));
        }
        if slot == primary_key_slot {
            primary_key_span = Some((start, end));
        }
    }

    let required_span = required_span.ok_or_else(|| {
        StructuralRowDecodeError::corruption(format!(
            "row decode: missing required slot span: slot={required_slot}",
        ))
    })?;
    let primary_key_span = primary_key_span.ok_or_else(|| {
        StructuralRowDecodeError::corruption(format!(
            "row decode: missing primary-key slot span: slot={primary_key_slot}",
        ))
    })?;
    let payload = match payload {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[data_start..]),
        Cow::Owned(bytes) => Cow::Owned(bytes[data_start..].to_vec()),
    };

    Ok((payload, required_span, primary_key_span))
}
