//! Module: data::structural_row
//! Responsibility: canonical structural persisted-row decode helpers.
//! Does not own: typed entity reconstruction, slot layout planning, or query semantics.
//! Boundary: runtime paths use this module when they need persisted-row structure without `E`.

use crate::{
    db::{
        codec::decode_row_payload_bytes,
        data::RawRow,
        schema::{AcceptedFieldDecodeContract, AcceptedRowDecodeContract},
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
};
use std::borrow::Cow;
use std::sync::Arc;
use thiserror::Error as ThisError;

type SlotSpan = Option<(usize, usize)>;
type SlotSpans = Vec<SlotSpan>;
type RowFieldSpans<'a> = (Cow<'a, [u8]>, SlotSpans);
type RowSlotTableSections<'a> = (usize, &'a [u8], &'a [u8]);

///
/// StructuralRowContract
///
/// StructuralRowContract is the compact static row-shape authority used by
/// structural row readers that do not need the full semantic `EntityModel`.
/// It keeps the entity path, generated-compatible field bridge, declared field
/// count, and primary-key slot required to open canonical persisted rows
/// through the data-layer decode boundary.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowContract {
    entity_path: &'static str,
    generated_fields: &'static [FieldModel],
    field_count: usize,
    primary_key_slot: usize,
    accepted_decode_contract: Option<Arc<AcceptedRowDecodeContract>>,
}

impl StructuralRowContract {
    /// Build one structural row contract from the generated entity model.
    #[must_use]
    pub(in crate::db) const fn from_model(model: &'static EntityModel) -> Self {
        Self {
            entity_path: model.path(),
            generated_fields: model.fields(),
            field_count: model.fields().len(),
            primary_key_slot: model.primary_key_slot(),
            accepted_decode_contract: None,
        }
    }

    /// Build one structural row contract from a generated model plus an owned
    /// accepted row-decode contract.
    #[must_use]
    pub(in crate::db) fn from_model_with_accepted_decode_contract(
        model: &'static EntityModel,
        accepted_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        Self {
            entity_path: model.path(),
            generated_fields: model.fields(),
            field_count: accepted_decode_contract.required_slot_count(),
            primary_key_slot: accepted_decode_contract.primary_key_slot_index(),
            accepted_decode_contract: Some(Arc::new(accepted_decode_contract)),
        }
    }

    /// Borrow the owning entity path for diagnostics.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Borrow one generated-compatible field model by structural slot.
    ///
    /// This remains a transitional adapter for the public `SlotReader`
    /// materialization trait and write-side generated codecs. Runtime decode
    /// code should prefer `field_decode_contract` whenever it only needs
    /// field decode facts.
    pub(in crate::db) fn generated_compatible_field_model(
        &self,
        slot: usize,
    ) -> Result<&'static FieldModel, InternalError> {
        self.generated_fields.get(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(self.entity_path(), slot)
        })
    }

    /// Return the declared structural field count.
    #[must_use]
    pub(in crate::db) const fn field_count(&self) -> usize {
        self.field_count
    }

    /// Return the authoritative primary-key slot.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot(&self) -> usize {
        self.primary_key_slot
    }

    /// Borrow one accepted field decode contract by physical row slot when
    /// this row contract was built from accepted schema authority.
    #[must_use]
    pub(in crate::db) fn accepted_field_decode_contract(
        &self,
        slot: usize,
    ) -> Option<AcceptedFieldDecodeContract<'_>> {
        self.accepted_decode_contract
            .as_ref()?
            .field_for_slot(slot)
            .map(|field| field.decode_contract())
    }

    /// Return the field-level decode contract for one structural slot.
    pub(in crate::db) fn field_decode_contract(
        &self,
        slot: usize,
    ) -> Result<StructuralFieldDecodeContract, InternalError> {
        self.generated_compatible_field_model(slot)
            .map(StructuralFieldDecodeContract::from_field_model)
    }

    /// Return the accepted-first leaf codec for one structural slot.
    ///
    /// This is the row-decode authority lookup for code paths that only need to
    /// decide scalar-vs-structural lane shape. Accepted saved-schema contracts
    /// take priority when present; generated-compatible fields remain the
    /// fallback for generated-only readers and compatibility bridges.
    pub(in crate::db) fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError> {
        if let Some(field) = self.accepted_field_decode_contract(slot) {
            return Ok(field.leaf_codec());
        }

        self.field_decode_contract(slot)
            .map(StructuralFieldDecodeContract::leaf_codec)
    }

    /// Return the persisted field name for diagnostics at one row slot.
    pub(in crate::db) fn field_name(&self, slot: usize) -> Result<&str, InternalError> {
        if let Some(field) = self.accepted_field_decode_contract(slot) {
            return Ok(field.field_name());
        }

        self.field_decode_contract(slot)
            .map(StructuralFieldDecodeContract::name)
    }
}

///
/// StructuralFieldDecodeContract
///
/// StructuralFieldDecodeContract is the narrow field-level decode shape used
/// by structural row readers once the owning row layout has already selected a
/// physical slot. It exists to keep value materialization on decode facts
/// instead of requiring every consumer to depend on the full generated
/// `FieldModel`.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) struct StructuralFieldDecodeContract {
    field_name: &'static str,
    kind: FieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    nullable: bool,
}

impl StructuralFieldDecodeContract {
    /// Build one decode contract from today's generated field metadata.
    #[must_use]
    pub(in crate::db) const fn from_field_model(field: &FieldModel) -> Self {
        Self {
            field_name: field.name(),
            kind: field.kind,
            storage_decode: field.storage_decode(),
            leaf_codec: field.leaf_codec(),
            nullable: field.nullable(),
        }
    }

    /// Borrow the field name used for diagnostics.
    #[must_use]
    pub(in crate::db) const fn name(self) -> &'static str {
        self.field_name
    }

    /// Return the field kind used by structural decoders.
    #[must_use]
    pub(in crate::db) const fn kind(self) -> FieldKind {
        self.kind
    }

    /// Return the storage decode lane for this field.
    #[must_use]
    pub(in crate::db) const fn storage_decode(self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the leaf codec for this field.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(self) -> LeafCodec {
        self.leaf_codec
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(self) -> bool {
        self.nullable
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
pub(in crate::db::data) struct StructuralRowFieldBytes<'a> {
    payload: Cow<'a, [u8]>,
    spans: SlotSpans,
}

impl<'a> StructuralRowFieldBytes<'a> {
    /// Decode one raw row payload into contract slot-aligned encoded field spans.
    fn from_row_bytes_with_contract(
        row_bytes: &'a [u8],
        contract: StructuralRowContract,
    ) -> Result<Self, StructuralRowDecodeError> {
        let payload = decode_structural_row_payload_bytes(row_bytes)?;
        let (payload, spans) = decode_row_field_spans(payload, &contract)?;

        Ok(Self { payload, spans })
    }

    /// Decode one raw row into model slot-aligned encoded field payload spans.
    pub(in crate::db::data) fn from_raw_row(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, StructuralRowDecodeError> {
        Self::from_raw_row_with_contract(raw_row, StructuralRowContract::from_model(model))
    }

    /// Decode one raw row into contract slot-aligned encoded field payload spans.
    pub(in crate::db::data) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, StructuralRowDecodeError> {
        Self::from_row_bytes_with_contract(raw_row.as_bytes(), contract)
    }

    /// Borrow one encoded persisted field payload by stable slot index.
    #[must_use]
    pub(in crate::db::data) fn field(&self, slot: usize) -> Option<&[u8]> {
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
pub(in crate::db::data) struct SparseRequiredRowFieldBytes<'a> {
    payload: Cow<'a, [u8]>,
    required_span: (usize, usize),
    primary_key_span: (usize, usize),
}

impl<'a> SparseRequiredRowFieldBytes<'a> {
    /// Decode one raw row into the selected and primary-key field spans needed
    /// by sparse direct slot reads.
    pub(in crate::db::data) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
        required_slot: usize,
    ) -> Result<Self, StructuralRowDecodeError> {
        let payload = decode_structural_row_payload_bytes(raw_row.as_bytes())?;
        let (payload, required_span, primary_key_span) =
            decode_sparse_required_row_field_spans(payload, &contract, required_slot)?;

        Ok(Self {
            payload,
            required_span,
            primary_key_span,
        })
    }

    /// Borrow the selected required field payload bytes.
    #[must_use]
    pub(in crate::db::data) fn required_field(&self) -> &[u8] {
        &self.payload[self.required_span.0..self.required_span.1]
    }

    /// Borrow the primary-key field payload bytes.
    #[must_use]
    pub(in crate::db::data) fn primary_key_field(&self) -> &[u8] {
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
pub(in crate::db::data) enum StructuralRowDecodeError {
    #[error(transparent)]
    Deserialize(#[from] InternalError),
}

impl StructuralRowDecodeError {
    // Collapse the local structural decode wrapper back into the internal taxonomy.
    pub(in crate::db::data) fn into_internal_error(self) -> InternalError {
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
fn decode_row_field_spans<'payload>(
    payload: Cow<'payload, [u8]>,
    contract: &StructuralRowContract,
) -> Result<RowFieldSpans<'payload>, StructuralRowDecodeError> {
    let bytes = payload.as_ref();
    let (data_start, table, data_section) = decode_slot_table_sections(bytes, contract)?;
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

type SparseRequiredRowFieldSpans<'a> =
    Result<(Cow<'a, [u8]>, (usize, usize), (usize, usize)), StructuralRowDecodeError>;

// Decode the canonical slot-container header while retaining only one required
// slot span plus the primary-key span for sparse direct slot reads.
fn decode_sparse_required_row_field_spans<'payload>(
    payload: Cow<'payload, [u8]>,
    contract: &StructuralRowContract,
    required_slot: usize,
) -> SparseRequiredRowFieldSpans<'payload> {
    let bytes = payload.as_ref();
    let (data_start, table, data_section) = decode_slot_table_sections(bytes, contract)?;
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

// Decode the shared slot-table header and validate that the physical row slot
// count matches the structural contract before any full or sparse slot scanner
// walks the table. This keeps raw-row shape authority in one place for both
// generated-only and accepted-layout row contracts.
fn decode_slot_table_sections<'bytes>(
    bytes: &'bytes [u8],
    contract: &StructuralRowContract,
) -> Result<RowSlotTableSections<'bytes>, StructuralRowDecodeError> {
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

    Ok((data_start, table, data_section))
}
