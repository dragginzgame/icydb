//! Module: executor::terminal::row_decode
//! Responsibility: structural scalar row decode from persisted bytes into kernel rows.
//! Does not own: typed response reconstruction or access-path iteration policy.
//! Boundary: scalar runtime row production consumes this structural decode contract.

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "sql"))]
use crate::db::data::SlotReader;
#[cfg(test)]
use crate::model::field::EnumVariantModel;
#[cfg(test)]
use crate::types::Ulid;
use crate::{
    db::{
        data::{
            CanonicalRow, CanonicalSlotReader, DataRow, DecodedDataStoreKey, RawRow,
            ScalarSlotValueRef, ScalarValueRef, StructuralRowContract, StructuralSlotReader,
            canonical_row_from_raw_row_with_structural_contract,
            decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
            decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
        },
        executor::terminal::{
            RetainedSlotLayout, RetainedSlotRow, RetainedSlotValueMode, page::KernelRow,
        },
        key_taxonomy::PrimaryKeyValue,
        schema::{AcceptedGeneratedRowCompatibilityProof, AcceptedRowDecodeContract},
    },
    error::InternalError,
    value::Value,
};

///
/// RowLayout
///
/// RowLayout is the structural scalar row-decode plan built once at the typed
/// boundary.
/// It captures stable field ordering so row production no longer needs typed
/// entity materialization.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct RowLayout {
    contract: StructuralRowContract,
}

impl RowLayout {
    /// Build one structural row layout from model metadata.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn from_generated_model_for_test(
        model: &'static crate::model::entity::EntityModel,
    ) -> Self {
        Self {
            contract: StructuralRowContract::from_generated_model_for_test(model),
        }
    }

    /// Build one row layout from a generated-compatible accepted row-decode contract.
    ///
    /// The proof object is consumed here so callers cannot attach accepted
    /// decode facts to an executor row layout without first proving the saved
    /// row layout is still readable by the current generated bridge. The
    /// resulting layout keeps accepted decode authority only.
    #[must_use]
    pub(in crate::db) fn from_generated_compatible_accepted_decode_contract(
        entity_path: &'static str,
        row_proof: AcceptedGeneratedRowCompatibilityProof,
        accepted_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        let _ = row_proof;
        let contract = StructuralRowContract::from_accepted_decode_contract(
            entity_path,
            accepted_decode_contract,
        );

        Self { contract }
    }

    /// Borrow the frozen field-count authority carried by this layout.
    #[must_use]
    pub(in crate::db) const fn field_count(&self) -> usize {
        self.contract.field_count()
    }

    /// Borrow the frozen structural row contract carried by this layout.
    #[must_use]
    pub(in crate::db) const fn contract(&self) -> &StructuralRowContract {
        &self.contract
    }

    /// Normalize one persisted raw row through this layout's structural contract.
    pub(in crate::db) fn canonical_row_from_raw_row(
        &self,
        row: &RawRow,
    ) -> Result<CanonicalRow, InternalError> {
        canonical_row_from_raw_row_with_structural_contract(row, &self.contract)
    }

    /// Open one raw row through the frozen structural decode contract without
    /// retaining model-backed slot-reader seams.
    pub(in crate::db) fn open_raw_row_with_contract<'a>(
        &'a self,
        row: &'a RawRow,
    ) -> Result<StructuralSlotReader<'a>, InternalError> {
        StructuralSlotReader::from_raw_row_with_borrowed_contract(row, &self.contract)
    }

    /// Decode one compact sparse slot buffer directly through the frozen row
    /// contract without constructing the general structural slot reader.
    pub(in crate::db) fn decode_indexed_values(
        &self,
        row: &RawRow,
        expected_key: &PrimaryKeyValue,
        required_slots: &[usize],
    ) -> Result<Vec<Option<Value>>, InternalError> {
        decode_sparse_indexed_raw_row_with_contract(
            row,
            &self.contract,
            expected_key,
            required_slots,
        )
    }

    pub(in crate::db) fn decode_indexed_values_from_data_key(
        &self,
        row: &RawRow,
        data_key: &DecodedDataStoreKey,
        required_slots: &[usize],
    ) -> Result<Vec<Option<Value>>, InternalError> {
        self.decode_indexed_values(row, &data_key.primary_key_value(), required_slots)
    }

    /// Decode one required structural slot directly through the frozen row
    /// contract without constructing a sparse slot buffer.
    pub(in crate::db) fn decode_required_value(
        &self,
        row: &RawRow,
        expected_key: &PrimaryKeyValue,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        decode_sparse_required_slot_with_contract(row, &self.contract, expected_key, required_slot)
    }

    pub(in crate::db) fn decode_required_value_from_data_key(
        &self,
        row: &RawRow,
        data_key: &DecodedDataStoreKey,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        self.decode_required_value(row, &data_key.primary_key_value(), required_slot)
    }

    /// Decode one full structural row through the scalar-or-composite row-key
    /// boundary. Composite-key callers use this path so row validation and
    /// primary-key component materialization do not reopen the scalar
    /// scalar-key accessor.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn decode_full_value_row_from_data_key_into(
        &self,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        values: &mut Vec<Value>,
    ) -> Result<(), InternalError> {
        values.clear();

        let mut slots = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            row,
            &self.contract,
        )?;
        slots.validate_primary_key(data_key)?;
        values.reserve(slots.field_count());

        for slot in 0..slots.field_count() {
            let Some(value) = slots.get_value(slot)? else {
                let field = self.contract.field_name(slot)?;
                return Err(InternalError::persisted_row_declared_field_missing(field));
            };
            values.push(value);
        }

        Ok(())
    }
}

///
/// RowDecoder
///
/// RowDecoder is the named structural decode contract for scalar row
/// production.
/// The scalar runtime owns this decoder and feeds it raw persisted rows plus a
/// precomputed `RowLayout`, keeping typed entity reconstruction out of the hot
/// execution loop.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct RowDecoder {
    decode: fn(&RowLayout, DataRow) -> Result<KernelRow, InternalError>,
    #[cfg(test)]
    decode_slots: RowDecodeSlotsFn,
}

#[cfg(test)]
type RowDecodeSlotsFn = fn(
    &RowLayout,
    &PrimaryKeyValue,
    &RawRow,
    Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError>;

impl RowDecoder {
    /// Build the canonical structural row decoder used by scalar execution.
    #[must_use]
    pub(in crate::db::executor) const fn structural() -> Self {
        Self {
            decode: decode_kernel_row_structural,
            #[cfg(test)]
            decode_slots: decode_structural_slots,
        }
    }

    /// Decode one persisted row into one structural kernel row.
    pub(in crate::db::executor) fn decode(
        self,
        layout: &RowLayout,
        data_row: DataRow,
    ) -> Result<KernelRow, InternalError> {
        (self.decode)(layout, data_row)
    }

    /// Decode one persisted row into slot-indexed structural values without
    /// constructing one full kernel-row envelope.
    #[cfg(test)]
    pub(in crate::db::executor) fn decode_slots(
        self,
        layout: &RowLayout,
        expected_key: &PrimaryKeyValue,
        row: &RawRow,
        required_slots: Option<&[usize]>,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        (self.decode_slots)(layout, expected_key, row, required_slots)
    }

    pub(in crate::db::executor) fn decode_retained_slots_from_data_key(
        layout: &RowLayout,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<RetainedSlotRow, InternalError> {
        if retained_slot_layout.has_value_mode_overrides() {
            let row_fields = layout.open_raw_row_with_contract(row)?;
            row_fields.validate_primary_key(data_key)?;

            return Ok(RetainedSlotRow::from_indexed_values(
                retained_slot_layout,
                Self::decode_indexed_slot_values_from_reader(&row_fields, retained_slot_layout)?,
            ));
        }

        Ok(RetainedSlotRow::from_indexed_values(
            retained_slot_layout,
            Self::decode_indexed_slot_values_from_data_key(
                layout,
                data_key,
                row,
                retained_slot_layout,
            )?,
        ))
    }

    /// Decode one compact retained-slot value buffer without constructing one
    /// retained-row wrapper or field-count-sized slot image.
    pub(in crate::db::executor) fn decode_indexed_slot_values(
        layout: &RowLayout,
        expected_key: &PrimaryKeyValue,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        if retained_slot_layout.has_value_mode_overrides() {
            return decode_indexed_slot_values_with_value_modes(
                layout,
                expected_key,
                row,
                retained_slot_layout,
            );
        }

        // Phase 1: let dense callers stay on the dedicated direct full-row
        // decode path so compact retained layouts do not regress all-slot reads.
        if required_slots_match_full_layout(layout, retained_slot_layout.required_slots()) {
            return decode_dense_raw_row_with_contract(row, &layout.contract, expected_key);
        }

        if let [required_slot] = retained_slot_layout.required_slots() {
            return Ok(vec![layout.decode_required_value(
                row,
                expected_key,
                *required_slot,
            )?]);
        }

        // Phase 2: reuse the canonical row-open validation boundary once, then
        // materialize only the caller-declared retained slots into compact
        // layout order.
        decode_sparse_indexed_raw_row_with_contract(
            row,
            &layout.contract,
            expected_key,
            retained_slot_layout.required_slots(),
        )
    }

    pub(in crate::db::executor) fn decode_indexed_slot_values_from_data_key(
        layout: &RowLayout,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        Self::decode_indexed_slot_values(
            layout,
            &data_key.primary_key_value(),
            row,
            retained_slot_layout,
        )
    }

    /// Decode one compact retained-slot value buffer from an already-opened
    /// structural slot reader. Filtered retained scans use this after
    /// scan-time predicate evaluation so accepted rows do not reopen the same
    /// raw row just to build retained projection/order slots.
    pub(in crate::db::executor) fn decode_indexed_slot_values_from_reader(
        row_fields: &StructuralSlotReader<'_>,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        let required_slots = retained_slot_layout.required_slots();
        let value_modes = retained_slot_layout.value_modes();
        let mut values = Vec::with_capacity(retained_slot_layout.retained_value_count());

        for (&slot, mode) in required_slots.iter().zip(value_modes) {
            let value = match mode {
                RetainedSlotValueMode::Normal => row_fields.required_value_by_contract(slot)?,
                RetainedSlotValueMode::ScalarOctetLength => {
                    decode_scalar_octet_length_value(row_fields, slot)?
                }
            };
            values.push(Some(value));
        }

        Ok(values)
    }
}

// Decode retained slots that mix normal value materialization with specialized
// scalar byte-length slots. Normal slots keep the canonical sparse decoder;
// byte-length slots use the row reader's scalar borrowed view so blob/text bytes
// do not become owned `Value::Blob` or `Value::Text` instances.
fn decode_indexed_slot_values_with_value_modes(
    layout: &RowLayout,
    expected_key: &PrimaryKeyValue,
    row: &RawRow,
    retained_slot_layout: &RetainedSlotLayout,
) -> Result<Vec<Option<Value>>, InternalError> {
    let row_fields = layout.open_raw_row_with_contract(row)?;
    row_fields.validate_primary_key_value(expected_key)?;

    RowDecoder::decode_indexed_slot_values_from_reader(&row_fields, retained_slot_layout)
}

fn decode_scalar_octet_length_value(
    row_fields: &StructuralSlotReader<'_>,
    slot: usize,
) -> Result<Value, InternalError> {
    let value = match row_fields.required_scalar(slot)? {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => {
            Value::Nat64(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => {
            Value::Nat64(u64::try_from(text.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(_) => {
            return Err(InternalError::query_executor_invariant());
        }
    };

    Ok(value)
}

// Decode one persisted data row into one structural kernel row using the
// precomputed slot layout and structural field decoders only.
fn decode_kernel_row_structural(
    layout: &RowLayout,
    data_row: DataRow,
) -> Result<KernelRow, InternalError> {
    let slots = decode_structural_slots_from_data_key(layout, &data_row.0, &data_row.1, None)?;

    Ok(KernelRow::new(data_row, slots))
}

fn decode_structural_slots_from_data_key(
    layout: &RowLayout,
    data_key: &DecodedDataStoreKey,
    row: &RawRow,
    required_slots: Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError> {
    decode_structural_slots(layout, &data_key.primary_key_value(), row, required_slots)
}

// Decode one persisted row directly into slot-indexed structural values while
// still validating the primary-key slot against storage identity.
fn decode_structural_slots(
    layout: &RowLayout,
    expected_key: &PrimaryKeyValue,
    row: &RawRow,
    required_slots: Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: route dense full-slot callers straight to the dedicated dense
    // decode path so they do not pay per-row sparse reader construction.
    if required_slots
        .is_none_or(|required_slots| required_slots_match_full_layout(layout, required_slots))
    {
        return decode_dense_raw_row_with_contract(row, &layout.contract, expected_key);
    }

    // Phase 2: sparse callers decode only the slots their compiled plan will
    // actually touch without building the general row-reader cache.
    let required_slots = required_slots.ok_or_else(InternalError::query_executor_invariant)?;
    decode_sparse_raw_row_with_contract(row, &layout.contract, expected_key, required_slots)
}

// Detect the dense retained-slot case up front so full-row and full-slot
// structural paths can stay on the straight-line dense decode before compact
// retained-row conversion instead of paying the sparse per-slot decode machinery.
fn required_slots_match_full_layout(layout: &RowLayout, required_slots: &[usize]) -> bool {
    required_slots.len() == layout.field_count()
        && required_slots
            .iter()
            .copied()
            .enumerate()
            .all(|(expected_slot, slot)| slot == expected_slot)
}
