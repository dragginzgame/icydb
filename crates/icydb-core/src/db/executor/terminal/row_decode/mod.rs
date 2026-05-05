//! Module: executor::terminal::row_decode
//! Responsibility: structural scalar row decode from persisted bytes into kernel rows.
//! Does not own: typed response reconstruction or access-path iteration policy.
//! Boundary: scalar runtime row production consumes this structural decode contract.

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::model::field::EnumVariantModel;
#[cfg(test)]
use crate::types::Ulid;
use crate::{
    db::{
        data::{
            CanonicalSlotReader, DataRow, RawRow, ScalarSlotValueRef, ScalarValueRef, StorageKey,
            StructuralRowContract, StructuralSlotReader, decode_dense_raw_row_with_contract,
            decode_sparse_indexed_raw_row_with_contract, decode_sparse_raw_row_with_contract,
            decode_sparse_required_slot_with_contract,
            decode_sparse_required_slot_with_contract_and_fields,
        },
        executor::terminal::{
            RetainedSlotLayout, RetainedSlotRow, RetainedSlotValueMode, page::KernelRow,
        },
        schema::AcceptedRowLayoutRuntimeDescriptor,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
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

#[derive(Clone, Copy, Debug)]
pub(in crate::db) struct RowLayout {
    contract: StructuralRowContract,
}

impl RowLayout {
    /// Build one structural row layout from model metadata.
    #[must_use]
    pub(in crate::db) const fn from_model(model: &'static EntityModel) -> Self {
        Self {
            contract: StructuralRowContract::from_model(model),
        }
    }

    /// Build one row layout from accepted runtime row-layout metadata while
    /// retaining generated field decoders.
    ///
    /// The current data decoder still needs generated `FieldModel` entries to
    /// decode field payloads. For that reason this bridge accepts only layouts
    /// whose accepted field slots exactly match generated field indices. Later
    /// schema evolution work can replace this with accepted-field decoders.
    pub(in crate::db) fn from_generated_compatible_accepted_descriptor(
        model: &'static EntityModel,
        descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    ) -> Result<Self, InternalError> {
        let row_shape = descriptor.generated_compatible_row_shape_for_model(model)?;
        let contract = StructuralRowContract::from_model_with_row_shape(
            model,
            row_shape.required_slot_count(),
            row_shape.primary_key_slot_index(),
        );

        Ok(Self { contract })
    }

    /// Borrow the frozen field-count authority carried by this layout.
    #[must_use]
    pub(in crate::db) const fn field_count(self) -> usize {
        self.contract.field_count()
    }

    /// Borrow the frozen primary-key slot authority carried by this layout.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot(self) -> usize {
        self.contract.primary_key_slot()
    }

    /// Borrow the frozen structural row contract carried by this layout.
    #[must_use]
    pub(in crate::db) const fn contract(self) -> StructuralRowContract {
        self.contract
    }

    /// Open one raw row through the frozen structural decode contract without
    /// retaining model-backed slot-reader seams.
    pub(in crate::db) fn open_raw_row_with_contract(
        self,
        row: &RawRow,
    ) -> Result<StructuralSlotReader<'_>, InternalError> {
        StructuralSlotReader::from_raw_row_with_contract(row, self.contract)
    }

    /// Decode one compact sparse slot buffer directly through the frozen row
    /// contract without constructing the general structural slot reader.
    pub(in crate::db) fn decode_indexed_values(
        self,
        row: &RawRow,
        expected_key: StorageKey,
        required_slots: &[usize],
    ) -> Result<Vec<Option<Value>>, InternalError> {
        decode_sparse_indexed_raw_row_with_contract(
            row,
            self.contract,
            expected_key,
            required_slots,
        )
    }

    /// Decode one required structural slot directly through the frozen row
    /// contract without constructing a sparse slot buffer.
    pub(in crate::db) fn decode_required_value(
        self,
        row: &RawRow,
        expected_key: StorageKey,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        decode_sparse_required_slot_with_contract(row, self.contract, expected_key, required_slot)
    }

    /// Decode one full structural row into a caller-owned reusable value buffer.
    pub(in crate::db) fn decode_full_value_row_into(
        self,
        expected_key: StorageKey,
        row: &RawRow,
        values: &mut Vec<Value>,
    ) -> Result<(), InternalError> {
        values.clear();

        let decoded = decode_dense_raw_row_with_contract(row, self.contract, expected_key)?;
        values.reserve(decoded.len());

        // Phase 1: dense row decode should produce every declared slot, but
        // keep the persisted-row error taxonomy explicit if a malformed row
        // ever reaches this boundary.
        for (slot, value) in decoded.into_iter().enumerate() {
            let value = value.ok_or_else(|| {
                let field = self
                    .contract
                    .fields()
                    .get(slot)
                    .expect("dense structural decode only returns declared slots");

                InternalError::persisted_row_declared_field_missing(field.name())
            })?;
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
    StorageKey,
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
        expected_key: StorageKey,
        row: &RawRow,
        required_slots: Option<&[usize]>,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        (self.decode_slots)(layout, expected_key, row, required_slots)
    }

    /// Decode one retained structural slot value without constructing one
    /// full kernel-row envelope or returning the surrounding slot vector.
    pub(in crate::db::executor) fn decode_required_slot_value(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        decode_sparse_required_slot_with_contract(row, layout.contract, expected_key, required_slot)
    }

    // Decode one retained structural slot value through caller-frozen field
    // metadata so one-slot grouped paths do not rediscover the selected and
    // primary-key field contracts per row.
    pub(in crate::db::executor) fn decode_required_slot_value_with_fields(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        required_slot: usize,
        required_field: &FieldModel,
        primary_key_field: &FieldModel,
    ) -> Result<Option<Value>, InternalError> {
        decode_sparse_required_slot_with_contract_and_fields(
            row,
            layout.contract,
            expected_key,
            required_slot,
            required_field,
            primary_key_field,
        )
    }

    /// Decode one retained structural slot-row without materializing a dense
    /// field-count-sized slot vector.
    pub(in crate::db::executor) fn decode_retained_slots(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<RetainedSlotRow, InternalError> {
        // Reuse the canonical indexed retained-slot decode for both sparse and
        // dense layouts so the retained row always stays on the shared indexed
        // representation instead of rebuilding a separate sparse wrapper.
        Ok(RetainedSlotRow::from_indexed_values(
            retained_slot_layout,
            Self::decode_indexed_slot_values(layout, expected_key, row, retained_slot_layout)?,
        ))
    }

    /// Decode one compact retained-slot value buffer without constructing one
    /// retained-row wrapper or field-count-sized slot image.
    pub(in crate::db::executor) fn decode_indexed_slot_values(
        layout: &RowLayout,
        expected_key: StorageKey,
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
            return decode_dense_raw_row_with_contract(row, layout.contract, expected_key);
        }

        // Phase 2: reuse the canonical row-open validation boundary once, then
        // materialize only the caller-declared retained slots into compact
        // layout order.
        decode_sparse_indexed_raw_row_with_contract(
            row,
            layout.contract,
            expected_key,
            retained_slot_layout.required_slots(),
        )
    }
}

// Decode retained slots that mix normal value materialization with specialized
// scalar byte-length slots. Normal slots keep the canonical sparse decoder;
// byte-length slots use the row reader's scalar borrowed view so blob/text bytes
// do not become owned `Value::Blob` or `Value::Text` instances.
fn decode_indexed_slot_values_with_value_modes(
    layout: &RowLayout,
    expected_key: StorageKey,
    row: &RawRow,
    retained_slot_layout: &RetainedSlotLayout,
) -> Result<Vec<Option<Value>>, InternalError> {
    let required_slots = retained_slot_layout.required_slots();
    let value_modes = retained_slot_layout.value_modes();
    let mut values = vec![None; retained_slot_layout.retained_value_count()];
    let mut normal_slots = Vec::new();
    let mut normal_value_indexes = Vec::new();

    // Phase 1: decode all regular retained slots through the existing sparse
    // structural decoder so non-optimized projection paths keep identical
    // error taxonomy and value materialization.
    for (value_index, (&slot, mode)) in required_slots.iter().zip(value_modes).enumerate() {
        if *mode == RetainedSlotValueMode::Normal {
            normal_slots.push(slot);
            normal_value_indexes.push(value_index);
        }
    }
    if normal_slots.is_empty() {
        decode_sparse_indexed_raw_row_with_contract(row, layout.contract, expected_key, &[])?;
    } else {
        let decoded_normal_values = decode_sparse_indexed_raw_row_with_contract(
            row,
            layout.contract,
            expected_key,
            normal_slots.as_slice(),
        )?;
        for (value_index, value) in normal_value_indexes.into_iter().zip(decoded_normal_values) {
            values[value_index] = value;
        }
    }

    // Phase 2: fill byte-length-only retained slots from the borrowed scalar
    // payload view. Phase 1 already validated the row storage key through the
    // direct structural decoder, even when there were no regular slots.
    let row_fields = layout.open_raw_row_with_contract(row)?;
    for (value_index, (&slot, mode)) in required_slots.iter().zip(value_modes).enumerate() {
        if *mode == RetainedSlotValueMode::ScalarOctetLength {
            values[value_index] = Some(decode_scalar_octet_length_value(&row_fields, slot)?);
        }
    }

    Ok(values)
}

fn decode_scalar_octet_length_value(
    row_fields: &StructuralSlotReader<'_>,
    slot: usize,
) -> Result<Value, InternalError> {
    let value = match row_fields.required_scalar(slot)? {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => {
            Value::Uint(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => {
            Value::Uint(u64::try_from(text.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(_) => {
            return Err(InternalError::query_executor_invariant(
                "retained-slot OCTET_LENGTH optimization requires text or blob scalar slots",
            ));
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
    let slots = decode_structural_slots(layout, data_row.0.storage_key(), &data_row.1, None)?;

    Ok(KernelRow::new(data_row, slots))
}

// Decode one persisted row directly into slot-indexed structural values while
// still validating the primary-key slot against storage identity.
fn decode_structural_slots(
    layout: &RowLayout,
    expected_key: StorageKey,
    row: &RawRow,
    required_slots: Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: route dense full-slot callers straight to the dedicated dense
    // decode path so they do not pay per-row sparse reader construction.
    if required_slots
        .is_none_or(|required_slots| required_slots_match_full_layout(layout, required_slots))
    {
        return decode_dense_raw_row_with_contract(row, layout.contract, expected_key);
    }

    // Phase 2: sparse callers decode only the slots their compiled plan will
    // actually touch without building the general row-reader cache.
    decode_sparse_raw_row_with_contract(
        row,
        layout.contract,
        expected_key,
        required_slots.expect("dense full-slot callers return earlier"),
    )
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
