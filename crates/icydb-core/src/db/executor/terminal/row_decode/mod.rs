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
            DataRow, RawRow, StorageKey, StructuralRowContract, StructuralSlotReader,
            decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
            decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
        },
        executor::terminal::{RetainedSlotLayout, RetainedSlotRow, page::KernelRow},
    },
    error::InternalError,
    model::entity::EntityModel,
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
    field_count: usize,
    primary_key_slot: usize,
}

impl RowLayout {
    /// Build one structural row layout from model metadata.
    #[must_use]
    pub(in crate::db) const fn from_model(model: &'static EntityModel) -> Self {
        let contract = StructuralRowContract::from_model(model);

        Self {
            contract,
            field_count: contract.field_count(),
            primary_key_slot: contract.primary_key_slot(),
        }
    }

    /// Borrow the frozen field-count authority carried by this layout.
    #[must_use]
    pub(in crate::db) const fn field_count(self) -> usize {
        self.field_count
    }

    /// Borrow the frozen primary-key slot authority carried by this layout.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot(self) -> usize {
        self.primary_key_slot
    }

    /// Open one raw row through the authority-owned structural decode contract.
    pub(in crate::db) fn open_raw_row(
        self,
        row: &RawRow,
    ) -> Result<StructuralSlotReader<'_>, InternalError> {
        StructuralSlotReader::from_raw_row_with_contract(row, self.contract)
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
    required_slots.len() == layout.field_count
        && required_slots
            .iter()
            .copied()
            .enumerate()
            .all(|(expected_slot, slot)| slot == expected_slot)
}
