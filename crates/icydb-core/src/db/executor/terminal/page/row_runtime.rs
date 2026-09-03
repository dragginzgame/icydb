use crate::{
    db::{
        data::{DataRow, DecodedDataStoreKey, RawRow},
        executor::{
            ExecutorError,
            budget::charge_current_execution_budget,
            projection::eval_effective_runtime_filter_program_with_slot_reader,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::plan::EffectiveRuntimeFilterProgram,
        registry::StoreHandle,
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

use super::{KernelRow, RetainedSlotLayout, RetainedSlotRow};

///
/// ScalarRowRuntimeState
///
/// ScalarRowRuntimeState is the concrete scalar row reader shared by the
/// executor's structural load paths.
/// It keeps store access plus precomputed decode metadata together so row
/// loops can call one fixed runtime shape without rebuilding decode state.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct ScalarRowRuntimeState {
    store: StoreHandle,
    row_layout: RowLayout,
}

impl ScalarRowRuntimeState {
    /// Build one structural scalar row-runtime descriptor from resolved
    /// boundary inputs.
    #[must_use]
    pub(in crate::db::executor) const fn new(store: StoreHandle, row_layout: RowLayout) -> Self {
        Self { store, row_layout }
    }

    // Read one raw row through the structural store handle while preserving
    // the scalar missing-row consistency contract.
    fn read_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key_result = key.raw_key();
        let raw_key = raw_key_result?;

        let row = self.store.with_data(|store| store.get(raw_key));

        charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
        if let Some(row) = row.as_ref() {
            let row_bytes = u64::try_from(row.len()).unwrap_or(u64::MAX);
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::StoredBytesRead,
                row_bytes,
            )?;
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::MaterializedBytes,
                row_bytes,
            )?;
        }

        match consistency {
            MissingRowPolicy::Error => row
                .map(Some)
                .ok_or_else(|| InternalError::from(ExecutorError::missing_row(key))),
            MissingRowPolicy::Ignore => Ok(row),
        }
    }

    // Read one row for current-row-only slot evaluation. Heap and journal-live
    // payloads remain borrowed through the store guard; stable rows are owned
    // by the store read. No full payload survives this callback.
    fn read_row_borrowed<R>(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
        evaluate: impl FnOnce(&RawRow) -> Result<R, InternalError>,
    ) -> Result<Option<R>, InternalError> {
        let raw_key_result = key.raw_key();
        let raw_key = raw_key_result?;

        let result = self.store.with_data(|store| {
            let row = store.read(raw_key);

            charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
            let Some(row) = row.as_row() else {
                return Ok(None);
            };
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::StoredBytesRead,
                u64::try_from(row.len()).unwrap_or(u64::MAX),
            )?;

            evaluate(row).map(Some)
        })?;

        match consistency {
            MissingRowPolicy::Error => result
                .map(Some)
                .ok_or_else(|| InternalError::from(ExecutorError::missing_row(key))),
            MissingRowPolicy::Ignore => Ok(result),
        }
    }

    // Read one full structural row without decoding any slot values when the
    // caller can prove no later executor phase will consume them.
    fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(data_row) = self.read_data_row(consistency, key)? else {
            return Ok(None);
        };

        Ok(Some(KernelRow::new_data_row_only(data_row)))
    }

    // Read one canonical structural data row without constructing one
    // intermediate kernel-row envelope.
    fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };

        Ok(Some((key, row)))
    }

    // Read one canonical structural data row and drop it early when the
    // residual filter rejects it. Raw-row filtering reads through RowLayout
    // directly and therefore does not require retained-slot materialization.
    fn read_data_row_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        if !self.raw_row_matches_filter_program(&row, filter_program)? {
            return Ok(None);
        }

        Ok(Some((key, row)))
    }

    // Decode one full structural row while retaining only one caller-declared
    // slot subset alongside the canonical data row.
    fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        charge_decoded_row(&row, retained_slot_layout.required_slots().len())?;
        let retained_slots = RowDecoder::decode_retained_slots_from_data_key(
            &self.row_layout,
            &key,
            &row,
            retained_slot_layout,
        )?;
        let data_row = (key, row);

        Ok(Some(KernelRow::new_with_retained_slots(
            data_row,
            retained_slots,
        )))
    }

    // Decode one retained full structural row and drop it early when the
    // residual filter rejects the retained slot values.
    fn read_full_row_retained_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let Some(retained_slots) = self.retained_slots_from_filtered_row(
            &key,
            &row,
            filter_program,
            retained_slot_layout,
        )?
        else {
            return Ok(None);
        };

        Ok(Some(KernelRow::new_with_retained_slots(
            (key, row),
            retained_slots,
        )))
    }

    // Decode one compact slot-only structural row under the shared retained layout.
    fn read_slot_only(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.read_row_borrowed(consistency, key, |row| {
            charge_decoded_row(row, retained_slot_layout.required_slots().len())?;
            let slots = RowDecoder::decode_retained_slots_from_data_key(
                &self.row_layout,
                key,
                row,
                retained_slot_layout,
            )?;

            Ok(KernelRow::new_slot_only(slots))
        })
    }

    // Decode compact slots while a fused primary traversal still owns the
    // stable row payload. No full row clone survives the callback.
    fn read_borrowed_slot_only(
        &self,
        key: &DecodedDataStoreKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<KernelRow, InternalError> {
        charge_borrowed_traversal_row(row)?;
        charge_decoded_row(row, retained_slot_layout.required_slots().len())?;
        let slots = RowDecoder::decode_retained_slots_from_data_key(
            &self.row_layout,
            key,
            row,
            retained_slot_layout,
        )?;

        Ok(KernelRow::new_slot_only(slots))
    }

    // Decode one compact slot-only structural row and drop it early when the
    // residual filter rejects the materialized slot values.
    fn read_slot_only_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.read_row_borrowed(consistency, key, |row| {
            self.retained_slots_from_filtered_row(key, row, filter_program, retained_slot_layout)
                .map(|retained_slots| retained_slots.map(KernelRow::new_slot_only))
        })
        .map(Option::flatten)
    }

    // Evaluate one fused primary row without cloning its backing payload.
    fn read_borrowed_slot_only_with_filter_program(
        &self,
        key: &DecodedDataStoreKey,
        row: &RawRow,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        charge_borrowed_traversal_row(row)?;
        self.retained_slots_from_filtered_row(key, row, filter_program, retained_slot_layout)
            .map(|slots| slots.map(KernelRow::new_slot_only))
    }

    // Evaluate the residual filter and decode retained slots from one opened
    // row reader so accepted rows do not pay a second structural decode.
    fn retained_slots_from_filtered_row(
        &self,
        key: &DecodedDataStoreKey,
        row: &RawRow,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<RetainedSlotRow>, InternalError> {
        charge_decoded_row(row, retained_slot_layout.required_slots().len())?;
        let row_fields = self.row_layout.open_raw_row_with_contract(row)?;
        if !eval_effective_runtime_filter_program_with_slot_reader(filter_program, &row_fields)? {
            return Ok(None);
        }
        row_fields.validate_primary_key(key)?;

        Ok(Some(RetainedSlotRow::from_indexed_values(
            retained_slot_layout,
            RowDecoder::decode_indexed_slot_values_from_reader(&row_fields, retained_slot_layout)?,
        )))
    }

    // Evaluate one scan-time filter while the raw row is still available.
    // This is the only scan lane that can resolve expression-owned field paths
    // without first materializing root fields into retained `Value` slots.
    fn raw_row_matches_filter_program(
        &self,
        row: &RawRow,
        filter_program: &EffectiveRuntimeFilterProgram,
    ) -> Result<bool, InternalError> {
        charge_decoded_row(row, 1)?;
        let slots = self.row_layout.open_raw_row_with_contract(row)?;

        eval_effective_runtime_filter_program_with_slot_reader(filter_program, &slots)
    }
}

fn charge_decoded_row(row: &RawRow, nested_steps: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::DecodedBytes,
        u64::try_from(row.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        u64::try_from(nested_steps).unwrap_or(u64::MAX),
    )
}

fn charge_borrowed_traversal_row(row: &RawRow) -> Result<(), InternalError> {
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::StoredBytesRead,
        u64::try_from(row.len()).unwrap_or(u64::MAX),
    )
}

///
/// KernelRowPayloadMode
///
/// KernelRowPayloadMode selects whether shared scalar row production must keep
/// a full `DataRow` payload or only decoded slot values.
/// Slot-only rows are valid for no-cursor retained-slot materialization lanes
/// that never reconstruct entity rows or continuation anchors.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum KernelRowPayloadMode {
    DataRowOnly,
    FullRowRetained,
    SlotsOnly,
}

///
/// ScalarRowRuntimeHandle
///
/// ScalarRowRuntimeHandle is the borrowed structural row reader passed through
/// the shared scalar page kernels.
/// It keeps the hot loop on one concrete runtime shape while the typed
/// boundary still owns store and decode authority.
///

pub(in crate::db::executor) struct ScalarRowRuntimeHandle<'a> {
    state: &'a ScalarRowRuntimeState,
}

impl<'a> ScalarRowRuntimeHandle<'a> {
    /// Borrow one pre-resolved row-runtime state object behind a structural
    /// runtime handle without rebuilding owned runtime state for the same
    /// query execution.
    #[must_use]
    pub(in crate::db::executor) const fn from_borrowed(state: &'a ScalarRowRuntimeState) -> Self {
        Self { state }
    }

    /// Borrow the authority-owned row layout used by raw-row materialization
    /// and direct raw-row order caching.
    #[must_use]
    pub(in crate::db::executor) fn row_layout(&self) -> RowLayout {
        self.state.row_layout.clone()
    }

    /// Read one structural data row without decoding any slot payload.
    pub(in crate::db::executor) fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_data_row_only(consistency, key)
    }

    /// Read one canonical structural data row without constructing one
    /// intermediate kernel-row envelope.
    pub(in crate::db::executor) fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row(consistency, key)
    }

    /// Read one canonical structural data row and apply the residual
    /// filter program before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_data_row_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state
            .read_data_row_with_filter_program(consistency, key, filter_program)
    }

    /// Read one full structural row while retaining only one shared compact
    /// slot subset alongside the canonical data row.
    pub(in crate::db::executor) fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state
            .read_full_row_retained(consistency, key, retained_slot_layout)
    }

    /// Read one retained full structural row and apply the residual filter
    /// program before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_full_row_retained_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_full_row_retained_with_filter_program(
            consistency,
            key,
            filter_program,
            retained_slot_layout,
        )
    }

    /// Read one compact slot-only structural row from one data key.
    pub(in crate::db::executor) fn read_slot_only(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state
            .read_slot_only(consistency, key, retained_slot_layout)
    }

    /// Decode one compact slot row borrowed from an open primary traversal.
    pub(in crate::db::executor) fn read_borrowed_slot_only(
        &self,
        key: &DecodedDataStoreKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<KernelRow, InternalError> {
        self.state
            .read_borrowed_slot_only(key, row, retained_slot_layout)
    }

    /// Read one compact slot-only structural row and apply the residual
    /// filter program before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_slot_only_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: &DecodedDataStoreKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_slot_only_with_filter_program(
            consistency,
            key,
            filter_program,
            retained_slot_layout,
        )
    }

    /// Decode and filter one compact row borrowed from an open primary traversal.
    pub(in crate::db::executor) fn read_borrowed_slot_only_with_filter_program(
        &self,
        key: &DecodedDataStoreKey,
        row: &RawRow,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_borrowed_slot_only_with_filter_program(
            key,
            row,
            filter_program,
            retained_slot_layout,
        )
    }
}
