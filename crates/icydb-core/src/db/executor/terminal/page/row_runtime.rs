use crate::{
    db::{
        data::{DataKey, DataRow, RawRow},
        executor::{
            ExecutorError,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::plan::EffectiveRuntimeFilterProgram,
        registry::StoreHandle,
    },
    error::InternalError,
};

use super::{KernelRow, RetainedSlotLayout, RetainedSlotRow, scan::filter_matches_retained_values};

#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_direct_data_row_phase, record_direct_data_row_key_encode_local_instructions,
    record_direct_data_row_store_get_local_instructions,
};

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
        key: &DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        #[cfg(feature = "diagnostics")]
        let (key_encode_local_instructions, raw_key_result) =
            measure_direct_data_row_phase(|| key.to_raw());
        #[cfg(not(feature = "diagnostics"))]
        let raw_key_result = key.to_raw();
        let raw_key = raw_key_result?;
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_key_encode_local_instructions(key_encode_local_instructions);

        #[cfg(feature = "diagnostics")]
        let (store_get_local_instructions, row) = measure_direct_data_row_phase(|| {
            Ok::<_, InternalError>(self.store.with_data(|store| store.get(&raw_key)))
        });
        #[cfg(not(feature = "diagnostics"))]
        let row = self.store.with_data(|store| store.get(&raw_key));
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_store_get_local_instructions(store_get_local_instructions);
        #[cfg(feature = "diagnostics")]
        let row = row?;

        match consistency {
            MissingRowPolicy::Error => row
                .map(Some)
                .ok_or_else(|| InternalError::from(ExecutorError::missing_row(key))),
            MissingRowPolicy::Ignore => Ok(row),
        }
    }

    // Read one full structural row without decoding any slot values when the
    // caller can prove no later executor phase will consume them.
    fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
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
        key: DataKey,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };

        Ok(Some((key, row)))
    }

    // Read one canonical structural data row and drop it early when the
    // residual filter rejects the retained slot values needed by scan-time
    // filtering.
    fn read_data_row_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !filter_matches_retained_values(
            filter_program,
            retained_slot_layout,
            retained_values.as_slice(),
        )? {
            return Ok(None);
        }

        Ok(Some((key, row)))
    }

    // Decode one full structural row while retaining only one caller-declared
    // slot subset alongside the canonical data row.
    fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_slots = RowDecoder::decode_retained_slots(
            &self.row_layout,
            key.storage_key(),
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
        key: DataKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !filter_matches_retained_values(
            filter_program,
            retained_slot_layout,
            retained_values.as_slice(),
        )? {
            return Ok(None);
        }

        Ok(Some(KernelRow::new_with_retained_slots(
            (key, row),
            RetainedSlotRow::from_indexed_values(retained_slot_layout, retained_values),
        )))
    }

    // Decode one compact slot-only structural row under the shared retained layout.
    fn read_slot_only(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, key)? else {
            return Ok(None);
        };
        let slots = RowDecoder::decode_retained_slots(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;

        Ok(Some(KernelRow::new_slot_only(slots)))
    }

    // Decode one compact slot-only structural row and drop it early when the
    // residual filter rejects the materialized slot values.
    fn read_slot_only_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !filter_matches_retained_values(
            filter_program,
            retained_slot_layout,
            retained_values.as_slice(),
        )? {
            return Ok(None);
        }

        Ok(Some(KernelRow::new_slot_only(
            RetainedSlotRow::from_indexed_values(retained_slot_layout, retained_values),
        )))
    }
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
    pub(in crate::db::executor) const fn row_layout(&self) -> RowLayout {
        self.state.row_layout
    }

    /// Read one structural data row without decoding any slot payload.
    pub(in crate::db::executor) fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_data_row_only(consistency, key)
    }

    /// Read one canonical structural data row without constructing one
    /// intermediate kernel-row envelope.
    pub(in crate::db::executor) fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row(consistency, key)
    }

    /// Read one canonical structural data row and apply the residual
    /// filter program before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_data_row_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        filter_program: &EffectiveRuntimeFilterProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row_with_filter_program(
            consistency,
            key,
            filter_program,
            retained_slot_layout,
        )
    }

    /// Read one full structural row while retaining only one shared compact
    /// slot subset alongside the canonical data row.
    pub(in crate::db::executor) fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
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
        key: DataKey,
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
        key: &DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state
            .read_slot_only(consistency, key, retained_slot_layout)
    }

    /// Read one compact slot-only structural row and apply the residual
    /// filter program before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_slot_only_with_filter_program(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
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
}

///
/// ResidualPredicateScanMode
///
/// ResidualPredicateScanMode keeps the scan-owned residual filter contract
/// explicit instead of overloading a boolean with both logical presence and
/// execution timing. The scalar kernel only needs to know whether no residual
/// filter exists, whether scan must evaluate it while slot reads are
/// available, or whether post-access must evaluate it later.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ResidualPredicateScanMode {
    Absent,
    AppliedDuringScan,
    DeferredPostAccess,
}

impl ResidualPredicateScanMode {
    /// Select the executor scan contract from the logical residual-filter
    /// presence plus the row payload capabilities already chosen for this lane.
    #[must_use]
    pub(in crate::db::executor) const fn from_plan_and_layout(
        has_residual_filter: bool,
        retained_slot_layout: Option<&RetainedSlotLayout>,
        _residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    ) -> Self {
        if !has_residual_filter {
            Self::Absent
        } else if retained_slot_layout.is_some() {
            Self::AppliedDuringScan
        } else {
            Self::DeferredPostAccess
        }
    }
}
