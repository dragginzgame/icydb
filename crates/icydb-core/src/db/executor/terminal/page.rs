//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

use crate::{
    db::{
        cursor::{CursorBoundary, MaterializedCursorRow, next_cursor_for_materialized_rows},
        data::{DataKey, DataRow, RawRow},
        direction::Direction,
        executor::{
            BudgetedOrderedKeyStream, EntityAuthority, ExecutionKernel, ExecutorError,
            OrderReadableRow, OrderedKeyStream, ScalarContinuationContext,
            apply_structural_order_window, compare_orderable_row_with_boundary,
            exact_output_key_count_hint, key_stream_budget_is_redundant,
            order::cursor_boundary_from_orderable_row,
            pipeline::contracts::{
                CursorEmissionMode, MaterializedExecutionPayload, PageCursor,
                ProjectionMaterializationMode, StructuralCursorPage,
            },
            pipeline::operators::PreparedSqlExecutionProjection,
            projection::{
                PreparedProjectionShape, PreparedSlotProjectionValidation,
                project_sql_distinct_projection_slot_rows_for_dispatch,
                project_sql_projection_slot_rows_for_dispatch,
                render_sql_distinct_projection_slot_rows_for_dispatch,
                render_sql_projection_slot_rows_for_dispatch, validate_prepared_projection_row,
            },
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{AccessPlannedQuery, ResolvedOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};
use std::{borrow::Cow, sync::Arc};

use crate::db::executor::terminal::{RowDecoder, RowLayout};

///
/// RetainedSlotLayout
///
/// RetainedSlotLayout is the executor-owned shared slot lookup compiled once
/// for one slot-only execution shape.
/// Retained rows clone this layout handle so each row can stay compact while
/// still resolving slot reads in O(1) time.
///

#[derive(Clone)]
pub(in crate::db::executor) struct RetainedSlotLayout {
    data: Arc<RetainedSlotLayoutData>,
}

///
/// RetainedSlotLayoutData
///
/// Shared retained-slot metadata carried by one retained-slot layout handle.
/// It preserves the retained slot order plus the reverse slot-to-value-index
/// lookup so row decode does not rebuild either structure per row.
///

struct RetainedSlotLayoutData {
    required_slots: Box<[usize]>,
    slot_to_value_index: Box<[Option<usize>]>,
}

impl RetainedSlotLayout {
    /// Compile one retained-slot layout from one stable retained-slot list.
    #[must_use]
    pub(in crate::db::executor) fn compile(slot_count: usize, required_slots: Vec<usize>) -> Self {
        let mut slot_to_value_index = vec![None; slot_count];
        for (value_index, &slot) in required_slots.iter().enumerate() {
            if let Some(entry) = slot_to_value_index.get_mut(slot) {
                *entry = Some(value_index);
            }
        }

        Self {
            data: Arc::new(RetainedSlotLayoutData {
                required_slots: required_slots.into_boxed_slice(),
                slot_to_value_index: slot_to_value_index.into_boxed_slice(),
            }),
        }
    }

    /// Borrow the retained slots in the same stable order used by retained-row value storage.
    #[must_use]
    pub(in crate::db::executor) fn required_slots(&self) -> &[usize] {
        self.data.required_slots.as_ref()
    }

    /// Resolve one global slot index to one retained-row value index.
    #[must_use]
    pub(in crate::db::executor) fn value_index_for_slot(&self, slot: usize) -> Option<usize> {
        self.data.slot_to_value_index.get(slot).copied().flatten()
    }

    /// Return the full slot span covered by this retained-slot layout.
    #[must_use]
    pub(in crate::db::executor) fn slot_count(&self) -> usize {
        self.data.slot_to_value_index.len()
    }

    /// Return the number of retained values each indexed retained row stores.
    #[must_use]
    pub(in crate::db::executor) fn retained_value_count(&self) -> usize {
        self.data.required_slots.len()
    }
}

///
/// RetainedSlotRow
///
/// RetainedSlotRow keeps only the caller-declared decoded slot values for one
/// SQL-only structural row.
/// The slot-only execution path stores those retained values in one compact
/// slot-sorted entry list so sparse SQL projections do not allocate a
/// field-count-sized `Vec<Option<Value>>` for every row.
///

pub(in crate::db::executor) struct RetainedSlotRow {
    storage: RetainedSlotRowStorage,
}

///
/// RetainedSlotEntry
///
/// RetainedSlotEntry stores one retained slot index plus its optional value.
/// Entries stay sorted by slot so retained rows can binary-search sparse slot
/// lookups without rebuilding a dense per-row slot image.
///

struct RetainedSlotEntry {
    slot: usize,
    value: Option<Value>,
}

// Retained rows either reuse one shared indexed layout for O(1) slot access
// or keep one compact sparse fallback shape when no prepared retained-slot
// layout exists for the producer.
enum RetainedSlotRowStorage {
    Indexed {
        layout: RetainedSlotLayout,
        values: Vec<Option<Value>>,
    },
    Sparse {
        slot_count: usize,
        entries: Vec<RetainedSlotEntry>,
    },
}

impl RetainedSlotRow {
    /// Build one retained slot row from sparse decoded `(slot, value)` pairs.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn new(slot_count: usize, entries: Vec<(usize, Value)>) -> Self {
        Self::from_sparse_entries(slot_count, entries)
    }

    /// Build one retained slot row from sparse decoded `(slot, value)` pairs.
    #[must_use]
    pub(in crate::db::executor) fn from_sparse_entries(
        slot_count: usize,
        entries: Vec<(usize, Value)>,
    ) -> Self {
        let mut compact_entries = entries
            .into_iter()
            .filter(|(slot, _)| *slot < slot_count)
            .collect::<Vec<_>>();
        compact_entries.sort_by_key(|(slot, _)| *slot);

        let mut deduped_entries: Vec<RetainedSlotEntry> = Vec::with_capacity(compact_entries.len());
        for (slot, value) in compact_entries {
            if let Some(entry) = deduped_entries.last_mut()
                && entry.slot == slot
            {
                entry.value = Some(value);
            } else {
                deduped_entries.push(RetainedSlotEntry {
                    slot,
                    value: Some(value),
                });
            }
        }

        Self {
            storage: RetainedSlotRowStorage::Sparse {
                slot_count,
                entries: deduped_entries,
            },
        }
    }

    /// Build one retained slot row from one already-materialized dense slot image.
    #[must_use]
    pub(in crate::db::executor) fn from_dense_slots(slots: Vec<Option<Value>>) -> Self {
        let slot_count = slots.len();
        let mut entries = Vec::new();

        for (slot, value) in slots.into_iter().enumerate() {
            let Some(value) = value else {
                continue;
            };

            entries.push(RetainedSlotEntry {
                slot,
                value: Some(value),
            });
        }

        Self {
            storage: RetainedSlotRowStorage::Sparse {
                slot_count,
                entries,
            },
        }
    }

    /// Build one retained slot row from compact retained values under one
    /// shared retained-slot layout.
    #[must_use]
    pub(in crate::db::executor) fn from_indexed_values(
        layout: &RetainedSlotLayout,
        values: Vec<Option<Value>>,
    ) -> Self {
        debug_assert_eq!(values.len(), layout.retained_value_count());

        Self {
            storage: RetainedSlotRowStorage::Indexed {
                layout: layout.clone(),
                values,
            },
        }
    }

    /// Borrow one retained slot value without cloning it back out of the row.
    #[must_use]
    pub(in crate::db::executor) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        match &self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let index = layout.value_index_for_slot(slot)?;

                values.get(index).and_then(Option::as_ref)
            }
            RetainedSlotRowStorage::Sparse { entries, .. } => {
                Self::find_sparse_entry(entries.as_slice(), slot)
                    .and_then(|entry| entry.value.as_ref())
            }
        }
    }

    /// Remove one retained slot value by slot index while consuming the row in
    /// direct field-projection paths.
    pub(in crate::db::executor) fn take_slot(&mut self, slot: usize) -> Option<Value> {
        match &mut self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let index = layout.value_index_for_slot(slot)?;

                values.get_mut(index)?.take()
            }
            RetainedSlotRowStorage::Sparse { entries, .. } => {
                let index = Self::find_sparse_entry_index(entries.as_slice(), slot)?;

                entries.get_mut(index)?.value.take()
            }
        }
    }

    /// Expand this retained row back into one dense slot vector for callers
    /// that still require slot-indexed access across the full row width.
    #[must_use]
    pub(in crate::db::executor) fn into_dense_slots(self) -> Vec<Option<Value>> {
        match self.storage {
            RetainedSlotRowStorage::Indexed { layout, values } => {
                let mut slots = vec![None; layout.slot_count()];

                for (&slot, value) in layout.required_slots().iter().zip(values) {
                    slots[slot] = value;
                }

                slots
            }
            RetainedSlotRowStorage::Sparse {
                slot_count,
                entries,
            } => {
                let mut slots = vec![None; slot_count];

                for entry in entries {
                    if let Some(value) = entry.value {
                        slots[entry.slot] = Some(value);
                    }
                }

                slots
            }
        }
    }

    // Resolve one retained sparse entry by slot index inside the slot-sorted compact row.
    fn find_sparse_entry(entries: &[RetainedSlotEntry], slot: usize) -> Option<&RetainedSlotEntry> {
        let index = Self::find_sparse_entry_index(entries, slot)?;

        entries.get(index)
    }

    // Binary-search one compact sparse retained-slot entry list by stable slot index.
    fn find_sparse_entry_index(entries: &[RetainedSlotEntry], slot: usize) -> Option<usize> {
        entries.binary_search_by_key(&slot, |entry| entry.slot).ok()
    }
}

///
/// KernelRow
///
/// Non-generic scalar-kernel row envelope used by shared ordering/cursor/page
/// control flow before conversion back to typed `(Id<E>, E)` rows.
///

pub(in crate::db) struct KernelRow {
    data_row: Option<DataRow>,
    slots: KernelRowSlots,
}

enum KernelRowSlots {
    NotMaterialized,
    Dense(Vec<Option<Value>>),
    Retained(RetainedSlotRow),
}

impl KernelRow {
    /// Build one structural kernel row from canonical data-row storage plus
    /// slot-indexed runtime values.
    #[must_use]
    pub(in crate::db) const fn new(data_row: DataRow, slots: Vec<Option<Value>>) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::Dense(slots),
        }
    }

    /// Build one structural kernel row that keeps only the canonical data row.
    #[must_use]
    pub(in crate::db::executor) const fn new_data_row_only(data_row: DataRow) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::NotMaterialized,
        }
    }

    /// Build one structural kernel row from canonical data-row storage plus
    /// one compact retained-slot row.
    #[must_use]
    pub(in crate::db::executor) const fn new_with_retained_slots(
        data_row: DataRow,
        slots: RetainedSlotRow,
    ) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::Retained(slots),
        }
    }

    /// Build one structural kernel row that retains only decoded slot values.
    #[must_use]
    pub(in crate::db::executor) const fn new_slot_only(slots: RetainedSlotRow) -> Self {
        Self {
            data_row: None,
            slots: KernelRowSlots::Retained(slots),
        }
    }

    /// Borrow one decoded slot value without cloning it back out of the
    /// structural row cache.
    #[must_use]
    pub(in crate::db) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        match &self.slots {
            KernelRowSlots::NotMaterialized => None,
            KernelRowSlots::Dense(slots) => slots.get(slot).and_then(Option::as_ref),
            KernelRowSlots::Retained(slots) => slots.slot_ref(slot),
        }
    }

    #[cfg(test)]
    pub(in crate::db) fn slot(&self, slot: usize) -> Option<Value> {
        self.slot_ref(slot).cloned()
    }

    pub(in crate::db) fn into_data_row(self) -> Result<DataRow, InternalError> {
        self.data_row.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "slot-only kernel row reached data-row materialization path",
            )
        })
    }

    pub(in crate::db) fn into_slots(self) -> Result<Vec<Option<Value>>, InternalError> {
        match self.slots {
            KernelRowSlots::NotMaterialized => Err(InternalError::query_executor_invariant(
                "data-row-only kernel row reached slot materialization path",
            )),
            KernelRowSlots::Dense(slots) => Ok(slots),
            KernelRowSlots::Retained(slots) => Ok(slots.into_dense_slots()),
        }
    }

    pub(in crate::db::executor) fn into_retained_slot_row(
        self,
    ) -> Result<RetainedSlotRow, InternalError> {
        match self.slots {
            KernelRowSlots::NotMaterialized => Err(InternalError::query_executor_invariant(
                "data-row-only kernel row reached retained-slot materialization path",
            )),
            KernelRowSlots::Dense(slots) => Ok(RetainedSlotRow::from_dense_slots(slots)),
            KernelRowSlots::Retained(slots) => Ok(slots),
        }
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn into_parts(self) -> Result<(DataRow, Vec<Option<Value>>), InternalError> {
        let Self { data_row, slots } = self;
        let data_row = data_row.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "slot-only kernel row reached delete row materialization path",
            )
        })?;

        let slots = match slots {
            KernelRowSlots::NotMaterialized => {
                return Err(InternalError::query_executor_invariant(
                    "data-row-only kernel row reached delete row materialization path",
                ));
            }
            KernelRowSlots::Dense(slots) => slots,
            KernelRowSlots::Retained(slots) => slots.into_dense_slots(),
        };

        Ok((data_row, slots))
    }
}

impl OrderReadableRow for KernelRow {
    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_ref(slot).map(Cow::Borrowed)
    }
}

fn resolved_order_required(plan: &AccessPlannedQuery) -> Result<&ResolvedOrder, InternalError> {
    plan.resolved_order().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "ordered execution must consume one planner-frozen resolved order program",
        )
    })
}

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

    /// Borrow the canonical store handle used by this structural row runtime.
    #[must_use]
    pub(in crate::db::executor) const fn store(&self) -> StoreHandle {
        self.store
    }

    // Read one raw row through the structural store handle while preserving
    // the scalar missing-row consistency contract.
    fn read_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        let row = self.store.with_data(|store| store.get(&raw_key));

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
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };

        Ok(Some(KernelRow::new_data_row_only((key, row))))
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
    // residual predicate rejects the retained slot values.
    fn read_full_row_retained_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(kernel_row) =
            self.read_full_row_retained(consistency, key, retained_slot_layout)?
        else {
            return Ok(None);
        };
        if !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| kernel_row.slot_ref(slot))
        {
            return Ok(None);
        }

        Ok(Some(kernel_row))
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
    // residual predicate rejects the materialized slot values.
    fn read_slot_only_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(kernel_row) = self.read_slot_only(consistency, key, retained_slot_layout)? else {
            return Ok(None);
        };
        if !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| kernel_row.slot_ref(slot))
        {
            return Ok(None);
        }

        Ok(Some(kernel_row))
    }
}

///
/// KernelRowPayloadMode
///
/// KernelRowPayloadMode selects whether shared scalar row production must keep
/// a full `DataRow` payload or only decoded slot values.
/// Slot-only rows are valid for no-cursor SQL materialization lanes that never
/// reconstruct entity rows or continuation anchors.
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

    /// Read one structural data row without decoding any slot payload.
    pub(in crate::db::executor) fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_data_row_only(consistency, key)
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

    /// Read one retained full structural row and apply the residual predicate
    /// before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_full_row_retained_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_full_row_retained_with_predicate(
            consistency,
            key,
            predicate_program,
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
    /// predicate before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_slot_only_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_slot_only_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    }
}

///
/// KernelPageMaterializationRequest
///
/// Structural inputs for one shared scalar page-materialization pass.
/// This keeps the kernel loop monomorphic while boundary adapters supply only
/// store access and outer typed response reconstruction.
///

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_shape: Option<&'a PreparedProjectionShape>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) projection_materialization: ProjectionMaterializationMode,
    pub(in crate::db::executor) fuse_immediate_sql_terminal: bool,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) direction: Direction,
}

/// Materialize one ordered key stream into one execution payload.
pub(in crate::db::executor) fn materialize_key_stream_into_execution_payload<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        authority,
        plan,
        predicate_slots,
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        validate_projection,
        retain_slot_rows,
        retained_slot_layout,
        prepared_projection_shape,
        prepared_projection_validation,
        #[cfg(feature = "sql")]
            prepared_sql_projection: _prepared_sql_projection,
        projection_materialization,
        fuse_immediate_sql_terminal,
        cursor_emission,
        consistency,
        continuation,
        direction,
    } = request;
    let payload_mode =
        select_kernel_row_payload_mode(retain_slot_rows, cursor_emission, retained_slot_layout);
    let predicate_preapplied = plan.has_residual_predicate();
    let defer_sql_distinct_window = plan.scalar_plan().distinct
        && fuse_immediate_sql_terminal
        && !cursor_emission.enabled()
        && retain_slot_rows;

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) = execute_scalar_page_kernel_dyn(ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        retained_slot_layout,
        continuation,
        row_runtime,
    })?;

    // Phase 2: apply post-access phases and only retain the shared projection
    // validation pass for surfaces that are not about to materialize the same
    // projection immediately afterwards.
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.post_access_cursor_boundary(),
        predicate_slots,
        predicate_preapplied,
        defer_sql_distinct_window,
    )?;
    if validate_projection {
        validate_prepared_projection_rows(prepared_projection_validation, rows.as_slice())?;
    }

    // Phase 3: assemble the structural cursor boundary before typed page emission.
    let post_access_rows = rows.len();
    let next_cursor = build_scalar_page_cursor(
        authority,
        plan,
        rows.as_slice(),
        cursor_emission,
        rows_after_cursor,
        continuation,
        direction,
    )?;

    // Phase 4: select the final payload shape once, then build it in one
    // explicit kernel-row shaping pass.
    let finalize_mode = select_kernel_row_finalize_mode(
        prepared_projection_shape,
        projection_materialization,
        fuse_immediate_sql_terminal,
        retain_slot_rows,
        cursor_emission,
        next_cursor,
    )?;
    let payload = finalize_kernel_rows_payload(plan, rows, finalize_mode)?;

    Ok((payload, rows_scanned, post_access_rows))
}

// Select one kernel payload mode before scanning so the row loop does not
// branch on retained/data-row shape per key.
const fn select_kernel_row_payload_mode(
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    if retain_slot_rows && !cursor_emission.enabled() {
        KernelRowPayloadMode::SlotsOnly
    } else if retained_slot_layout.is_some() {
        KernelRowPayloadMode::FullRowRetained
    } else {
        KernelRowPayloadMode::DataRowOnly
    }
}

// Resolve the optional scalar page cursor once from the post-access rows.
fn build_scalar_page_cursor(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
    cursor_emission: CursorEmissionMode,
    rows_after_cursor: usize,
    continuation: &ScalarContinuationContext,
    direction: Direction,
) -> Result<Option<PageCursor>, InternalError> {
    if !cursor_emission.enabled() {
        return Ok(None);
    }

    let post_access_rows = rows.len();
    let last_cursor_row = resolve_last_cursor_row(authority, plan, rows)?;

    Ok(next_cursor_for_materialized_rows(
        &plan.access,
        plan.scalar_plan().order.as_ref(),
        plan.scalar_plan().page.as_ref(),
        post_access_rows,
        last_cursor_row,
        rows_after_cursor,
        continuation.post_access_cursor_boundary(),
        continuation.previous_index_range_anchor(),
        direction,
        continuation.continuation_signature(),
    )?
    .map(PageCursor::Scalar))
}

// Kernel-row payload finalization still has two families:
// structural page output and fused immediate SQL terminal output.
// Select that family once before the final row-shaping pass.
enum KernelRowFinalizeMode<'a> {
    StructuralDataRows {
        next_cursor: Option<PageCursor>,
    },
    #[cfg(feature = "sql")]
    StructuralSlotRows {
        next_cursor: Option<PageCursor>,
    },
    #[cfg(feature = "sql")]
    SqlProjected {
        prepared_projection_shape: &'a PreparedProjectionShape,
    },
    #[cfg(feature = "sql")]
    SqlRendered {
        prepared_projection_shape: &'a PreparedProjectionShape,
    },
}

// Select one final payload shape before converting kernel rows into their
// outward structural or immediate SQL boundary.
fn select_kernel_row_finalize_mode(
    prepared_projection_shape: Option<&PreparedProjectionShape>,
    projection_materialization: ProjectionMaterializationMode,
    fuse_immediate_sql_terminal: bool,
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
    next_cursor: Option<PageCursor>,
) -> Result<KernelRowFinalizeMode<'_>, InternalError> {
    #[cfg(feature = "sql")]
    if fuse_immediate_sql_terminal && !cursor_emission.enabled() && retain_slot_rows {
        let prepared_projection_shape =
            required_prepared_projection_shape(prepared_projection_shape)?;

        return match projection_materialization {
            ProjectionMaterializationMode::SqlImmediateMaterialization => {
                Ok(KernelRowFinalizeMode::SqlProjected {
                    prepared_projection_shape,
                })
            }
            ProjectionMaterializationMode::SqlImmediateRenderedDispatch => {
                Ok(KernelRowFinalizeMode::SqlRendered {
                    prepared_projection_shape,
                })
            }
            ProjectionMaterializationMode::SharedValidation => {
                Ok(KernelRowFinalizeMode::StructuralSlotRows { next_cursor })
            }
        };
    }

    #[cfg(feature = "sql")]
    if retain_slot_rows {
        return Ok(KernelRowFinalizeMode::StructuralSlotRows { next_cursor });
    }

    #[cfg(not(feature = "sql"))]
    let _ = retain_slot_rows;

    Ok(KernelRowFinalizeMode::StructuralDataRows { next_cursor })
}

// Finalize one already-materialized kernel row set without re-branching on
// output mode inside the per-row shaping loop.
fn finalize_kernel_rows_payload(
    plan: &AccessPlannedQuery,
    rows: Vec<KernelRow>,
    finalize_mode: KernelRowFinalizeMode<'_>,
) -> Result<MaterializedExecutionPayload, InternalError> {
    match finalize_mode {
        KernelRowFinalizeMode::StructuralDataRows { next_cursor } => {
            Ok(MaterializedExecutionPayload::StructuralPage(
                StructuralCursorPage::new(collect_kernel_data_rows(rows)?, next_cursor),
            ))
        }
        #[cfg(feature = "sql")]
        KernelRowFinalizeMode::StructuralSlotRows { next_cursor } => Ok(
            MaterializedExecutionPayload::StructuralPage(StructuralCursorPage::new_with_slot_rows(
                collect_kernel_slot_rows(rows)?,
                next_cursor,
            )),
        ),
        #[cfg(feature = "sql")]
        KernelRowFinalizeMode::SqlProjected {
            prepared_projection_shape,
        } => Ok(MaterializedExecutionPayload::SqlProjectedRows(
            finalize_immediate_projected_sql_rows(
                plan,
                prepared_projection_shape,
                collect_kernel_slot_rows(rows)?,
            )?,
        )),
        #[cfg(feature = "sql")]
        KernelRowFinalizeMode::SqlRendered {
            prepared_projection_shape,
        } => Ok(MaterializedExecutionPayload::SqlRenderedRows(
            finalize_immediate_rendered_sql_rows(
                plan,
                prepared_projection_shape,
                collect_kernel_slot_rows(rows)?,
            )?,
        )),
    }
}

#[cfg(feature = "sql")]
fn finalize_immediate_projected_sql_rows(
    plan: &AccessPlannedQuery,
    prepared_projection_shape: &PreparedProjectionShape,
    slot_rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut rows = if plan.scalar_plan().distinct {
        project_sql_distinct_projection_slot_rows_for_dispatch(
            prepared_projection_shape,
            slot_rows,
        )?
    } else {
        project_sql_projection_slot_rows_for_dispatch(prepared_projection_shape, slot_rows)?
    };

    if plan.scalar_plan().distinct {
        apply_immediate_sql_page_window(plan, &mut rows);
    }

    Ok(rows)
}

#[cfg(feature = "sql")]
fn finalize_immediate_rendered_sql_rows(
    plan: &AccessPlannedQuery,
    prepared_projection_shape: &PreparedProjectionShape,
    slot_rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<String>>, InternalError> {
    let mut rows = if plan.scalar_plan().distinct {
        render_sql_distinct_projection_slot_rows_for_dispatch(prepared_projection_shape, slot_rows)?
    } else {
        render_sql_projection_slot_rows_for_dispatch(prepared_projection_shape, slot_rows)?
    };

    if plan.scalar_plan().distinct {
        apply_immediate_sql_page_window(plan, &mut rows);
    }

    Ok(rows)
}

// Convert kernel rows into retained slot rows in one straight-line pass.
fn collect_kernel_slot_rows(rows: Vec<KernelRow>) -> Result<Vec<RetainedSlotRow>, InternalError> {
    rows.into_iter()
        .map(KernelRow::into_retained_slot_row)
        .collect()
}

// Convert kernel rows into data rows in one straight-line pass.
fn collect_kernel_data_rows(rows: Vec<KernelRow>) -> Result<Vec<DataRow>, InternalError> {
    rows.into_iter().map(KernelRow::into_data_row).collect()
}

// Run the shared slot-row projection validator from already-prepared
// projection state and fail closed if that prepared state was not supplied.
fn validate_prepared_projection_rows(
    prepared_projection_validation: Option<&PreparedSlotProjectionValidation>,
    rows: &[KernelRow],
) -> Result<(), InternalError> {
    let prepared_projection_validation = prepared_projection_validation.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "projection validation requires prepared slot-row projection state",
        )
    })?;
    for row in rows {
        validate_prepared_projection_row(prepared_projection_validation, &mut |slot| {
            row.slot_ref(slot)
        })?;
    }

    Ok(())
}

// Require prepared projection shape whenever one fused immediate SQL terminal
// is about to emit final rows directly from retained slot rows.
#[cfg(feature = "sql")]
fn required_prepared_projection_shape(
    prepared_projection_shape: Option<&PreparedProjectionShape>,
) -> Result<&PreparedProjectionShape, InternalError> {
    prepared_projection_shape.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "fused immediate SQL projection requires prepared projection shape",
        )
    })
}

// Resolve the last structural cursor row before typed response decode.
fn resolve_last_cursor_row(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
) -> Result<Option<MaterializedCursorRow>, InternalError> {
    let Some(resolved_order) = plan.resolved_order() else {
        return Ok(None);
    };
    let Some(row) = rows.last() else {
        return Ok(None);
    };

    // Phase 1: derive the structural boundary from already-materialized row slots.
    let boundary = cursor_boundary_from_orderable_row(row, resolved_order);

    // Phase 2: derive the optional raw index-range anchor once for index-range paths.
    let index_anchor = if let Some((index, _, _, _)) = plan.access.as_index_range_path() {
        let data_key = &row
            .data_row
            .as_ref()
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "slot-only kernel row reached cursor anchor derivation path",
                )
            })?
            .0;
        let mut read_slot = |slot| row.slot_ref(slot);
        authority
            .index_key_from_slot_ref_reader(data_key.storage_key(), index, &mut read_slot)?
            .map(|key| key.to_raw())
    } else {
        None
    };

    Ok(Some(MaterializedCursorRow::new(boundary, index_anchor)))
}

// Run canonical post-access phases over kernel rows.
fn apply_post_access_to_kernel_rows_dyn(
    plan: &AccessPlannedQuery,
    rows: &mut Vec<KernelRow>,
    cursor: Option<&CursorBoundary>,
    predicate_slots: Option<&PredicateProgram>,
    predicate_preapplied: bool,
    defer_sql_distinct_window: bool,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();
    let has_residual_predicate = plan.has_residual_predicate();

    // Phase 1: predicate filtering.
    let filtered = if has_residual_predicate {
        if !predicate_preapplied {
            let Some(predicate_program) = predicate_slots else {
                return Err(InternalError::scalar_page_predicate_slots_required());
            };

            compact_kernel_rows_in_place(rows, |row| {
                let mut read_slot = |slot| row.slot_ref(slot);
                predicate_program.eval_with_slot_value_ref_reader(&mut read_slot)
            });
        }

        true
    } else {
        false
    };

    // Phase 2: ordering.
    let mut ordered = false;
    let mut rows_after_order = rows.len();
    if let Some(order) = logical.order.as_ref()
        && !order.fields.is_empty()
    {
        if has_residual_predicate && !filtered {
            return Err(InternalError::scalar_page_ordering_after_filtering_required());
        }

        ordered = true;
        if !access_order_satisfied_by_route_contract(plan) {
            let resolved_order = resolved_order_required(plan)?;
            let ordered_total = rows.len();

            if rows.len() > 1 {
                apply_structural_order_window(
                    rows,
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, cursor),
                );
            }
            rows_after_order = ordered_total;
        }
    }

    // Phase 3: continuation boundary.
    let rows_after_cursor = if logical.mode.is_load() {
        if cursor.is_some() {
            if logical.order.is_none() {
                return Err(InternalError::scalar_page_cursor_boundary_order_required());
            }
            if !ordered {
                return Err(InternalError::scalar_page_cursor_boundary_after_ordering_required());
            }
        }
        if logical
            .page
            .as_ref()
            .is_some_and(|_| logical.order.is_some() && !ordered)
        {
            return Err(InternalError::scalar_page_pagination_after_ordering_required());
        }
        if defer_sql_distinct_window {
            rows_after_order
        } else {
            let resolved_order = cursor.map(|_| resolved_order_required(plan)).transpose()?;

            apply_load_cursor_and_pagination_window(
                rows,
                cursor
                    .zip(resolved_order)
                    .map(|(boundary, resolved_order)| (resolved_order, boundary)),
                ExecutionKernel::effective_page_offset(plan, cursor),
                logical.page.as_ref().and_then(|page| page.limit),
            )
        }
    } else {
        rows_after_order
    };

    // Phase 5: apply the ordered delete window.
    if logical.mode.is_delete()
        && let Some(delete_window) = logical.delete_limit.as_ref()
    {
        if logical.order.is_some() && !ordered {
            return Err(InternalError::scalar_page_delete_limit_after_ordering_required());
        }
        apply_delete_window(rows, delete_window.offset, delete_window.limit);
    }

    Ok(rows_after_cursor)
}

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
struct ScalarPageKernelRequest<'a, 'r> {
    key_stream: &'a mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    consistency: MissingRowPolicy,
    payload_mode: KernelRowPayloadMode,
    predicate_slots: Option<&'a PredicateProgram>,
    predicate_preapplied: bool,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
    continuation: &'a ScalarContinuationContext,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

///
/// KernelRowScanRequest
///
/// KernelRowScanRequest is the canonical executor-owned row scan contract for
/// structural key-stream materialization.
/// Both the generic scalar-page path and the SQL row-collector short path
/// select one payload kernel through this boundary instead of duplicating the
/// same payload-mode dispatch locally.
///

pub(in crate::db::executor) struct KernelRowScanRequest<'a, 'r> {
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) payload_mode: KernelRowPayloadMode,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) predicate_preapplied: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) row_keep_cap: Option<usize>,
    pub(in crate::db::executor) row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

pub(in crate::db::executor) fn execute_kernel_row_scan(
    request: KernelRowScanRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        retained_slot_layout,
        row_keep_cap,
        row_runtime,
    } = request;

    // Phase 1: select the concrete row-read kernel once so the inner scan
    // loop does not branch on payload shape or predicate mode per row.
    match (payload_mode, predicate_preapplied) {
        (KernelRowPayloadMode::DataRowOnly, false) => {
            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel(key_stream, consistency, row_keep_cap, row_runtime)
            })
        }
        (KernelRowPayloadMode::DataRowOnly, true) => Err(InternalError::query_executor_invariant(
            "data-row-only kernel rows require residual predicates to be absent",
        )),
        (KernelRowPayloadMode::FullRowRetained, false) => {
            let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "retained full-row kernel rows require one retained-slot layout",
                )
            })?;

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_full_retained_rows_into_kernel(
                    key_stream,
                    consistency,
                    retained_slot_layout,
                    row_keep_cap,
                    row_runtime,
                )
            })
        }
        (KernelRowPayloadMode::FullRowRetained, true) => {
            let predicate_program =
                predicate_slots.ok_or_else(InternalError::scalar_page_predicate_slots_required)?;
            let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "retained full-row kernel rows require one retained-slot layout",
                )
            })?;

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_full_retained_rows_into_kernel_with_predicate(
                    key_stream,
                    consistency,
                    predicate_program,
                    retained_slot_layout,
                    row_keep_cap,
                    row_runtime,
                )
            })
        }
        (KernelRowPayloadMode::SlotsOnly, false) => {
            let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "slot-only kernel rows require one retained-slot layout",
                )
            })?;

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_slot_rows_into_kernel(
                    key_stream,
                    consistency,
                    retained_slot_layout,
                    row_keep_cap,
                    row_runtime,
                )
            })
        }
        (KernelRowPayloadMode::SlotsOnly, true) => {
            let predicate_program =
                predicate_slots.ok_or_else(InternalError::scalar_page_predicate_slots_required)?;
            let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "slot-only kernel rows require one retained-slot layout",
                )
            })?;

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_slot_rows_into_kernel_with_predicate(
                    key_stream,
                    consistency,
                    predicate_program,
                    retained_slot_layout,
                    row_keep_cap,
                    row_runtime,
                )
            })
        }
    }
}

fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        retained_slot_layout,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    execute_kernel_row_scan(KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        retained_slot_layout,
        row_keep_cap: None,
        row_runtime,
    })
}

// Run one scalar read loop with one optional scan budget without re-checking
// the same budget logic inside each specialized row reader.
fn execute_scalar_page_read_loop(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
    ) -> Result<(Vec<KernelRow>, usize), InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        return scan_rows(&mut budgeted);
    }

    scan_rows(key_stream)
}

// Scan one ordered key stream into kernel rows through one caller-selected
// row reader while preserving the shared scanned-key accounting contract.
fn scan_kernel_rows_with(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = exact_output_key_count_hint(key_stream, None).map_or_else(
        || row_keep_cap.unwrap_or(0),
        |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
    );
    let mut rows = Vec::with_capacity(staged_capacity);

    while let Some(key) = key_stream.next_key()? {
        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = read_row(key)? else {
            continue;
        };
        rows.push(row);
        if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
            break;
        }
    }

    Ok((rows, rows_scanned))
}

fn scan_data_rows_only_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_data_row_only(consistency, key)
    })
}

// Scan keys into full structural rows while retaining only the caller-declared
// shared slot subset needed by later executor phases.
fn scan_full_retained_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_full_row_retained(consistency, key, retained_slot_layout)
    })
}

// Scan keys into retained full structural rows while applying the residual
// predicate before rows enter shared post-access processing.
fn scan_full_retained_rows_into_kernel_with_predicate(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    predicate_program: &PredicateProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_full_row_retained_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    })
}

// Scan keys into compact slot-only rows when the final lane never needs a
// full `DataRow` payload.
fn scan_slot_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_slot_only(consistency, &key, retained_slot_layout)
    })
}

// Scan keys into compact slot-only rows while applying the residual predicate
// before rows enter shared post-access processing.
fn scan_slot_rows_into_kernel_with_predicate(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    predicate_program: &PredicateProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_slot_only_with_predicate(
            consistency,
            &key,
            predicate_program,
            retained_slot_layout,
        )
    })
}

fn apply_delete_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), offset as usize);
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), limit as usize);
        rows.truncate(limit);
    }
}

// Compact kernel rows in place under one keep predicate so row filtering stays
// on one straight-line loop instead of `Vec::retain`'s generic callback path.
fn compact_kernel_rows_in_place(
    rows: &mut Vec<KernelRow>,
    mut keep_row: impl FnMut(&KernelRow) -> bool,
) -> usize {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index]) {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    kept
}

// Apply the ordered-load continuation boundary and page window in one in-place
// compaction pass so rows do not go through separate retain, drain, and
// truncate passes after materialization.
fn apply_load_cursor_and_pagination_window(
    rows: &mut Vec<KernelRow>,
    cursor: Option<(&ResolvedOrder, &CursorBoundary)>,
    offset: u32,
    limit: Option<u32>,
) -> usize {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let mut kept_after_cursor = 0usize;
    let mut kept_after_page = 0usize;
    let mut limit_remaining = limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    for read_index in 0..rows.len() {
        let passes_cursor = match cursor {
            Some((resolved_order, boundary)) => {
                compare_orderable_row_with_boundary(&rows[read_index], resolved_order, boundary)
                    .is_gt()
            }
            None => true,
        };
        if !passes_cursor {
            continue;
        }

        kept_after_cursor = kept_after_cursor.saturating_add(1);
        if kept_after_cursor <= offset {
            continue;
        }
        if limit_remaining.is_some_and(|remaining| remaining == 0) {
            continue;
        }

        if let Some(remaining) = limit_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }

        if kept_after_page != read_index {
            rows.swap(kept_after_page, read_index);
        }
        kept_after_page = kept_after_page.saturating_add(1);
    }

    rows.truncate(kept_after_page);

    kept_after_cursor
}

#[cfg(feature = "sql")]
fn apply_immediate_sql_page_window<T>(plan: &AccessPlannedQuery, rows: &mut Vec<T>) {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return;
    };

    let total = rows.len();
    let start = usize::try_from(page.offset)
        .unwrap_or(usize::MAX)
        .min(total);
    let end = match page.limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        cursor::CursorBoundarySlot,
        query::plan::{OrderDirection, ResolvedOrderField, ResolvedOrderValueSource},
    };

    fn kernel_row_u64(value: u64) -> KernelRow {
        KernelRow::new_slot_only(RetainedSlotRow::new(1, vec![(0, Value::Uint(value))]))
    }

    fn direct_field_order(slot: usize) -> ResolvedOrder {
        ResolvedOrder::new(vec![ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(slot),
            OrderDirection::Asc,
        )])
    }

    #[test]
    fn retained_slot_row_slot_ref_and_take_slot_use_indexed_lookup() {
        let mut row = RetainedSlotRow::new(
            8,
            vec![
                (1, Value::Text("alpha".to_string())),
                (5, Value::Uint(7)),
                (3, Value::Bool(true)),
            ],
        );

        assert_eq!(row.slot_ref(5), Some(&Value::Uint(7)));
        assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
        assert_eq!(row.slot_ref(1), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
        assert_eq!(row.take_slot(5), Some(Value::Uint(7)));
        assert_eq!(row.slot_ref(5), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    }

    #[test]
    fn retained_slot_row_sparse_constructor_preserves_dense_overwrite_semantics() {
        let row = RetainedSlotRow::new(
            4,
            vec![
                (3, Value::Bool(false)),
                (1, Value::Text("first".to_string())),
                (7, Value::Uint(99)),
                (1, Value::Text("last".to_string())),
            ],
        );

        assert_eq!(
            row.into_dense_slots(),
            vec![
                None,
                Some(Value::Text("last".to_string())),
                None,
                Some(Value::Bool(false)),
            ]
        );
    }

    #[test]
    fn retained_slot_row_indexed_layout_uses_shared_slot_lookup() {
        let layout = RetainedSlotLayout::compile(8, vec![1, 3, 5]);
        let mut row = RetainedSlotRow::from_indexed_values(
            &layout,
            vec![
                Some(Value::Text("alpha".to_string())),
                Some(Value::Bool(true)),
                Some(Value::Uint(7)),
            ],
        );

        assert_eq!(row.slot_ref(5), Some(&Value::Uint(7)));
        assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
        assert_eq!(row.slot_ref(1), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
        assert_eq!(row.into_dense_slots()[5], Some(Value::Uint(7)));
    }

    #[test]
    fn load_cursor_and_pagination_window_compacts_in_one_pass() {
        let resolved_order = direct_field_order(0);
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(2))],
        };
        let mut rows = vec![
            kernel_row_u64(1),
            kernel_row_u64(2),
            kernel_row_u64(3),
            kernel_row_u64(4),
            kernel_row_u64(5),
        ];

        let rows_after_cursor = apply_load_cursor_and_pagination_window(
            &mut rows,
            Some((&resolved_order, &boundary)),
            1,
            Some(2),
        );

        assert_eq!(rows_after_cursor, 3);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(4)), Some(Value::Uint(5))]
        );
    }

    #[test]
    fn load_pagination_window_without_cursor_skips_offset_then_limits() {
        let mut rows = vec![
            kernel_row_u64(10),
            kernel_row_u64(20),
            kernel_row_u64(30),
            kernel_row_u64(40),
        ];

        let rows_after_cursor =
            apply_load_cursor_and_pagination_window(&mut rows, None, 2, Some(1));

        assert_eq!(rows_after_cursor, 4);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(30))]
        );
    }

    #[test]
    fn compact_kernel_rows_in_place_preserves_kept_order() {
        let mut rows = vec![
            kernel_row_u64(1),
            kernel_row_u64(2),
            kernel_row_u64(3),
            kernel_row_u64(4),
        ];

        let kept = compact_kernel_rows_in_place(
            &mut rows,
            |row| matches!(row.slot(0), Some(Value::Uint(value)) if value % 2 == 0),
        );

        assert_eq!(kept, 2);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(2)), Some(Value::Uint(4))]
        );
    }
}
