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
            apply_structural_order_window, apply_structural_order_window_to_data_rows,
            compare_orderable_row_with_boundary, exact_output_key_count_hint,
            key_stream_budget_is_redundant,
            order::cursor_boundary_from_orderable_row,
            pipeline::contracts::{
                CursorEmissionMode, MaterializedExecutionPayload, PageCursor, StructuralCursorPage,
            },
            projection::{
                PreparedSlotProjectionValidation, ProjectionValidationRow,
                validate_prepared_projection_row,
            },
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
            window::compute_page_keep_count,
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{AccessPlannedQuery, ResolvedOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};
#[cfg(feature = "perf-attribution")]
use std::cell::Cell;
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::RefCell;
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

#[derive(Clone, Debug)]
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

#[derive(Debug)]
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
/// retained-slot structural row.
/// The slot-only execution path stores those retained values in one compact
/// slot-sorted entry list so sparse outer projections do not allocate a
/// field-count-sized `Vec<Option<Value>>` for every row.
///

pub(in crate::db) struct RetainedSlotRow {
    storage: RetainedSlotRowStorage,
}

///
/// ScalarMaterializationLaneMetrics
///
/// ScalarMaterializationLaneMetrics aggregates one test-scoped or
/// metrics-scoped view of which shared scalar materialization lane actually
/// executed for one workload.
/// This keeps lane attribution explicit so runtime work can be tied back to
/// the executor contract instead of inferred indirectly from instruction
/// totals alone.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
#[expect(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScalarMaterializationLaneMetrics {
    pub direct_data_row_path_hits: u64,
    pub direct_filtered_data_row_path_hits: u64,
    pub kernel_data_row_path_hits: u64,
    pub kernel_full_row_retained_path_hits: u64,
    pub kernel_slots_only_path_hits: u64,
}

///
/// DirectDataRowPhaseAttribution
///
/// DirectDataRowPhaseAttribution isolates the direct raw-row scalar lane into
/// scan-local subphases plus the later order/page windows that still matter
/// for warmed fluent perf work.
/// Non-direct executor lanes leave these counters at zero so the attribution
/// surface stays lane-local instead of pretending to describe every runtime.
///

#[cfg(feature = "perf-attribution")]
#[expect(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct DirectDataRowPhaseAttribution {
    pub(in crate::db::executor) scan_local_instructions: u64,
    pub(in crate::db::executor) key_stream_local_instructions: u64,
    pub(in crate::db::executor) row_read_local_instructions: u64,
    pub(in crate::db::executor) key_encode_local_instructions: u64,
    pub(in crate::db::executor) store_get_local_instructions: u64,
    pub(in crate::db::executor) order_window_local_instructions: u64,
    pub(in crate::db::executor) page_window_local_instructions: u64,
}

#[cfg(any(test, feature = "structural-read-metrics"))]
std::thread_local! {
    static SCALAR_MATERIALIZATION_LANE_METRICS: RefCell<Option<ScalarMaterializationLaneMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(feature = "perf-attribution")]
std::thread_local! {
    static DIRECT_DATA_ROW_PHASE_ATTRIBUTION: Cell<DirectDataRowPhaseAttribution> = const {
        Cell::new(DirectDataRowPhaseAttribution {
            scan_local_instructions: 0,
            key_stream_local_instructions: 0,
            row_read_local_instructions: 0,
            key_encode_local_instructions: 0,
            store_get_local_instructions: 0,
            order_window_local_instructions: 0,
            page_window_local_instructions: 0,
        })
    };
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn update_scalar_materialization_lane_metrics(
    update: impl FnOnce(&mut ScalarMaterializationLaneMetrics),
) {
    SCALAR_MATERIALIZATION_LANE_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_direct_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.direct_data_row_path_hits = metrics.direct_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_direct_filtered_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.direct_filtered_data_row_path_hits =
            metrics.direct_filtered_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_kernel_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_data_row_path_hits = metrics.kernel_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_kernel_full_row_retained_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_full_row_retained_path_hits =
            metrics.kernel_full_row_retained_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_kernel_slots_only_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_slots_only_path_hits = metrics.kernel_slots_only_path_hits.saturating_add(1);
    });
}

///
/// with_scalar_materialization_lane_metrics
///
/// Run one closure while collecting executor-owned scalar materialization lane
/// metrics on the current thread, then return the closure result plus the
/// aggregated snapshot.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
pub fn with_scalar_materialization_lane_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, ScalarMaterializationLaneMetrics) {
    SCALAR_MATERIALIZATION_LANE_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "scalar materialization lane metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(ScalarMaterializationLaneMetrics::default());
    });

    let result = f();
    let metrics = SCALAR_MATERIALIZATION_LANE_METRICS
        .with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}

#[cfg(feature = "perf-attribution")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_direct_data_row_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "perf-attribution")]
fn measure_direct_data_row_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_direct_data_row_local_instruction_counter();
    let result = run();
    let delta = read_direct_data_row_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_scan_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            scan_local_instructions: current.scan_local_instructions.saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_key_stream_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            key_stream_local_instructions: current
                .key_stream_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_row_read_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            row_read_local_instructions: current.row_read_local_instructions.saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_key_encode_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            key_encode_local_instructions: current
                .key_encode_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_store_get_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            store_get_local_instructions: current
                .store_get_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_order_window_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            order_window_local_instructions: current
                .order_window_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
fn record_direct_data_row_page_window_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            page_window_local_instructions: current
                .page_window_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "perf-attribution")]
pub(in crate::db::executor) fn with_direct_data_row_phase_attribution<T>(
    f: impl FnOnce() -> T,
) -> (T, DirectDataRowPhaseAttribution) {
    let previous = DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let previous = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution::default());

        previous
    });

    let result = f();
    let captured = DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let captured = attribution.get();
        attribution.set(previous);

        captured
    });

    (result, captured)
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
    pub(in crate::db) fn new(slot_count: usize, entries: Vec<(usize, Value)>) -> Self {
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
    pub(in crate::db) fn slot_ref(&self, slot: usize) -> Option<&Value> {
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
    pub(in crate::db) fn take_slot(&mut self, slot: usize) -> Option<Value> {
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

impl ProjectionValidationRow for RetainedSlotRow {
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
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

impl ProjectionValidationRow for KernelRow {
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
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

    // Read one raw row through the structural store handle while preserving
    // the scalar missing-row consistency contract.
    fn read_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        #[cfg(feature = "perf-attribution")]
        let (key_encode_local_instructions, raw_key_result) =
            measure_direct_data_row_phase(|| key.to_raw());
        #[cfg(not(feature = "perf-attribution"))]
        let raw_key_result = key.to_raw();
        let raw_key = raw_key_result?;
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_key_encode_local_instructions(key_encode_local_instructions);

        #[cfg(feature = "perf-attribution")]
        let (store_get_local_instructions, row) = measure_direct_data_row_phase(|| {
            Ok::<_, InternalError>(self.store.with_data(|store| store.get(&raw_key)))
        });
        #[cfg(not(feature = "perf-attribution"))]
        let row = self.store.with_data(|store| store.get(&raw_key));
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_store_get_local_instructions(store_get_local_instructions);
        #[cfg(feature = "perf-attribution")]
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
    // residual predicate rejects the retained slot values needed by scan-time
    // filtering.
    fn read_data_row_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
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
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
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
    // residual predicate rejects the retained slot values.
    fn read_full_row_retained_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
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
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
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
    // residual predicate rejects the materialized slot values.
    fn read_slot_only_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_program: &PredicateProgram,
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
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
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
    /// predicate before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_data_row_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row_with_predicate(
            consistency,
            key,
            predicate_program,
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

///
/// ScalarMaterializationCapabilities
///
/// ScalarMaterializationCapabilities carries the raw scalar-page execution
/// capabilities recovered before the terminal runtime runs.
/// It is intentionally capability-only data: the terminal resolver decides
/// policy from this bundle once instead of reinterpreting layout and cursor
/// fields across multiple sibling branches.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct ScalarMaterializationCapabilities<'a> {
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
}

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) capabilities: ScalarMaterializationCapabilities<'a>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) direction: Direction,
}

///
/// ResidualPredicateScanMode
///
/// ResidualPredicateScanMode keeps the scan-owned residual predicate contract
/// explicit instead of overloading a boolean with both logical presence and
/// execution timing. The scalar kernel only needs to know whether no residual
/// predicate exists, whether scan must evaluate it while slot reads are
/// available, or whether post-access must evaluate it later.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ResidualPredicateScanMode {
    Absent,
    AppliedDuringScan,
    DeferredPostAccess,
}

impl ResidualPredicateScanMode {
    /// Select the executor scan contract from the logical residual-predicate
    /// presence plus the row payload capabilities already chosen for this lane.
    #[must_use]
    pub(in crate::db::executor) const fn from_plan_and_layout(
        has_residual_predicate: bool,
        retained_slot_layout: Option<&RetainedSlotLayout>,
    ) -> Self {
        if !has_residual_predicate {
            Self::Absent
        } else if retained_slot_layout.is_some() {
            Self::AppliedDuringScan
        } else {
            Self::DeferredPostAccess
        }
    }
}

/// Materialize one ordered key stream into one execution payload.
#[expect(clippy::too_many_lines)]
pub(in crate::db::executor) fn materialize_key_stream_into_execution_payload<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        authority,
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        capabilities,
        consistency,
        continuation,
        direction,
    } = request;
    let scalar_materialization_plan = resolve_scalar_materialization_plan(plan, capabilities)?;
    if let Some(direct_data_row_path) = scalar_materialization_plan.direct_data_row_path {
        return execute_direct_data_row_path(
            plan,
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            continuation,
            row_runtime,
            direct_data_row_path,
        );
    }

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) =
        execute_scalar_page_kernel_dyn(scalar_materialization_plan.kernel_request(
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            continuation,
            row_runtime,
        ))?;

    // Phase 2: apply post-access phases and only retain the shared projection
    // validation pass for surfaces that are not about to materialize the same
    // projection immediately afterwards.
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.post_access_cursor_boundary(),
        scalar_materialization_plan.post_access_strategy,
    )?;
    if let Some(prepared_projection_validation) =
        scalar_materialization_plan.projection_validation()
    {
        validate_prepared_projection_rows(Some(prepared_projection_validation), rows.as_slice())?;
    }

    // Phase 3: assemble the structural cursor boundary before typed page emission.
    let post_access_rows = rows.len();
    let next_cursor = build_scalar_page_cursor(
        authority,
        plan,
        rows.as_slice(),
        scalar_materialization_plan.cursor_emission(),
        rows_after_cursor,
        continuation,
        direction,
    )?;

    // Phase 4: select the final payload shape once, then build it in one
    // explicit kernel-row shaping pass.
    let finalize_mode = scalar_materialization_plan.finalize_mode(next_cursor);
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

///
/// ScalarMaterializationPlan
///
/// ScalarMaterializationPlan freezes the scalar page policy resolved from one
/// raw capability bundle.
/// The terminal runtime executes this plan directly so payload mode,
/// residual timing, and direct-lane eligibility are decided once up front.
///

struct ScalarMaterializationPlan<'a> {
    direct_data_row_path: Option<DirectDataRowPath<'a>>,
    kernel_row_scan_strategy: KernelRowScanStrategy<'a>,
    post_access_strategy: PostAccessStrategy<'a>,
    projection_validation: Option<&'a PreparedSlotProjectionValidation>,
    cursor_emission: CursorEmissionMode,
    final_payload_strategy: FinalPayloadStrategy,
}

impl<'a> ScalarMaterializationPlan<'a> {
    // Build the shared scalar page-kernel request from one already-resolved
    // materialization plan so the terminal runtime does not re-read raw
    // capabilities after policy resolution.
    fn kernel_request<'r>(
        &self,
        key_stream: &'a mut dyn OrderedKeyStream,
        scan_budget_hint: Option<usize>,
        load_order_route_contract: LoadOrderRouteContract,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    ) -> ScalarPageKernelRequest<'a, 'r> {
        ScalarPageKernelRequest {
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            scan_strategy: self.kernel_row_scan_strategy,
            continuation,
            row_runtime,
        }
    }

    // Return the shared prepared projection validation contract when this
    // scalar materialization plan still owns that validation pass.
    const fn projection_validation(&self) -> Option<&'a PreparedSlotProjectionValidation> {
        self.projection_validation
    }

    // Return the outward cursor-emission mode already frozen into this plan.
    const fn cursor_emission(&self) -> CursorEmissionMode {
        self.cursor_emission
    }

    // Resolve the final payload family from the already-frozen plan instead
    // of re-reading retained-slot capability flags in the terminal runtime.
    const fn finalize_mode(&self, next_cursor: Option<PageCursor>) -> KernelRowFinalizeMode {
        self.final_payload_strategy.finalize_mode(next_cursor)
    }
}

// Resolve the scalar page execution plan once from one raw capability bundle
// so later phases consume one stable policy object instead of branching on
// retained-layout presence repeatedly.
fn resolve_scalar_materialization_plan<'a>(
    plan: &'a AccessPlannedQuery,
    capabilities: ScalarMaterializationCapabilities<'a>,
) -> Result<ScalarMaterializationPlan<'a>, InternalError> {
    let kernel_payload_mode = select_kernel_row_payload_mode(
        capabilities.retain_slot_rows,
        capabilities.cursor_emission,
        capabilities.retained_slot_layout,
    );
    let residual_predicate_scan_mode = ResidualPredicateScanMode::from_plan_and_layout(
        plan.has_residual_predicate(),
        capabilities.retained_slot_layout,
    );
    let kernel_row_scan_strategy = resolve_kernel_row_scan_strategy(
        kernel_payload_mode,
        capabilities.predicate_slots,
        residual_predicate_scan_mode,
        capabilities.retained_slot_layout,
    )?;
    let direct_data_row_path = resolve_direct_data_row_path(
        plan,
        capabilities.validate_projection,
        capabilities.retain_slot_rows,
        capabilities.retained_slot_layout,
        capabilities.predicate_slots,
        capabilities.cursor_emission,
        residual_predicate_scan_mode,
    )?;
    let post_access_strategy = resolve_post_access_strategy(
        plan,
        capabilities.predicate_slots,
        residual_predicate_scan_mode,
        capabilities.cursor_emission,
        capabilities.retain_slot_rows,
    )?;

    Ok(ScalarMaterializationPlan {
        direct_data_row_path,
        kernel_row_scan_strategy,
        post_access_strategy,
        projection_validation: if capabilities.validate_projection {
            capabilities.prepared_projection_validation
        } else {
            None
        },
        cursor_emission: capabilities.cursor_emission,
        final_payload_strategy: FinalPayloadStrategy::from_retain_slot_rows(
            capabilities.retain_slot_rows,
        ),
    })
}

///
/// DirectDataRowPath
///
/// DirectDataRowPath captures one executor-owned raw `DataRow` fast path.
/// It lets scalar materialization choose one direct-lane strategy once, then
/// run one shared execution shell instead of scattering residual-timing and
/// retained-layout checks across sibling branches.
///

#[derive(Clone, Copy)]
enum DirectDataRowPath<'a> {
    Plain {
        row_keep_cap: Option<usize>,
    },
    Filtered {
        row_keep_cap: Option<usize>,
        predicate_program: &'a PredicateProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    MaterializedOrder {
        residual_predicate_scan_mode: ResidualPredicateScanMode,
        resolved_order: &'a ResolvedOrder,
        predicate_program: Option<&'a PredicateProgram>,
        retained_slot_layout: Option<&'a RetainedSlotLayout>,
    },
}

///
/// KernelRowScanStrategy
///
/// KernelRowScanStrategy is the resolved structural scan strategy for the
/// non-direct scalar page lane.
/// It removes the raw `(payload_mode, residual_predicate_scan_mode)` pairing
/// from the hot execution loop by freezing one concrete retained/data-row scan
/// contract up front.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum KernelRowScanStrategy<'a> {
    DataRows,
    RetainedFullRows {
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    RetainedFullRowsFiltered {
        predicate_program: &'a PredicateProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    SlotOnlyRows {
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    SlotOnlyRowsFiltered {
        predicate_program: &'a PredicateProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
}

///
/// PostAccessPredicateStrategy
///
/// PostAccessPredicateStrategy captures whether residual predicate filtering
/// is absent, already handled during scan, or still deferred to the
/// post-access kernel-row phase.
///

#[derive(Clone, Copy)]
enum PostAccessPredicateStrategy<'a> {
    NotPresent,
    AppliedDuringScan,
    Deferred {
        predicate_program: &'a PredicateProgram,
    },
}

impl PostAccessPredicateStrategy<'_> {
    // Return whether post-access still owns residual predicate filtering.
    const fn requires_post_access_filtering(self) -> bool {
        matches!(self, Self::Deferred { .. })
    }
}

///
/// PostAccessStrategy
///
/// PostAccessStrategy freezes the remaining post-scan policy for scalar
/// kernel rows.
/// It owns residual predicate handling plus distinct-window deferral so the
/// post-access executor does not interpret raw mode flags directly.
///

#[derive(Clone, Copy)]
struct PostAccessStrategy<'a> {
    predicate_strategy: PostAccessPredicateStrategy<'a>,
    defer_retained_slot_distinct_window: bool,
}

///
/// FinalPayloadStrategy
///
/// FinalPayloadStrategy freezes the outward scalar payload family selected for
/// one materialization plan.
/// The terminal tail then only adds the already-derived next cursor instead of
/// reinterpreting retain-slot policy at the last step.
///

#[derive(Clone, Copy)]
struct FinalPayloadStrategy {
    retain_slot_rows: bool,
}

impl FinalPayloadStrategy {
    // Resolve the scalar final payload family from the outer slot-retention
    // policy once at plan construction time.
    const fn from_retain_slot_rows(retain_slot_rows: bool) -> Self {
        Self { retain_slot_rows }
    }

    // Attach the already-built cursor boundary to the frozen final payload
    // family for this scalar materialization plan.
    const fn finalize_mode(self, next_cursor: Option<PageCursor>) -> KernelRowFinalizeMode {
        select_kernel_row_finalize_mode(self.retain_slot_rows, next_cursor)
    }
}

// Resolve whether the scalar materializer can stay entirely on the direct
// `DataRow` lane and, if so, which direct-lane strategy owns the scan.
fn resolve_direct_data_row_path<'a>(
    plan: &'a AccessPlannedQuery,
    validate_projection: bool,
    retain_slot_rows: bool,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
    predicate_slots: Option<&'a PredicateProgram>,
    cursor_emission: CursorEmissionMode,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
) -> Result<Option<DirectDataRowPath<'a>>, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: direct raw-row lanes are only valid for cursorless load paths
    // that do not need projection validation or retained-slot surfaces.
    let direct_load_surface_eligible = logical.mode.is_load()
        && !logical.distinct
        && !validate_projection
        && !retain_slot_rows
        && !cursor_emission.enabled();
    if !direct_load_surface_eligible {
        return Ok(None);
    }

    // Phase 2: route-ordered paths can stay direct only when scan-time
    // residual timing and retained-layout availability already match.
    if access_order_satisfied_by_route_contract(plan) {
        return Ok(match residual_predicate_scan_mode {
            ResidualPredicateScanMode::Absent if retained_slot_layout.is_none() => {
                Some(DirectDataRowPath::Plain {
                    row_keep_cap: direct_data_row_keep_cap(plan),
                })
            }
            ResidualPredicateScanMode::AppliedDuringScan => {
                predicate_slots.zip(retained_slot_layout).map(
                    |(predicate_program, retained_slot_layout)| DirectDataRowPath::Filtered {
                        row_keep_cap: direct_data_row_keep_cap(plan),
                        predicate_program,
                        retained_slot_layout,
                    },
                )
            }
            ResidualPredicateScanMode::DeferredPostAccess => None,
            _ => None,
        });
    }

    // Phase 3: non-route-ordered direct lanes are only valid when an
    // in-memory order window can run on raw data rows after scan-time
    // residual filtering has already been settled.
    let materialized_order_direct_eligible = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty())
        && retained_slot_layout.is_some()
        && (matches!(
            residual_predicate_scan_mode,
            ResidualPredicateScanMode::Absent
        ) || (matches!(
            residual_predicate_scan_mode,
            ResidualPredicateScanMode::AppliedDuringScan
        ) && predicate_slots.is_some()));
    if !materialized_order_direct_eligible {
        return Ok(None);
    }

    Ok(Some(DirectDataRowPath::MaterializedOrder {
        residual_predicate_scan_mode,
        resolved_order: resolved_order_required(plan)?,
        predicate_program: predicate_slots,
        retained_slot_layout,
    }))
}

// Resolve one concrete kernel-row scan strategy from the payload mode and
// residual timing already selected for the scalar materialization plan.
pub(in crate::db::executor) fn resolve_kernel_row_scan_strategy<'a>(
    payload_mode: KernelRowPayloadMode,
    predicate_slots: Option<&'a PredicateProgram>,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
) -> Result<KernelRowScanStrategy<'a>, InternalError> {
    match (payload_mode, residual_predicate_scan_mode) {
        (
            KernelRowPayloadMode::DataRowOnly,
            ResidualPredicateScanMode::Absent | ResidualPredicateScanMode::DeferredPostAccess,
        ) => Ok(KernelRowScanStrategy::DataRows),
        (KernelRowPayloadMode::DataRowOnly, ResidualPredicateScanMode::AppliedDuringScan) => {
            Err(InternalError::query_executor_invariant(
                "data-row-only kernel rows must not apply residual predicates during scan",
            ))
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::Absent) => {
            Ok(KernelRowScanStrategy::RetainedFullRows {
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "retained full-row kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::AppliedDuringScan) => {
            Ok(KernelRowScanStrategy::RetainedFullRowsFiltered {
                predicate_program: predicate_slots
                    .ok_or_else(InternalError::scalar_page_predicate_slots_required)?,
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "retained full-row kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::DeferredPostAccess) => {
            Err(InternalError::query_executor_invariant(
                "retained full-row kernel rows must apply residual predicates during scan",
            ))
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::Absent) => {
            Ok(KernelRowScanStrategy::SlotOnlyRows {
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "slot-only kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::AppliedDuringScan) => {
            Ok(KernelRowScanStrategy::SlotOnlyRowsFiltered {
                predicate_program: predicate_slots
                    .ok_or_else(InternalError::scalar_page_predicate_slots_required)?,
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "slot-only kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::DeferredPostAccess) => {
            Err(InternalError::query_executor_invariant(
                "slot-only kernel rows must apply residual predicates during scan",
            ))
        }
    }
}

// Resolve the scalar post-access execution contract once from the residual
// predicate timing and the distinct-window shape already chosen for this plan.
fn resolve_post_access_strategy<'a>(
    plan: &AccessPlannedQuery,
    predicate_slots: Option<&'a PredicateProgram>,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    cursor_emission: CursorEmissionMode,
    retain_slot_rows: bool,
) -> Result<PostAccessStrategy<'a>, InternalError> {
    let predicate_strategy = match residual_predicate_scan_mode {
        ResidualPredicateScanMode::Absent => PostAccessPredicateStrategy::NotPresent,
        ResidualPredicateScanMode::AppliedDuringScan => {
            PostAccessPredicateStrategy::AppliedDuringScan
        }
        ResidualPredicateScanMode::DeferredPostAccess => PostAccessPredicateStrategy::Deferred {
            predicate_program: predicate_slots
                .ok_or_else(InternalError::scalar_page_predicate_slots_required)?,
        },
    };

    Ok(PostAccessStrategy {
        predicate_strategy,
        defer_retained_slot_distinct_window: plan.scalar_plan().distinct
            && !cursor_emission.enabled()
            && retain_slot_rows,
    })
}

// Execute one already-resolved direct `DataRow` strategy through the shared
// direct-lane scan and page-window shell.
fn execute_direct_data_row_path(
    plan: &AccessPlannedQuery,
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    consistency: MissingRowPolicy,
    continuation: &ScalarContinuationContext,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    direct_data_row_path: DirectDataRowPath<'_>,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    // Phase 1: record the chosen direct-lane family once before scan.
    #[cfg(any(test, feature = "structural-read-metrics"))]
    match direct_data_row_path {
        DirectDataRowPath::Plain { .. } => record_direct_data_row_path_hit(),
        DirectDataRowPath::Filtered { .. } => record_direct_filtered_data_row_path_hit(),
        DirectDataRowPath::MaterializedOrder {
            residual_predicate_scan_mode,
            ..
        } => match residual_predicate_scan_mode {
            ResidualPredicateScanMode::Absent => record_direct_data_row_path_hit(),
            ResidualPredicateScanMode::AppliedDuringScan => {
                record_direct_filtered_data_row_path_hit()
            }
            ResidualPredicateScanMode::DeferredPostAccess => {
                return Err(InternalError::query_executor_invariant(
                    "materialized-order direct data-row path cannot defer residual filtering",
                ));
            }
        },
    }

    // Phase 2: run the direct scan through the shared residual-policy helper.
    #[cfg(feature = "perf-attribution")]
    let (scan_local_instructions, scan_result) =
        measure_direct_data_row_phase(|| match direct_data_row_path {
            DirectDataRowPath::Plain { row_keep_cap } => {
                scan_direct_data_rows_with_residual_policy(
                    key_stream,
                    scan_budget_hint,
                    row_keep_cap,
                    consistency,
                    ResidualPredicateScanMode::Absent,
                    row_runtime,
                    None,
                    None,
                    "direct data-row path cannot defer residual filtering",
                )
            }
            DirectDataRowPath::Filtered {
                row_keep_cap,
                predicate_program,
                retained_slot_layout,
            } => scan_direct_data_rows_with_residual_policy(
                key_stream,
                scan_budget_hint,
                row_keep_cap,
                consistency,
                ResidualPredicateScanMode::AppliedDuringScan,
                row_runtime,
                Some(predicate_program),
                Some(retained_slot_layout),
                "direct filtered data-row path cannot defer residual filtering",
            ),
            DirectDataRowPath::MaterializedOrder {
                residual_predicate_scan_mode,
                predicate_program,
                retained_slot_layout,
                ..
            } => scan_materialized_order_direct_data_rows(
                key_stream,
                scan_budget_hint,
                consistency,
                residual_predicate_scan_mode,
                row_runtime,
                predicate_program,
                retained_slot_layout,
            ),
        });
    #[cfg(not(feature = "perf-attribution"))]
    let scan_result = match direct_data_row_path {
        DirectDataRowPath::Plain { row_keep_cap } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            consistency,
            ResidualPredicateScanMode::Absent,
            row_runtime,
            None,
            None,
            "direct data-row path cannot defer residual filtering",
        ),
        DirectDataRowPath::Filtered {
            row_keep_cap,
            predicate_program,
            retained_slot_layout,
        } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            consistency,
            ResidualPredicateScanMode::AppliedDuringScan,
            row_runtime,
            Some(predicate_program),
            Some(retained_slot_layout),
            "direct filtered data-row path cannot defer residual filtering",
        ),
        DirectDataRowPath::MaterializedOrder {
            residual_predicate_scan_mode,
            predicate_program,
            retained_slot_layout,
            ..
        } => scan_materialized_order_direct_data_rows(
            key_stream,
            scan_budget_hint,
            consistency,
            residual_predicate_scan_mode,
            row_runtime,
            predicate_program,
            retained_slot_layout,
        ),
    };
    let (mut data_rows, rows_scanned) = scan_result?;
    #[cfg(feature = "perf-attribution")]
    record_direct_data_row_scan_local_instructions(scan_local_instructions);

    // Phase 3: materialized-order direct lanes still own one in-memory order
    // pass before the final page window.
    if let DirectDataRowPath::MaterializedOrder { resolved_order, .. } = direct_data_row_path
        && data_rows.len() > 1
    {
        #[cfg(feature = "perf-attribution")]
        let (order_window_local_instructions, order_window_result) =
            measure_direct_data_row_phase(|| {
                apply_structural_order_window_to_data_rows(
                    &mut data_rows,
                    row_runtime.row_layout(),
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, None),
                )
            });
        #[cfg(not(feature = "perf-attribution"))]
        apply_structural_order_window_to_data_rows(
            &mut data_rows,
            row_runtime.row_layout(),
            resolved_order,
            ExecutionKernel::bounded_order_keep_count(plan, None),
        )?;
        #[cfg(feature = "perf-attribution")]
        order_window_result?;
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_order_window_local_instructions(order_window_local_instructions);
    }

    // Phase 4: direct-lane accounting matches the shared kernel path, then
    // the final offset/limit window runs once on canonical data rows.
    let post_access_rows = data_rows.len();
    #[cfg(feature = "perf-attribution")]
    let (page_window_local_instructions, page_window_result) =
        measure_direct_data_row_phase(|| {
            apply_data_row_page_window(plan, &mut data_rows);

            Ok::<(), InternalError>(())
        });
    #[cfg(not(feature = "perf-attribution"))]
    apply_data_row_page_window(plan, &mut data_rows);
    #[cfg(feature = "perf-attribution")]
    page_window_result?;
    #[cfg(feature = "perf-attribution")]
    record_direct_data_row_page_window_local_instructions(page_window_local_instructions);

    Ok((
        StructuralCursorPage::new(data_rows, None),
        rows_scanned,
        post_access_rows,
    ))
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
// structural data-row output and structural retained-slot-row output.
// Select that family once before the final row-shaping pass.
enum KernelRowFinalizeMode {
    StructuralDataRows {
        next_cursor: Option<PageCursor>,
    },
    #[cfg(feature = "sql")]
    StructuralSlotRows {
        next_cursor: Option<PageCursor>,
    },
}

// Select one final payload shape before converting kernel rows into their
// outward structural page boundary.
const fn select_kernel_row_finalize_mode(
    retain_slot_rows: bool,
    next_cursor: Option<PageCursor>,
) -> KernelRowFinalizeMode {
    #[cfg(feature = "sql")]
    if retain_slot_rows {
        return KernelRowFinalizeMode::StructuralSlotRows { next_cursor };
    }

    #[cfg(not(feature = "sql"))]
    let _ = retain_slot_rows;

    KernelRowFinalizeMode::StructuralDataRows { next_cursor }
}

// Finalize one already-materialized kernel row set without re-branching on
// output mode inside the per-row shaping loop.
fn finalize_kernel_rows_payload(
    plan: &AccessPlannedQuery,
    rows: Vec<KernelRow>,
    finalize_mode: KernelRowFinalizeMode,
) -> Result<MaterializedExecutionPayload, InternalError> {
    let _ = plan;

    match finalize_mode {
        KernelRowFinalizeMode::StructuralDataRows { next_cursor } => Ok(StructuralCursorPage::new(
            collect_kernel_data_rows(rows)?,
            next_cursor,
        )),
        #[cfg(feature = "sql")]
        KernelRowFinalizeMode::StructuralSlotRows { next_cursor } => Ok(
            StructuralCursorPage::new_with_slot_rows(collect_kernel_slot_rows(rows)?, next_cursor),
        ),
    }
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
        validate_prepared_projection_row(prepared_projection_validation, row)?;
    }

    Ok(())
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
    post_access_strategy: PostAccessStrategy<'_>,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: predicate filtering.
    let filtered = match post_access_strategy.predicate_strategy {
        PostAccessPredicateStrategy::NotPresent => false,
        PostAccessPredicateStrategy::AppliedDuringScan => true,
        PostAccessPredicateStrategy::Deferred { predicate_program } => {
            if rows.is_empty() {
                return Ok(0);
            }

            compact_kernel_rows_in_place(rows, |row| {
                let mut read_slot = |slot| row.slot_ref(slot);
                predicate_program.eval_with_slot_value_ref_reader(&mut read_slot)
            });

            true
        }
    };

    // Phase 2: ordering.
    let mut ordered = false;
    let mut rows_after_order = rows.len();
    if let Some(order) = logical.order.as_ref()
        && !order.fields.is_empty()
    {
        if post_access_strategy
            .predicate_strategy
            .requires_post_access_filtering()
            && !filtered
        {
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
        if post_access_strategy.defer_retained_slot_distinct_window {
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
    scan_strategy: KernelRowScanStrategy<'a>,
    continuation: &'a ScalarContinuationContext,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

///
/// KernelRowScanRequest
///
/// KernelRowScanRequest is the canonical executor-owned row scan contract for
/// structural key-stream materialization.
/// Both the generic scalar-page path and the row-collector short path
/// select one payload kernel through this boundary instead of duplicating the
/// same payload-mode dispatch locally.
///

pub(in crate::db::executor) struct KernelRowScanRequest<'a, 'r> {
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) scan_strategy: KernelRowScanStrategy<'a>,
    pub(in crate::db::executor) row_keep_cap: Option<usize>,
    pub(in crate::db::executor) row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

#[expect(clippy::too_many_lines)]
pub(in crate::db::executor) fn execute_kernel_row_scan(
    request: KernelRowScanRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
        row_keep_cap,
        row_runtime,
    } = request;

    // Phase 1: select the concrete row-read kernel once so the inner scan
    // loop does not branch on payload shape or predicate mode per row.
    match scan_strategy {
        KernelRowScanStrategy::DataRows => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_kernel_data_row_path_hit();

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel(key_stream, consistency, row_keep_cap, row_runtime)
            })
        }
        KernelRowScanStrategy::RetainedFullRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_kernel_full_row_retained_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "retained full-row kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        row_keep_cap,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::RetainedFullRowsFiltered {
            predicate_program,
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_kernel_full_row_retained_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "retained full-row kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel_with_predicate(
                        key_stream,
                        consistency,
                        predicate_program,
                        retained_slot_layout,
                        row_keep_cap,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::SlotOnlyRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_kernel_slots_only_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "slot-only kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        row_keep_cap,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::SlotOnlyRowsFiltered {
            predicate_program,
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_kernel_slots_only_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "slot-only kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel_with_predicate(
                        key_stream,
                        consistency,
                        predicate_program,
                        retained_slot_layout,
                        row_keep_cap,
                        row_runtime,
                    )
                },
            )
        }
    }
}

// Require one retained-slot layout and run the shared scalar read loop over
// one retained-row scan closure. Full-row-retained and slot-only kernel lanes
// both use this shell, so retained-layout enforcement lives in one place.
fn execute_retained_kernel_scan(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    missing_layout_message: &'static str,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
        &RetainedSlotLayout,
    ) -> Result<(Vec<KernelRow>, usize), InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let retained_slot_layout = retained_slot_layout
        .ok_or_else(|| InternalError::query_executor_invariant(missing_layout_message))?;

    execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
        scan_rows(key_stream, retained_slot_layout)
    })
}

fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        scan_strategy,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    execute_kernel_row_scan(KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
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

// Run one direct data-row read loop with one optional scan budget without
// paying the structural kernel-row envelope cost.
fn execute_scalar_data_row_read_loop(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
        Option<usize>,
    ) -> Result<(Vec<DataRow>, usize), InternalError>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    if row_keep_cap == Some(0) {
        return Ok((Vec::new(), 0));
    }

    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        return scan_rows(&mut budgeted, row_keep_cap);
    }

    scan_rows(key_stream, row_keep_cap)
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

// Evaluate one residual predicate against compact retained-slot values before
// the executor commits to a retained-row wrapper for the surviving row.
fn predicate_matches_retained_values(
    predicate_program: &PredicateProgram,
    retained_slot_layout: &RetainedSlotLayout,
    retained_values: &[Option<Value>],
) -> bool {
    predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
        let index = retained_slot_layout.value_index_for_slot(slot)?;

        retained_values.get(index).and_then(Option::as_ref)
    })
}

// Scan one ordered key stream directly into canonical data rows when the
// caller already proved no later phase needs a kernel-row wrapper.
fn scan_data_rows_direct(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_data_row(consistency, key)
    })
}

// Scan one ordered key stream directly into canonical data rows through one
// caller-selected row reader while preserving shared direct-lane key-stream
// and row-read attribution.
fn scan_data_rows_direct_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_data_row: impl FnMut(DataKey) -> Result<Option<DataRow>, InternalError>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = exact_output_key_count_hint(key_stream, None).map_or_else(
        || row_keep_cap.unwrap_or(0),
        |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
    );
    let mut data_rows = Vec::with_capacity(staged_capacity);

    loop {
        #[cfg(feature = "perf-attribution")]
        let (key_stream_local_instructions, read_result) =
            measure_direct_data_row_phase(|| key_stream.next_key());
        #[cfg(not(feature = "perf-attribution"))]
        let read_result = key_stream.next_key();
        let Some(key) = read_result? else {
            break;
        };
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_key_stream_local_instructions(key_stream_local_instructions);

        rows_scanned = rows_scanned.saturating_add(1);
        #[cfg(feature = "perf-attribution")]
        let (row_read_local_instructions, row_read_result) =
            measure_direct_data_row_phase(|| read_data_row(key));
        #[cfg(not(feature = "perf-attribution"))]
        let row_read_result = read_data_row(key);
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_row_read_local_instructions(row_read_local_instructions);
        let Some(data_row) = row_read_result? else {
            continue;
        };
        data_rows.push(data_row);
        if row_keep_cap.is_some_and(|cap| data_rows.len() >= cap) {
            break;
        }
    }

    Ok((data_rows, rows_scanned))
}

// Scan one ordered key stream directly into canonical data rows while
// applying the residual predicate during scan-time retained-slot decoding.
fn scan_data_rows_direct_with_predicate(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    predicate_program: &PredicateProgram,
    retained_slot_layout: &RetainedSlotLayout,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_data_row_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    })
}

// Run the materialized-order raw data-row lane through one residual-predicate
// policy helper so perf-attributed and normal scans share the same scan-time
// filtering contract.
fn scan_materialized_order_direct_data_rows(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    consistency: MissingRowPolicy,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    predicate_slots: Option<&PredicateProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_direct_data_rows_with_residual_policy(
        key_stream,
        scan_budget_hint,
        None,
        consistency,
        residual_predicate_scan_mode,
        row_runtime,
        predicate_slots,
        retained_slot_layout,
        "materialized-order direct data-row path cannot defer residual filtering",
    )
}

// Run one direct data-row scan through the shared residual-predicate timing
// contract so plain, filtered, and materialized-order raw lanes all choose
// the same row reader in one place.
fn scan_direct_data_rows_with_residual_policy(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    consistency: MissingRowPolicy,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    predicate_program: Option<&PredicateProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    deferred_filtering_message: &'static str,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    execute_scalar_data_row_read_loop(
        key_stream,
        scan_budget_hint,
        row_keep_cap,
        |key_stream, row_keep_cap| match residual_predicate_scan_mode {
            ResidualPredicateScanMode::Absent => {
                scan_data_rows_direct(key_stream, consistency, row_keep_cap, row_runtime)
            }
            ResidualPredicateScanMode::AppliedDuringScan => {
                let predicate_program = predicate_program.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "scan-time residual filtering requires one compiled predicate program",
                    )
                })?;
                let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "scan-time residual filtering requires one retained-slot layout",
                    )
                })?;

                scan_data_rows_direct_with_predicate(
                    key_stream,
                    consistency,
                    row_keep_cap,
                    row_runtime,
                    predicate_program,
                    retained_slot_layout,
                )
            }
            ResidualPredicateScanMode::DeferredPostAccess => Err(
                InternalError::query_executor_invariant(deferred_filtering_message),
            ),
        },
    )
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
    scan_full_retained_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_full_row_retained(consistency, key, retained_slot_layout)
    })
}

// Scan keys into full retained structural rows through one caller-selected
// row reader while preserving the shared kernel-row scan envelope.
fn scan_full_retained_rows_into_kernel_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| read_row(key))
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
    scan_full_retained_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
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
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_slot_only(consistency, &key, retained_slot_layout)
    })
}

// Scan keys into compact slot-only rows through one caller-selected row
// reader while preserving the shared kernel-row scan envelope.
fn scan_slot_rows_into_kernel_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| read_row(key))
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
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
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

// Apply one simple cursorless load page window directly on canonical data
// rows when route order is already final and no later slot-aware phase exists.
fn apply_data_row_page_window(plan: &AccessPlannedQuery, rows: &mut Vec<DataRow>) {
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

// Return the maximum number of route-ordered direct data rows worth staging
// before the final cursorless page window runs. These lanes never need one
// continuation lookahead row, so `offset + limit` is the real working set.
fn direct_data_row_keep_cap(plan: &AccessPlannedQuery) -> Option<usize> {
    let page = plan.scalar_plan().page.as_ref()?;
    let limit = page.limit?;

    Some(compute_page_keep_count(page.offset, limit))
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
    fn residual_predicate_scan_mode_fails_closed_by_row_capability() {
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(false, None),
            ResidualPredicateScanMode::Absent
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(true, None),
            ResidualPredicateScanMode::DeferredPostAccess
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(
                true,
                Some(&RetainedSlotLayout::compile(2, vec![0]))
            ),
            ResidualPredicateScanMode::AppliedDuringScan
        );
    }

    #[test]
    fn scalar_materialization_lane_metrics_capture_direct_and_kernel_paths() {
        let ((), metrics) = with_scalar_materialization_lane_metrics(|| {
            record_direct_data_row_path_hit();
            record_direct_filtered_data_row_path_hit();
            record_kernel_data_row_path_hit();
            record_kernel_full_row_retained_path_hit();
            record_kernel_slots_only_path_hit();
        });

        assert_eq!(
            metrics.direct_data_row_path_hits, 1,
            "direct data-row lane should increment once",
        );
        assert_eq!(
            metrics.direct_filtered_data_row_path_hits, 1,
            "direct filtered data-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_data_row_path_hits, 1,
            "kernel data-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_full_row_retained_path_hits, 1,
            "kernel retained full-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_slots_only_path_hits, 1,
            "kernel slot-only lane should increment once",
        );
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
