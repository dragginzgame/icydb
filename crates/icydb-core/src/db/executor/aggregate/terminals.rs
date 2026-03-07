//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use crate::{
    db::{
        access::{ExecutionPathKind, ExecutionPathPayload},
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings, Context,
            ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            aggregate::{
                AggregateFoldMode, AggregateKind, AggregateOutput,
                field::resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
            plan_metrics::record_rows_scanned,
            validate_executor_plan,
        },
        index::predicate::IndexPredicateExecution,
        query::builder::aggregate::{count, exists, first, last, max, max_by, min, min_by},
        query::plan::{FieldSlot as PlannedFieldSlot, PageSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
#[cfg(test)]
use std::cell::Cell;
use std::ops::Bound;

#[cfg(test)]
thread_local! {
    static COVERING_EXISTS_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
    static COVERING_COUNT_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
    static PK_CARDINALITY_COUNT_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `count()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        if Self::pk_cardinality_count_eligible(&plan) {
            Self::record_pk_cardinality_count_fast_path_hit_for_tests();
            return self.aggregate_count_from_pk_cardinality(plan);
        }

        if Self::primary_key_count_eligible(&plan) {
            return self.aggregate_count_from_existing_row_stream(plan);
        }

        if Self::index_covering_count_eligible(&plan) {
            Self::record_covering_count_fast_path_hit_for_tests();
            return self.aggregate_count_from_existing_row_stream(plan);
        }

        match ExecutionKernel::execute_aggregate_spec(self, plan, count())? {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(invariant("aggregate COUNT result kind mismatch")),
        }
    }

    /// Execute `exists()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        if Self::index_covering_exists_eligible(&plan) {
            Self::record_covering_exists_fast_path_hit_for_tests();
            return self.aggregate_exists_from_index_covering_stream(plan);
        }

        match ExecutionKernel::execute_aggregate_spec(self, plan, exists())? {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(invariant("aggregate EXISTS result kind mismatch")),
        }
    }

    /// Execute `min()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, min())? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(invariant("aggregate MIN result kind mismatch")),
        }
    }

    /// Execute `max()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, max())? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(invariant("aggregate MAX result kind mismatch")),
        }
    }

    /// Execute `min(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_min_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        match ExecutionKernel::execute_aggregate_spec(self, plan, min_by(target_field.field()))? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(invariant("aggregate MIN(field) result kind mismatch")),
        }
    }

    /// Execute `max(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_max_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        match ExecutionKernel::execute_aggregate_spec(self, plan, max_by(target_field.field()))? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(invariant("aggregate MAX(field) result kind mismatch")),
        }
    }

    /// Execute `nth(field, n)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_nth_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_nth_field_aggregate_with_slot(plan, target_field.field(), field_slot, nth)
    }

    /// Execute `median(field)` over the effective aggregate window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn aggregate_median_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_median_field_aggregate_with_slot(plan, target_field.field(), field_slot)
    }

    #[expect(clippy::type_complexity)]
    /// Execute paired extrema `min_max(field)` over the effective aggregate
    /// window using one planner-resolved field slot.
    pub(in crate::db) fn aggregate_min_max_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<(Id<E>, Id<E>)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;

        self.execute_min_max_field_aggregate_with_slot(plan, target_field.field(), field_slot)
    }

    /// Execute `first()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_first(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, first())? {
            AggregateOutput::First(value) => Ok(value),
            _ => Err(invariant("aggregate FIRST result kind mismatch")),
        }
    }

    /// Execute `last()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(self, plan, last())? {
            AggregateOutput::Last(value) => Ok(value),
            _ => Err(invariant("aggregate LAST result kind mismatch")),
        }
    }

    // Full-scan and key-range primary-key shapes can compute COUNT from
    // primary-store cardinality without row materialization or key decoding.
    fn pk_cardinality_count_eligible(plan: &ExecutablePlan<E>) -> bool {
        if plan.is_distinct() {
            return false;
        }
        if plan.has_predicate() {
            return false;
        }

        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };

        matches!(
            path.kind(),
            ExecutionPathKind::FullScan | ExecutionPathKind::KeyRange
        )
    }

    // Resolve COUNT for PK full-scan/key-range shapes from store cardinality
    // while preserving canonical page-window and scan-accounting semantics.
    fn aggregate_count_from_pk_cardinality(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        // Phase 1: snapshot pagination + access payload before resolving store cardinality.
        let page = plan.page_spec().cloned();
        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return Err(invariant(
                "pk cardinality COUNT fast path requires single-path access strategy",
            ));
        };

        // Phase 2: read candidate-row cardinality directly from primary storage.
        let available_rows = match path.payload() {
            ExecutionPathPayload::FullScan => self.recovered_context()?.with_store(
                |store| -> Result<usize, InternalError> {
                    let store_len = store.len();

                    Ok(usize::try_from(store_len).unwrap_or(usize::MAX))
                },
            )??,
            ExecutionPathPayload::KeyRange { start, end } => self
                .recovered_context()?
                .with_store(|store| -> Result<usize, InternalError> {
                    let start_raw = Context::<E>::data_key_from_key(**start)?.to_raw()?;
                    let end_raw = Context::<E>::data_key_from_key(**end)?.to_raw()?;
                    let count = store
                        .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                        .count();

                    Ok(count)
                })??,
            _ => {
                return Err(invariant(
                    "pk cardinality COUNT fast path requires full-scan or key-range access",
                ));
            }
        };

        // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
        let (count, rows_scanned) = count_window_result_from_page(page.as_ref(), available_rows);
        record_rows_scanned::<E>(rows_scanned);

        Ok(count)
    }

    // Primary-key point lookups can count without row materialization by
    // folding over one key while preserving missing-row consistency checks.
    fn primary_key_count_eligible(plan: &ExecutablePlan<E>) -> bool {
        // Keep the dedicated point-lookup fast path narrow: exact PK equality,
        // no residual predicate, and no explicit ordering contract.
        if plan.order_spec().is_some() {
            return false;
        }
        if plan.has_predicate() {
            return false;
        }

        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };

        path.kind() == ExecutionPathKind::ByKey
    }

    // Secondary index shapes can count without row materialization by folding
    // over key streams while still preserving missing-row consistency checks.
    fn index_covering_count_eligible(plan: &ExecutablePlan<E>) -> bool {
        // Ordered COUNT windows depend on route/kernel ordering contracts.
        // Keep this fast path scoped to unordered scalar COUNT shapes.
        if plan.order_spec().is_some() {
            return false;
        }

        // COUNT prefilter pushdown requires one strict all-or-none index
        // predicate program when a residual predicate exists.
        let index_shape_supported = plan.access().as_index_prefix_path().is_some()
            || plan.access().as_index_range_path().is_some();
        if !index_shape_supported {
            return false;
        }
        if !plan.has_predicate() {
            return true;
        }

        plan.execution_preparation().strict_mode().is_some()
    }

    // Fold COUNT over one key stream using `ExistingRows` mode.
    // This avoids entity decode/materialization while preserving stale-key and
    // strict-missing-row semantics via `row_exists_for_key`.
    fn aggregate_count_from_existing_row_stream(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        // Phase 1: collect lowered index specs before consuming the executable plan.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        validate_executor_plan::<E>(&logical_plan)?;
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);
        let index_predicate_execution =
            execution_preparation
                .strict_mode()
                .map(|program| IndexPredicateExecution {
                    program,
                    rejected_keys_counter: None,
                });

        // Phase 2: resolve the access key stream directly from index-backed bindings.
        let ctx = self.recovered_context()?;
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &logical_plan.access,
            AccessStreamBindings::new(
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, Direction::Asc),
            ),
            None,
            index_predicate_execution,
        );
        let mut key_stream = ctx.ordered_key_stream_from_access_descriptor(descriptor)?;

        // Phase 3: fold COUNT through existing-row semantics and record scan metrics.
        let (aggregate_output, rows_scanned) = ExecutionKernel::run_streaming_aggregate_reducer(
            &ctx,
            &logical_plan,
            AggregateKind::Count,
            Direction::Asc,
            AggregateFoldMode::ExistingRows,
            key_stream.as_mut(),
        )?;
        record_rows_scanned::<E>(rows_scanned);

        match aggregate_output {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(invariant("existing-row COUNT reducer result kind mismatch")),
        }
    }

    // Secondary index shapes can satisfy EXISTS using key-stream fold semantics
    // without row materialization when no residual predicate remains.
    fn index_covering_exists_eligible(plan: &ExecutablePlan<E>) -> bool {
        // Keep this fast path scoped to unordered scalar EXISTS shapes for now.
        if plan.order_spec().is_some() {
            return false;
        }

        // Residual predicates require row evaluation and must stay on canonical path.
        if plan.has_predicate() {
            return false;
        }

        plan.access().as_index_prefix_path().is_some()
            || plan.access().as_index_range_path().is_some()
    }

    // Fold EXISTS over an index-backed key stream using `ExistingRows` mode.
    // This keeps stale-key and strict-missing-row behavior aligned with the
    // canonical reducer path while avoiding row decode/materialization.
    fn aggregate_exists_from_index_covering_stream(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        // Phase 1: collect lowered index specs before consuming the executable plan.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        validate_executor_plan::<E>(&logical_plan)?;

        // Phase 2: resolve the access key stream directly from index-backed bindings.
        let ctx = self.recovered_context()?;
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &logical_plan.access,
            AccessStreamBindings::new(
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, Direction::Asc),
            ),
            None,
            None,
        );
        let mut key_stream = ctx.ordered_key_stream_from_access_descriptor(descriptor)?;

        // Phase 3: fold EXISTS through existing-row semantics and record scan metrics.
        let (aggregate_output, rows_scanned) = ExecutionKernel::run_streaming_aggregate_reducer(
            &ctx,
            &logical_plan,
            AggregateKind::Exists,
            Direction::Asc,
            AggregateFoldMode::ExistingRows,
            key_stream.as_mut(),
        )?;
        record_rows_scanned::<E>(rows_scanned);

        match aggregate_output {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(invariant("covering EXISTS reducer result kind mismatch")),
        }
    }

    #[cfg(test)]
    pub(crate) fn take_covering_exists_fast_path_hits_for_tests() -> u64 {
        COVERING_EXISTS_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    pub(crate) fn take_covering_count_fast_path_hits_for_tests() -> u64 {
        COVERING_COUNT_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    pub(crate) fn take_pk_cardinality_count_fast_path_hits_for_tests() -> u64 {
        PK_CARDINALITY_COUNT_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    fn record_covering_exists_fast_path_hit_for_tests() {
        COVERING_EXISTS_FAST_PATH_HITS.with(|counter| {
            counter.set(counter.get().saturating_add(1));
        });
    }

    #[cfg(test)]
    fn record_covering_count_fast_path_hit_for_tests() {
        COVERING_COUNT_FAST_PATH_HITS.with(|counter| {
            counter.set(counter.get().saturating_add(1));
        });
    }

    #[cfg(test)]
    fn record_pk_cardinality_count_fast_path_hit_for_tests() {
        PK_CARDINALITY_COUNT_FAST_PATH_HITS.with(|counter| {
            counter.set(counter.get().saturating_add(1));
        });
    }

    #[cfg(not(test))]
    const fn record_covering_exists_fast_path_hit_for_tests() {}

    #[cfg(not(test))]
    const fn record_covering_count_fast_path_hit_for_tests() {}

    #[cfg(not(test))]
    const fn record_pk_cardinality_count_fast_path_hit_for_tests() {}
}

// Map one candidate cardinality and optional page contract to canonical COUNT
// result and scan accounting (`rows_scanned`) semantics.
fn count_window_result_from_page(page: Option<&PageSpec>, available_rows: usize) -> (u32, usize) {
    let Some(page) = page else {
        return (usize_to_u32_saturating(available_rows), available_rows);
    };
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);

    match page.limit {
        Some(0) => (0, 0),
        Some(limit) => {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let rows_scanned = available_rows.min(offset.saturating_add(limit));
            let count = available_rows.saturating_sub(offset).min(limit);

            (usize_to_u32_saturating(count), rows_scanned)
        }
        None => {
            let count = available_rows.saturating_sub(offset);
            (usize_to_u32_saturating(count), available_rows)
        }
    }
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
