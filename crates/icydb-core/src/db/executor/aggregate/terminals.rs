//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use crate::{
    db::{
        access::ExecutionPathKind,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
            ExecutablePlan, ExecutionKernel,
            aggregate::{
                AggregateFoldMode, AggregateKind, AggregateOutput,
                field::resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
            plan_metrics::record_rows_scanned,
            validate_executor_plan,
        },
        query::builder::aggregate::{count, exists, first, last, max, max_by, min, min_by},
        query::plan::FieldSlot as PlannedFieldSlot,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
static COVERING_EXISTS_FAST_PATH_HITS: AtomicU64 = AtomicU64::new(0);

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `count()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        if Self::primary_key_count_eligible(&plan) {
            return self.aggregate_count_from_existing_row_stream(plan);
        }

        if Self::index_covering_count_eligible(&plan) {
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

        // Predicates require row evaluation; index-only counting would be incorrect.
        if plan.has_predicate() {
            return false;
        }

        plan.access().as_index_prefix_path().is_some()
            || plan.access().as_index_range_path().is_some()
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
        COVERING_EXISTS_FAST_PATH_HITS.swap(0, Ordering::Relaxed)
    }

    #[cfg(test)]
    fn record_covering_exists_fast_path_hit_for_tests() {
        COVERING_EXISTS_FAST_PATH_HITS.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(test))]
    const fn record_covering_exists_fast_path_hit_for_tests() {}
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
