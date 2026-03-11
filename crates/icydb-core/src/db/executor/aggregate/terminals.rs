//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use crate::{
    db::{
        access::ExecutionPathPayload,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings, Context,
            ExecutablePlan, ExecutionKernel, ExecutionOptimizationCounter, ExecutionPreparation,
            aggregate::{
                AggregateFoldMode, AggregateKind, AggregateOutput,
                aggregate_zero_output_if_window_empty,
                field::resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned,
            route::{CountTerminalFastPathContract, ExistsTerminalFastPathContract},
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
use std::ops::Bound;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `count()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Count)
        {
            return Self::expect_count_output(
                aggregate_output,
                "aggregate COUNT zero-window result kind mismatch",
            );
        }

        if let Some(contract) = Self::derive_count_terminal_fast_path_contract(&plan) {
            return match contract {
                CountTerminalFastPathContract::PrimaryKeyCardinality => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath,
                    );
                    self.aggregate_count_from_pk_cardinality(plan)
                }
                CountTerminalFastPathContract::PrimaryKeyExistingRows(direction) => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::PrimaryKeyCountFastPath,
                    );
                    self.aggregate_count_from_existing_row_stream(plan, direction)
                }
                CountTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::CoveringCountFastPath,
                    );
                    self.aggregate_count_from_existing_row_stream(plan, direction)
                }
            };
        }

        Self::expect_count_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, count())?,
            "aggregate COUNT result kind mismatch",
        )
    }

    /// Execute `exists()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Exists)
        {
            return Self::expect_exists_output(
                aggregate_output,
                "aggregate EXISTS zero-window result kind mismatch",
            );
        }

        if let Some(contract) = Self::derive_exists_terminal_fast_path_contract(&plan) {
            return match contract {
                ExistsTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::CoveringExistsFastPath,
                    );
                    self.aggregate_exists_from_index_covering_stream(plan, direction)
                }
            };
        }

        Self::expect_exists_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, exists())?,
            "aggregate EXISTS result kind mismatch",
        )
    }

    /// Execute `min()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Min)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::Min,
                "aggregate MIN zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, min())?,
            AggregateKind::Min,
            "aggregate MIN result kind mismatch",
        )
    }

    /// Execute `max()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Max)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::Max,
                "aggregate MAX zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, max())?,
            AggregateKind::Max,
            "aggregate MAX result kind mismatch",
        )
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
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Min)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::Min,
                "aggregate MIN(field) zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, min_by(target_field.field()))?,
            AggregateKind::Min,
            "aggregate MIN(field) result kind mismatch",
        )
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
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Max)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::Max,
                "aggregate MAX(field) zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, max_by(target_field.field()))?,
            AggregateKind::Max,
            "aggregate MAX(field) result kind mismatch",
        )
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

    /// Execute paired extrema `min_max(field)` over the effective aggregate
    /// window using one planner-resolved field slot.
    #[expect(clippy::type_complexity)]
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
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::First)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::First,
                "aggregate FIRST zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, first())?,
            AggregateKind::First,
            "aggregate FIRST result kind mismatch",
        )
    }

    /// Execute `last()` over the effective aggregate window.
    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        if let Some(aggregate_output) =
            aggregate_zero_output_if_window_empty(&plan, AggregateKind::Last)
        {
            return Self::expect_optional_id_terminal_output(
                aggregate_output,
                AggregateKind::Last,
                "aggregate LAST zero-window result kind mismatch",
            );
        }

        Self::expect_optional_id_terminal_output(
            ExecutionKernel::execute_aggregate_spec(self, plan, last())?,
            AggregateKind::Last,
            "aggregate LAST result kind mismatch",
        )
    }

    // Decode COUNT outputs while preserving call-site mismatch context.
    fn expect_count_output(
        aggregate_output: AggregateOutput<E>,
        mismatch_context: &'static str,
    ) -> Result<u32, InternalError> {
        match aggregate_output {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
        }
    }

    // Decode EXISTS outputs while preserving call-site mismatch context.
    fn expect_exists_output(
        aggregate_output: AggregateOutput<E>,
        mismatch_context: &'static str,
    ) -> Result<bool, InternalError> {
        match aggregate_output {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
        }
    }

    // Decode id-returning aggregate outputs for MIN/MAX/FIRST/LAST terminals.
    fn expect_optional_id_terminal_output(
        aggregate_output: AggregateOutput<E>,
        kind: AggregateKind,
        mismatch_context: &'static str,
    ) -> Result<Option<Id<E>>, InternalError> {
        match (kind, aggregate_output) {
            (AggregateKind::Min, AggregateOutput::Min(value))
            | (AggregateKind::Max, AggregateOutput::Max(value))
            | (AggregateKind::First, AggregateOutput::First(value))
            | (AggregateKind::Last, AggregateOutput::Last(value)) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
        }
    }

    // Resolve an index-backed existing-row key stream and execute one reducer kind.
    fn aggregate_existing_rows_terminal_output(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        direction: Direction,
    ) -> Result<AggregateOutput<E>, InternalError> {
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
                AccessScanContinuationInput::new(None, direction),
            ),
            None,
            index_predicate_execution,
        );
        let mut key_stream = ctx.ordered_key_stream_from_access_descriptor(descriptor)?;

        // Phase 3: fold through existing-row semantics and record scan metrics.
        let (aggregate_output, rows_scanned) = ExecutionKernel::run_streaming_aggregate_reducer(
            &ctx,
            &logical_plan,
            kind,
            direction,
            AggregateFoldMode::ExistingRows,
            key_stream.as_mut(),
        )?;
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
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
            return Err(crate::db::error::query_executor_invariant(
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
                return Err(crate::db::error::query_executor_invariant(
                    "pk cardinality COUNT fast path requires full-scan or key-range access",
                ));
            }
        };

        // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
        let (count, rows_scanned) = count_window_result_from_page(page.as_ref(), available_rows);
        record_rows_scanned::<E>(rows_scanned);

        Ok(count)
    }

    // Fold COUNT over one key stream using `ExistingRows` mode.
    // This avoids entity decode/materialization while preserving stale-key and
    // strict-missing-row semantics via `row_exists_for_key`.
    fn aggregate_count_from_existing_row_stream(
        &self,
        plan: ExecutablePlan<E>,
        direction: Direction,
    ) -> Result<u32, InternalError> {
        let aggregate_output =
            self.aggregate_existing_rows_terminal_output(plan, AggregateKind::Count, direction)?;

        Self::expect_count_output(
            aggregate_output,
            "existing-row COUNT reducer result kind mismatch",
        )
    }

    // Fold EXISTS over an index-backed key stream using `ExistingRows` mode.
    // This keeps stale-key and strict-missing-row behavior aligned with the
    // canonical reducer path while avoiding row decode/materialization.
    fn aggregate_exists_from_index_covering_stream(
        &self,
        plan: ExecutablePlan<E>,
        direction: Direction,
    ) -> Result<bool, InternalError> {
        let aggregate_output =
            self.aggregate_existing_rows_terminal_output(plan, AggregateKind::Exists, direction)?;

        Self::expect_exists_output(
            aggregate_output,
            "covering EXISTS reducer result kind mismatch",
        )
    }
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
