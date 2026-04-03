//! Module: executor::aggregate
//! Responsibility: aggregate execution orchestration, reducers, and aggregate contracts.
//! Does not own: logical aggregate planning or access-path lowering semantics.
//! Boundary: executor-owned aggregate runtime behavior over executable plans.

pub(in crate::db::executor) mod capability;
mod contracts;
mod distinct;
mod execution;
mod fast_path;
pub(in crate::db::executor) mod field;
mod field_extrema;
mod helpers;
mod materialized_distinct;
mod numeric;
mod projection;
pub(in crate::db::executor) mod runtime;
mod terminals;

use crate::db::executor::aggregate::field::{
    AggregateFieldValueError, resolve_orderable_aggregate_target_slot_with_model,
};
use crate::{
    db::{
        data::DataRow,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            ExecutionPreparation, PreparedAggregatePlan,
            pipeline::contracts::{ExecutionInputs, ExecutionRuntimeAdapter, LoadExecutor},
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path},
            route::aggregate_materialized_fold_direction,
            terminal::RowLayout,
            validate_executor_plan_for_authority,
        },
        index::IndexCompilePolicy,
        query::builder::AggregateExpr,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use capability::{
    AggregateExecutionPolicyInputs, derive_aggregate_execution_policy_for_model,
    field_target_is_tie_free_probe_target_for_model,
};
pub(in crate::db::executor) use contracts::{
    AggregateFoldMode, AggregateKind, ExecutionConfig, ExecutionContext, FoldControl, GroupError,
    GroupedAggregateEngine, ScalarAggregateEngine, ScalarAggregateOutput, execute_scalar_aggregate,
    execute_scalar_aggregate as execute_aggregate_engine,
};
pub(in crate::db::executor) use execution::{
    AggregateExecutionDescriptor, AggregateFastPathInputs, PreparedAggregateExecutionState,
    PreparedAggregateStreamingInputs, PreparedAggregateStreamingInputsCore,
    PreparedCoveringDistinctStrategy, PreparedScalarNumericBoundary,
    PreparedScalarNumericExecutionState, PreparedScalarNumericOp, PreparedScalarNumericStrategy,
    PreparedScalarProjectionBoundary, PreparedScalarProjectionExecutionState,
    PreparedScalarProjectionOp, PreparedScalarProjectionStrategy, PreparedScalarTerminalBoundary,
    PreparedScalarTerminalExecutionState, PreparedScalarTerminalOp, PreparedScalarTerminalStrategy,
    ScalarProjectionWindow,
};
pub(in crate::db) use numeric::ScalarNumericFieldBoundaryRequest;
pub(in crate::db) use projection::ScalarProjectionBoundaryRequest;
pub(in crate::db) use terminals::ScalarTerminalBoundaryRequest;

///
/// AggregateReducerDispatch
///
/// AggregateReducerDispatch maps one aggregate descriptor to one kernel-owned
/// reducer execution adapter. This keeps orchestration declarative and avoids
/// re-deriving execution-mode semantics at call sites.
///

enum AggregateReducerDispatch<'a> {
    Materialized {
        aggregate: &'a AggregateExpr,
    },
    FieldExtremaStreaming {
        kind: AggregateKind,
        target_field: &'a str,
        direction: Direction,
        route_plan: &'a crate::db::executor::ExecutionPlan,
    },
    StreamingFold,
}

///
/// AggregateReducerSelection
///
/// AggregateReducerSelection carries either a completed aggregate output or a
/// prepared execution state that must continue through canonical streaming fold
/// execution.
///

#[expect(clippy::large_enum_variant)]
enum AggregateReducerSelection<'ctx> {
    Completed(ScalarAggregateOutput),
    Streaming(PreparedAggregateExecutionState<'ctx>),
}

impl<'a> AggregateReducerDispatch<'a> {
    // Derive one reducer adapter from a validated aggregate descriptor.
    fn from_descriptor(descriptor: &'a AggregateExecutionDescriptor) -> Self {
        if descriptor.route_plan.shape().is_materialized() {
            return Self::Materialized {
                aggregate: &descriptor.aggregate,
            };
        }
        if let Some(target_field) = descriptor.aggregate.target_field() {
            return Self::FieldExtremaStreaming {
                kind: descriptor.aggregate.kind(),
                target_field,
                direction: descriptor.direction,
                route_plan: &descriptor.route_plan,
            };
        }

        Self::StreamingFold
    }

    // Execute eager reducers immediately, or defer to canonical streaming fold.
    fn execute_or_stream<'ctx, E>(
        self,
        executor: &LoadExecutor<E>,
        descriptor: AggregateExecutionDescriptor,
        prepared: PreparedAggregateStreamingInputs<'ctx>,
    ) -> Result<AggregateReducerSelection<'ctx>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Materialized { aggregate } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_materialized_aggregate_spec(
                    executor, prepared, aggregate,
                )?,
            )),
            Self::FieldExtremaStreaming {
                kind,
                target_field,
                direction,
                route_plan,
            } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_field_target_extrema_aggregate(
                    &prepared,
                    kind,
                    target_field,
                    direction,
                    route_plan,
                )?,
            )),
            Self::StreamingFold => Ok(AggregateReducerSelection::Streaming(
                PreparedAggregateExecutionState {
                    descriptor,
                    prepared,
                },
            )),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Consume one typed scalar aggregate wrapper plan into the canonical
    // prepared aggregate boundary payload before handing execution to
    // prepared-state helpers.
    pub(in crate::db::executor::aggregate) fn prepare_scalar_aggregate_boundary(
        &self,
        plan: PreparedAggregatePlan,
    ) -> Result<PreparedAggregateStreamingInputs<'_>, InternalError> {
        ExecutionKernel::prepare_aggregate_streaming_inputs(self, plan)
    }
}

impl ExecutionKernel {
    // Build one canonical aggregate descriptor from already-prepared aggregate
    // inputs so execution no longer reconstructs `ExecutablePlan<E>` shells.
    pub(in crate::db::executor::aggregate) fn prepare_aggregate_execution_state_from_prepared(
        prepared: PreparedAggregateStreamingInputs<'_>,
        aggregate: AggregateExpr,
    ) -> PreparedAggregateExecutionState<'_> {
        let slot_map = crate::db::executor::preparation::resolved_index_slots_for_access_path(
            prepared.authority.model(),
            prepared.logical_plan.access.resolve_strategy().executable(),
        );
        let execution_preparation = ExecutionPreparation::from_plan(
            prepared.authority.model(),
            &prepared.logical_plan,
            slot_map,
        );

        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let route_plan =
            crate::db::executor::route::build_execution_route_plan_for_aggregate_spec_with_model(
                prepared.authority.model(),
                &prepared.logical_plan,
                aggregate.clone(),
                &execution_preparation,
            );
        let direction = route_plan.direction();

        PreparedAggregateExecutionState {
            descriptor: AggregateExecutionDescriptor {
                aggregate,
                direction,
                route_plan,
                execution_preparation,
            },
            prepared,
        }
    }

    // Consume one executable aggregate plan into canonical streaming execution
    // inputs used by both aggregate streaming branches.
    pub(in crate::db::executor::aggregate) fn prepare_aggregate_streaming_inputs<E>(
        executor: &'_ LoadExecutor<E>,
        plan: PreparedAggregatePlan,
    ) -> Result<PreparedAggregateStreamingInputs<'_>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let authority = plan.authority();
        // Direction and specs must be read before consuming `ExecutablePlan`.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into logical plan form once so aggregate paths keep setup
        // structural after the typed executor boundary.
        let logical_plan = plan.into_plan();

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan_for_authority(authority, &logical_plan)?;
        let store = executor.db.recovered_store(authority.store_path())?;
        let store_resolver = executor.db.store_resolver();
        record_plan_metrics(&logical_plan.access);

        Ok(PreparedAggregateStreamingInputs {
            store_resolver,
            authority,
            store,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
        })
    }

    // Execute one aggregate terminal via canonical materialized load execution.
    // Kernel owns field-target vs non-field reducer selection for this branch.
    fn execute_materialized_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        prepared: PreparedAggregateStreamingInputs<'_>,
        aggregate: &AggregateExpr,
    ) -> Result<ScalarAggregateOutput, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = aggregate.kind();
        if let Some(target_field) = aggregate.target_field() {
            // Validate field-target semantics before execution to preserve
            // fail-fast unsupported behavior without scan-budget consumption.
            let field_slot = resolve_orderable_aggregate_target_slot_with_model(
                prepared.authority.model(),
                target_field,
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            let row_layout = RowLayout::from_model(prepared.authority.model());
            let page = executor.execute_scalar_materialized_page_stage(prepared)?;
            let (rows, _) = page.into_parts();

            return Self::aggregate_field_extrema_from_materialized(
                rows,
                &row_layout,
                kind,
                target_field,
                field_slot,
            );
        }
        let page = executor.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Self::aggregate_from_materialized(rows, kind)
    }

    // Reduce one materialized response into a standard aggregate terminal
    // result using the shared aggregate state-machine boundary.
    fn aggregate_from_materialized(
        rows: Vec<DataRow>,
        kind: AggregateKind,
    ) -> Result<ScalarAggregateOutput, InternalError> {
        // Materialized fallback can observe response order that is unrelated to
        // primary-key order. Use non-short-circuit directions for extrema so
        // MIN/MAX remain globally correct over the full response window.
        let direction = aggregate_materialized_fold_direction(kind);
        let mut ingest_all = |engine: &mut ScalarAggregateEngine| -> Result<(), InternalError> {
            for (data_key, _) in &rows {
                let fold_control = engine.ingest(data_key)?;
                if matches!(fold_control, FoldControl::Break) {
                    break;
                }
            }

            Ok(())
        };

        execute_aggregate_engine(
            ScalarAggregateEngine::new_scalar(kind, direction),
            &mut ingest_all,
        )
    }
    // Execute one aggregate terminal stage through kernel-owned
    // orchestration while preserving route-owned execution-mode and fast-path
    // behavior.
    pub(in crate::db::executor::aggregate) fn execute_prepared_aggregate_state<E>(
        executor: &LoadExecutor<E>,
        state: PreparedAggregateExecutionState<'_>,
    ) -> Result<ScalarAggregateOutput, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = state.descriptor.aggregate.kind();
        let descriptor = state.descriptor;
        let prepared = state.prepared;

        // Kernel-owned reducer adapter selection. Eager reducers return
        // immediately; streaming reducers continue through canonical key-stream
        // execution using the already-prepared aggregate stage.
        let state = match AggregateReducerDispatch::from_descriptor(&descriptor).execute_or_stream(
            executor,
            descriptor.clone(),
            prepared,
        )? {
            AggregateReducerSelection::Completed(aggregate_output) => return Ok(aggregate_output),
            AggregateReducerSelection::Streaming(state) => state,
        };
        let PreparedAggregateExecutionState {
            descriptor,
            prepared,
        } = state;
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;

        let fast_path_inputs = AggregateFastPathInputs {
            logical_plan: &prepared.logical_plan,
            authority: prepared.authority,
            store: prepared.store,
            route_plan: &descriptor.route_plan,
            index_prefix_specs: prepared.index_prefix_specs.as_slice(),
            index_range_specs: prepared.index_range_specs.as_slice(),
            index_predicate_program: descriptor.execution_preparation.strict_mode(),
            direction: descriptor.direction,
            physical_fetch_hint,
            kind,
            fold_mode,
        };
        // Policy boundary: all aggregate optimizations must dispatch through the
        // route-owned fast-path order below (no ad-hoc kind-specialized branches
        // in executor call sites).
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_fast_path_aggregate(&fast_path_inputs)?
        {
            record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);
            return Ok(aggregate_output);
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let runtime = ExecutionRuntimeAdapter::from_runtime_parts(
            &prepared.logical_plan.access,
            crate::db::executor::TraversalRuntime::new(
                prepared.store,
                prepared.authority.entity_tag(),
            ),
            prepared.store,
            prepared.authority.model(),
        );
        let execution_inputs = ExecutionInputs::new(
            &runtime,
            &prepared.logical_plan,
            AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                continuation: AccessScanContinuationInput::new(None, descriptor.direction),
            },
            &descriptor.execution_preparation,
            true,
            false,
        );

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            &descriptor.route_plan,
            IndexCompilePolicy::StrictAllOrNone,
        )?;

        // Fold through the canonical kernel reducer runner. Dispatch-level
        // field-target/materialized decisions were already handled above.
        let (aggregate_output, keys_scanned) = Self::run_streaming_aggregate_reducer(
            prepared.store,
            &prepared.logical_plan,
            kind,
            descriptor.direction,
            fold_mode,
            resolved.key_stream_mut(),
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);
        record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);

        Ok(aggregate_output)
    }
}
