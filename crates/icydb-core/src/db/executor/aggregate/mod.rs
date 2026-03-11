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
#[cfg(test)]
mod tests;

use crate::db::executor::aggregate::field::{
    AggregateFieldValueError, resolve_orderable_aggregate_target_slot,
};
use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutablePlan, ExecutionKernel,
            ExecutionPreparation,
            pipeline::contracts::{ExecutionInputs, LoadExecutor},
            plan_metrics::{record_plan_metrics, record_rows_scanned},
            route::aggregate_materialized_fold_direction,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::builder::AggregateExpr,
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use contracts::{
    AggregateEngine, AggregateFoldMode, AggregateKind, AggregateOutput, AggregateState,
    AggregateStateFactory, ExecutionConfig, ExecutionContext, FoldControl, GroupError,
    TerminalAggregateState,
};
pub(in crate::db::executor) use execution::{
    AggregateExecutionDescriptor, AggregateFastPathInputs, PreparedAggregateStreamingInputs,
};

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
/// plan that must continue through canonical streaming fold execution.
///

// Streaming selection carries an owned executable plan; keep this explicit
// expectation until reducer migration shrinks the enum payload.
#[expect(clippy::large_enum_variant)]
enum AggregateReducerSelection<E: EntityKind + EntityValue> {
    Completed(AggregateOutput<E>),
    Streaming(ExecutablePlan<E>),
}

// Return true when executor-visible plan shape proves the effective aggregate
// input window is empty.
//
// Phase 1: honor explicit zero-window pagination contracts.
// Phase 2: honor planner-lowered empty by-keys access contracts.
pub(in crate::db::executor) fn aggregate_window_is_provably_empty<E>(
    plan: &ExecutablePlan<E>,
) -> bool
where
    E: EntityKind,
{
    plan.page_spec().is_some_and(|page| page.limit == Some(0))
        || plan
            .access()
            .resolve_strategy()
            .as_path()
            .is_some_and(|path| path.capabilities().is_by_keys_empty())
}

// Return one canonical terminal aggregate output when executor-visible plan
// shape proves the effective aggregate window is empty.
pub(in crate::db::executor) fn aggregate_zero_output_if_window_empty<E>(
    plan: &ExecutablePlan<E>,
    kind: AggregateKind,
) -> Option<AggregateOutput<E>>
where
    E: EntityKind,
{
    aggregate_window_is_provably_empty(plan).then(|| kind.zero_output())
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
    fn execute_or_stream<E>(
        self,
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
    ) -> Result<AggregateReducerSelection<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Materialized { aggregate } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_materialized_aggregate_spec(executor, plan, aggregate)?,
            )),
            Self::FieldExtremaStreaming {
                kind,
                target_field,
                direction,
                route_plan,
            } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_field_target_extrema_aggregate(
                    executor,
                    plan,
                    kind,
                    target_field,
                    direction,
                    route_plan,
                )?,
            )),
            Self::StreamingFold => Ok(AggregateReducerSelection::Streaming(plan)),
        }
    }
}

impl ExecutionKernel {
    // Validate aggregate spec contract at the executor boundary.
    // Planner owns semantic aggregate-shape validation; executor only fails
    // closed when a bypassed shape violates the terminal contract.
    fn validate_scalar_aggregate_spec_invariant(
        aggregate: &AggregateExpr,
    ) -> Result<(), InternalError> {
        if aggregate.target_field().is_some() && !aggregate.kind().supports_field_targets() {
            return Err(crate::db::error::query_executor_invariant(format!(
                "field-target aggregate requires MIN/MAX terminal after planning: found {:?}",
                aggregate.kind()
            )));
        }

        Ok(())
    }

    // Build one canonical aggregate descriptor so kernel dispatch works from a
    // validated shape with route-owned mode/direction/fold decisions.
    fn build_aggregate_execution_descriptor<E>(
        plan: ExecutablePlan<E>,
        aggregate: AggregateExpr,
    ) -> Result<(AggregateExecutionDescriptor, ExecutablePlan<E>), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Move to one logical-plan spine for route derivation and capability snapshots.
        let logical_plan = plan.into_inner();

        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(&logical_plan)?;

        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);

        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let route_plan =
            LoadExecutor::<E>::build_execution_route_plan_for_aggregate_spec_with_preparation(
                &logical_plan,
                aggregate.clone(),
                &execution_preparation,
            );
        let direction = route_plan.direction();

        // Rebuild executable contracts for downstream reducers after descriptor derivation.
        let plan = ExecutablePlan::new(logical_plan);

        Ok((
            AggregateExecutionDescriptor {
                aggregate,
                direction,
                route_plan,
                execution_preparation,
            },
            plan,
        ))
    }

    // Consume one executable aggregate plan into canonical streaming execution
    // inputs used by both aggregate streaming branches.
    fn prepare_aggregate_streaming_inputs<E>(
        executor: &'_ LoadExecutor<E>,
        plan: ExecutablePlan<E>,
    ) -> Result<PreparedAggregateStreamingInputs<'_, E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Direction and specs must be read before consuming `ExecutablePlan`.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into logical + compiled predicate state.
        let logical_plan = plan.into_inner();

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Recover read context and record plan metrics before stream resolution.
        let ctx = executor.recovered_context()?;
        record_plan_metrics(&logical_plan.access);

        Ok(PreparedAggregateStreamingInputs {
            ctx,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
        })
    }

    // Execute one aggregate terminal via canonical materialized load execution.
    // Kernel owns field-target vs non-field reducer selection for this branch.
    fn execute_materialized_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        aggregate: &AggregateExpr,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = aggregate.kind();
        if let Some(target_field) = aggregate.target_field() {
            // Validate field-target semantics before execution to preserve
            // fail-fast unsupported behavior without scan-budget consumption.
            let field_slot = resolve_orderable_aggregate_target_slot::<E>(target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let response = executor.execute(plan)?;

            return Self::aggregate_field_extrema_from_materialized(
                response,
                kind,
                target_field,
                field_slot,
            );
        }
        let response = executor.execute(plan)?;

        Self::aggregate_from_materialized(response, kind)
    }

    // Reduce one materialized response into a standard aggregate terminal
    // result using the shared aggregate state-machine boundary.
    fn aggregate_from_materialized<E>(
        response: EntityResponse<E>,
        kind: AggregateKind,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Materialized fallback can observe response order that is unrelated to
        // primary-key order. Use non-short-circuit directions for extrema so
        // MIN/MAX remain globally correct over the full response window.
        let direction = aggregate_materialized_fold_direction(kind);
        let mut engine = AggregateEngine::new_scalar(kind, direction);
        for row in response {
            let id = row.id();
            let data_key = DataKey::try_new::<E>(id.key())?;
            let fold_control = engine.ingest_scalar(&data_key)?;
            if matches!(fold_control, FoldControl::Break) {
                break;
            }
        }

        engine.finalize_scalar()
    }
    // Execute one scalar terminal aggregate stage through kernel-owned
    // orchestration while preserving route-owned execution-mode and fast-path
    // behavior.
    fn execute_scalar_terminal_stage<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        aggregate: AggregateExpr,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let (descriptor, plan) = Self::build_aggregate_execution_descriptor(plan, aggregate)?;
        let kind = descriptor.aggregate.kind();

        // Kernel-owned reducer adapter selection. Eager reducers return
        // immediately; streaming reducers continue through canonical key-stream
        // preparation and fold.
        let plan = match AggregateReducerDispatch::from_descriptor(&descriptor)
            .execute_or_stream(executor, plan)?
        {
            AggregateReducerSelection::Completed(aggregate_output) => return Ok(aggregate_output),
            AggregateReducerSelection::Streaming(plan) => plan,
        };
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;
        let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &prepared.ctx,
            logical_plan: &prepared.logical_plan,
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
            record_rows_scanned::<E>(rows_scanned);
            return Ok(aggregate_output);
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = ExecutionInputs::new(
            &prepared.ctx,
            &prepared.logical_plan,
            AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                continuation: AccessScanContinuationInput::new(None, descriptor.direction),
            },
            &descriptor.execution_preparation,
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
            &prepared.ctx,
            &prepared.logical_plan,
            kind,
            descriptor.direction,
            fold_mode,
            resolved.key_stream_mut(),
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Execute one aggregate spec through staged grouped/scalar orchestration.
    // Grouped stage currently validates contract shape and routes only
    // scalar-terminal shapes into the scalar execution stage.
    pub(in crate::db::executor) fn execute_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        aggregate: AggregateExpr,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        Self::validate_scalar_aggregate_spec_invariant(&aggregate)?;

        // Scalar terminal execution boundary.
        Self::execute_scalar_terminal_stage(executor, plan, aggregate)
    }
}
