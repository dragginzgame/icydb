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
mod numeric;
mod projection;
mod terminals;
#[cfg(test)]
mod tests;

pub(in crate::db::executor) use contracts::{
    AggregateEngine, AggregateFoldMode, AggregateKind, AggregateOutput, AggregateSpec,
    AggregateState, AggregateStateFactory, ExecutionConfig, ExecutionContext, FoldControl,
    GroupError, TerminalAggregateState, ensure_grouped_spec_supported_for_execution,
};
pub(in crate::db::executor) use execution::{
    AggregateExecutionDescriptor, AggregateFastPathInputs, PreparedAggregateStreamingInputs,
};

use crate::db::executor::aggregate::field::{
    AggregateFieldValueError, resolve_orderable_aggregate_target_slot,
};
use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            load::{ExecutionInputs, LoadExecutor},
            plan_metrics::{record_plan_metrics, record_rows_scanned},
            route::ExecutionMode,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::policy,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
        spec: &'a AggregateSpec,
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

impl<'a> AggregateReducerDispatch<'a> {
    // Derive one reducer adapter from a validated aggregate descriptor.
    fn from_descriptor(descriptor: &'a AggregateExecutionDescriptor) -> Self {
        if matches!(
            descriptor.route_plan.execution_mode,
            ExecutionMode::Materialized
        ) {
            return Self::Materialized {
                spec: &descriptor.spec,
            };
        }
        if let Some(target_field) = descriptor.spec.target_field() {
            return Self::FieldExtremaStreaming {
                kind: descriptor.spec.kind(),
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
            Self::Materialized { spec } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_materialized_aggregate_spec(executor, plan, spec)?,
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
    // Build one canonical aggregate descriptor so kernel dispatch works from a
    // validated shape with route-owned mode/direction/fold decisions.
    fn build_aggregate_execution_descriptor<E>(
        plan: &ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateExecutionDescriptor, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        debug_assert!(
            policy::validate_plan_shape(&plan.as_inner().logical).is_ok(),
            "aggregate executor received a plan shape that bypassed planning validation",
        );

        spec.ensure_supported_for_execution()
            .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;

        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(plan.as_inner())?;

        let execution_preparation = ExecutionPreparation::for_plan::<E>(plan.as_inner());

        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let route_plan =
            LoadExecutor::<E>::build_execution_route_plan_for_aggregate_spec_with_preparation(
                plan.as_inner(),
                spec.clone(),
                &execution_preparation,
            );
        let direction = route_plan.direction();

        Ok(AggregateExecutionDescriptor {
            spec,
            direction,
            route_plan,
            execution_preparation,
        })
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
        spec: &AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = spec.kind();
        if let Some(target_field) = spec.target_field() {
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
        response: Response<E>,
        kind: AggregateKind,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Materialized fallback can observe response order that is unrelated to
        // primary-key order. Use non-short-circuit directions for extrema so
        // MIN/MAX remain globally correct over the full response window.
        let direction = kind.materialized_fold_direction();
        let mut engine = AggregateEngine::new_scalar(kind, direction);
        for (id, _) in response {
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
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let descriptor = Self::build_aggregate_execution_descriptor(&plan, spec)?;
        let kind = descriptor.spec.kind();

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
        let execution_inputs = ExecutionInputs {
            ctx: &prepared.ctx,
            plan: &prepared.logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction: descriptor.direction,
            },
            execution_preparation: &descriptor.execution_preparation,
        };

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
            resolved.key_stream.as_mut(),
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Execute one aggregate spec through staged grouped/scalar orchestration.
    // Grouped stage currently validates contract shape and routes only
    // scalar-terminal shapes into the scalar execution stage.
    pub(in crate::db::executor) fn execute_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Scalar terminal execution boundary.
        Self::execute_scalar_terminal_stage(executor, plan, spec)
    }
}
