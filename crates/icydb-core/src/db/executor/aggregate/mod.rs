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

use crate::{
    db::{
        data::DataRow,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            PreparedAggregatePlan,
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntimeAdapter, LoadExecutor,
                PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path},
            planning::route::{
                RoutePlanRequest, aggregate_materialized_fold_direction, build_execution_route_plan,
            },
            terminal::RowLayout,
            validate_executor_plan_for_authority,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(in crate::db::executor) use capability::{
    AggregateExecutionPolicyInputs, derive_aggregate_execution_policy,
    field_target_is_tie_free_probe_target,
};
pub(in crate::db::executor) use contracts::{
    AggregateFoldMode, AggregateKind, ExecutionConfig, ExecutionContext, FoldControl, GroupError,
    ScalarAggregateEngine, ScalarAggregateOutput, execute_scalar_aggregate,
    execute_scalar_aggregate as execute_aggregate_engine,
};
pub(in crate::db::executor) use execution::{
    AggregateExecutionDescriptor, AggregateFastPathInputs, PreparedAggregateExecutionState,
    PreparedAggregateSpec, PreparedAggregateStreamingInputs, PreparedAggregateTargetField,
    PreparedCoveringDistinctStrategy, PreparedFieldOrderSensitiveTerminalOp,
    PreparedOrderSensitiveTerminalBoundary, PreparedOrderSensitiveTerminalOp,
    PreparedScalarNumericAggregateStrategy, PreparedScalarNumericBoundary, PreparedScalarNumericOp,
    PreparedScalarNumericPayload, PreparedScalarProjectionBoundary, PreparedScalarProjectionOp,
    PreparedScalarProjectionStrategy, PreparedScalarTerminalBoundary, PreparedScalarTerminalOp,
    PreparedScalarTerminalStrategy, ScalarProjectionWindow,
};
pub(in crate::db) use numeric::ScalarNumericFieldBoundaryRequest;
pub(in crate::db) use projection::{
    ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest,
};
pub(in crate::db) use terminals::{ScalarTerminalBoundaryOutput, ScalarTerminalBoundaryRequest};

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

    // Materialize one aggregate response page into structural rows plus the
    // authority-owned row layout used to decode those rows.
    pub(in crate::db::executor::aggregate) fn load_materialized_aggregate_rows(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<(Vec<DataRow>, RowLayout), InternalError> {
        let row_layout = prepared.authority.row_layout();
        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        Ok((rows, row_layout))
    }
}

impl ExecutionKernel {
    // Build one canonical aggregate descriptor from already-prepared aggregate
    // inputs so execution no longer reconstructs `PreparedExecutionPlan<E>` shells.
    pub(in crate::db::executor::aggregate) fn prepare_aggregate_execution_state_from_prepared(
        prepared: PreparedAggregateStreamingInputs<'_>,
        aggregate: PreparedAggregateSpec,
    ) -> PreparedAggregateExecutionState<'_> {
        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let route_plan = build_execution_route_plan(
            &prepared.logical_plan,
            RoutePlanRequest::Aggregate {
                aggregate: aggregate.route_shape(),
                execution_preparation: &prepared.execution_preparation,
            },
        )
        .expect("aggregate route planning should not fail for prepared aggregate requests");
        let direction = route_plan.direction();

        PreparedAggregateExecutionState {
            descriptor: AggregateExecutionDescriptor {
                aggregate,
                direction,
                route_plan,
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
        let execution_preparation = plan.execution_preparation();

        // Move the prepared aggregate plan into one structural runtime payload
        // once so aggregate execution does not clone lowered index specs.
        let (authority, logical_plan, index_prefix_specs, index_range_specs) =
            plan.into_streaming_parts()?;

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
            execution_preparation,
            index_prefix_specs,
            index_range_specs,
        })
    }

    // Execute one aggregate terminal via canonical materialized load execution.
    // Kernel owns field-target vs non-field reducer selection for this branch.
    fn execute_materialized_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        prepared: PreparedAggregateStreamingInputs<'_>,
        aggregate: &PreparedAggregateSpec,
    ) -> Result<ScalarAggregateOutput, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = aggregate.kind();
        if let Some(target_field) = aggregate.target_field() {
            let (rows, row_layout) = executor.load_materialized_aggregate_rows(prepared)?;

            return Self::aggregate_field_extrema_from_materialized(
                rows,
                &row_layout,
                kind,
                target_field.target_field_name(),
                target_field.field_slot(),
            );
        }
        let (rows, _) = executor.load_materialized_aggregate_rows(prepared)?;

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
        let ingest_all = |engine: &mut ScalarAggregateEngine| -> Result<(), InternalError> {
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
            ingest_all,
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

        // Phase 1: let eager reducers consume their owned descriptor directly
        // so aggregate execution does not clone descriptor state just to
        // decide whether it can skip canonical streaming fold execution.
        if descriptor.route_plan.is_materialized() {
            return Self::execute_materialized_aggregate_spec(
                executor,
                prepared,
                &descriptor.aggregate,
            );
        }
        if let Some(target_field) = descriptor.aggregate.target_field() {
            return Self::execute_field_target_extrema_aggregate(
                &prepared,
                kind,
                target_field.target_field_name(),
                target_field.field_slot(),
                descriptor.direction,
                &descriptor.route_plan,
            );
        }

        // Phase 2: continue through the canonical aggregate streaming fold
        // with the original prepared descriptor and prepared aggregate inputs.
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;

        let fast_path_inputs = AggregateFastPathInputs {
            logical_plan: &prepared.logical_plan,
            authority: prepared.authority,
            store: prepared.store,
            route_plan: &descriptor.route_plan,
            index_prefix_specs: prepared.index_prefix_specs.as_slice(),
            index_range_specs: prepared.index_range_specs.as_slice(),
            index_predicate_program: prepared.execution_preparation.strict_mode(),
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
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime_parts(
            &prepared.logical_plan.access,
            crate::db::executor::TraversalRuntime::new(
                prepared.store,
                prepared.authority.entity_tag(),
            ),
        );
        let execution_inputs = ExecutionInputs::new_prepared(
            &runtime,
            &prepared.logical_plan,
            AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                continuation: AccessScanContinuationInput::new(None, descriptor.direction),
            },
            &prepared.execution_preparation,
            ProjectionMaterializationMode::SharedValidation,
            PreparedExecutionProjection::empty(),
            false,
        );

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = execution_inputs.resolve_execution_key_stream(
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
