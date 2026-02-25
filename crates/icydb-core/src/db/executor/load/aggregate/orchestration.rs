use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionKernel, IndexPredicateCompileMode, compile_predicate_slots,
            fold::{AggregateKind, AggregateOutput, AggregateSpec},
            load::{
                LoadExecutor,
                aggregate::contracts::{
                    AggregateExecutionDescriptor, PreparedAggregateStreamingInputs,
                },
            },
            plan::{record_plan_metrics, record_rows_scanned},
        },
        query::{
            plan::{Direction, validate::validate_executor_plan},
            policy,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build the canonical aggregate execution descriptor so route/fold
    // boundaries consume one internal aggregate spec shape only.
    pub(in crate::db::executor) fn build_aggregate_execution_descriptor(
        plan: &ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateExecutionDescriptor, InternalError> {
        debug_assert!(
            policy::validate_plan_shape(plan.as_inner()).is_ok(),
            "aggregate executor received a plan shape that bypassed planning validation",
        );

        spec.ensure_supported_for_execution()
            .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;

        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(plan.as_inner())?;

        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let predicate_slots = compile_predicate_slots::<E>(plan.as_inner());
        let strict_index_predicate_program =
            predicate_slots
                .as_ref()
                .and_then(|resolved_predicate_slots| {
                    let index_slots =
                        Self::resolved_index_slots_for_access_path(&plan.as_inner().access)?;
                    ExecutionKernel::compile_index_predicate_program_from_slots(
                        resolved_predicate_slots,
                        index_slots.as_slice(),
                        IndexPredicateCompileMode::StrictAllOrNone,
                    )
                });
        let route_plan =
            Self::build_execution_route_plan_for_aggregate_spec(plan.as_inner(), spec.clone());
        let direction = route_plan.direction();

        Ok(AggregateExecutionDescriptor {
            spec,
            direction,
            route_plan,
            strict_index_predicate_program,
        })
    }

    // Execute one aggregate using an explicit aggregate spec. This keeps
    // unsupported aggregate taxonomy and route capability selection under one
    // shared boundary as field-target aggregates are introduced.
    pub(in crate::db::executor) fn execute_aggregate_spec(
        &self,
        plan: ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError> {
        ExecutionKernel::execute_aggregate_spec(self, plan, spec)
    }

    // Execute one aggregate terminal via canonical materialized load execution.
    // Field-target extrema and non-field terminals share this branch boundary.
    pub(in crate::db::executor) fn execute_materialized_aggregate_spec(
        &self,
        plan: ExecutablePlan<E>,
        spec: &AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError> {
        let kind = spec.kind();
        if let Some(target_field) = spec.target_field() {
            // Validate field-target semantics before any execution work so
            // unsupported targets fail without consuming scan budget.
            let field_slot = Self::resolve_orderable_field_slot(target_field)?;
            let response = self.execute(plan)?;

            return Self::aggregate_field_extrema_from_materialized(
                response,
                kind,
                target_field,
                field_slot,
            );
        }
        let response = self.execute(plan)?;

        Ok(Self::aggregate_from_materialized(response, kind))
    }

    // Execute `min(field)` / `max(field)` via canonical materialized fallback.
    // Route still owns eligibility and hint derivation; this branch currently
    // keeps field-target semantics correctness-first until fast paths are enabled.
    pub(in crate::db::executor) fn execute_field_target_extrema_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &crate::db::executor::ExecutionPlan,
    ) -> Result<AggregateOutput<E>, InternalError> {
        let field_fast_path_eligible = match kind {
            AggregateKind::Min => route_plan.field_min_fast_path_eligible(),
            AggregateKind::Max => route_plan.field_max_fast_path_eligible(),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "field-target aggregate execution requires MIN/MAX terminal",
                ));
            }
        };
        if !field_fast_path_eligible {
            return Err(InternalError::query_executor_invariant(
                "field-target aggregate streaming requires route-eligible field-extrema fast path",
            ));
        }

        // Validate user-provided field targets before any scan-budget consumption.
        let field_slot = Self::resolve_orderable_field_slot(target_field)?;

        // Route-planned streaming path for index-leading field extrema.
        let prepared = self.prepare_aggregate_streaming_inputs(plan)?;
        let execution_inputs = prepared.execution_inputs(direction);
        let mut resolved = ExecutionKernel::resolve_execution_key_stream(
            &execution_inputs,
            route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_field_extrema(
            &prepared.ctx,
            prepared.logical_plan.consistency,
            resolved.key_stream.as_mut(),
            target_field,
            field_slot,
            kind,
            direction,
        )?;
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Consume one executable aggregate plan into canonical streaming execution
    // inputs used by both aggregate streaming branches.
    pub(in crate::db::executor) fn prepare_aggregate_streaming_inputs(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<PreparedAggregateStreamingInputs<'_, E>, InternalError> {
        // Direction and specs must be read before consuming `ExecutablePlan`.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into logical + compiled predicate state.
        let logical_plan = plan.into_inner();
        let predicate_slots = compile_predicate_slots::<E>(&logical_plan);

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Recover read context and record plan metrics before stream resolution.
        let ctx = self.db.recovered_context::<E>()?;
        record_plan_metrics(&logical_plan.access);

        Ok(PreparedAggregateStreamingInputs {
            ctx,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
            predicate_slots,
        })
    }
}
