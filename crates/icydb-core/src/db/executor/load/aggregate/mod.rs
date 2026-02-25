mod distinct;
mod helpers;
mod numeric;
mod projection;

use crate::{
    db::{
        Context,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, ExecutablePlan, IndexPrefixSpec,
            IndexRangeSpec, IndexStreamConstraints, StreamExecutionHints,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
                compare_entities_by_orderable_field, compare_entities_for_field_extrema,
            },
            compile_predicate_slots,
            fold::{
                AggregateFoldMode, AggregateKind, AggregateOutput, AggregateReducerState,
                AggregateSpec, AggregateWindowState, FoldControl,
            },
            load::{
                LoadExecutor,
                execute::{ExecutionInputs, IndexPredicateCompileMode},
            },
            plan::{record_plan_metrics, record_rows_scanned},
            route::{
                ExecutionMode, ExecutionRoutePlan, FastPathOrder, RoutedKeyStreamRequest,
                ensure_index_range_aggregate_fast_path_specs,
                ensure_secondary_aggregate_fast_path_arity,
            },
        },
        index::predicate::{IndexPredicateExecution, IndexPredicateProgram},
        query::{
            ReadConsistency,
            plan::{AccessPath, AccessPlannedQuery, Direction, validate::validate_executor_plan},
            policy,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

// -----------------------------------------------------------------------------
// Aggregate Subdomains (Pre-Split Planning)
// -----------------------------------------------------------------------------
// 1) Terminal wrappers (`count/exists/min/max/first/last`).
// 2) Aggregate orchestration (validation, route, stream setup).
// 3) Fast-path dispatch via route-owned precedence.
// 4) Fast-path implementations by access shape.
// 5) Fallback + terminal utility helpers.

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue> {
    ctx: &'exec Context<'ctx, E>,
    logical_plan: &'exec AccessPlannedQuery<E::Key>,
    route_plan: &'exec ExecutionRoutePlan,
    index_prefix_specs: &'exec [IndexPrefixSpec],
    index_range_specs: &'exec [IndexRangeSpec],
    index_predicate_program: Option<&'exec IndexPredicateProgram>,
    direction: Direction,
    physical_fetch_hint: Option<usize>,
    kind: AggregateKind,
    fold_mode: AggregateFoldMode,
}

///
/// VerifiedAggregateFastPathRoute
///
/// Capability marker returned only by aggregate fast-path eligibility verification.
/// Fast-path branch dispatch requires this marker so branch execution cannot skip
/// the shared gate by accident.
///

struct VerifiedAggregateFastPathRoute {
    route: FastPathOrder,
}

///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from a terminal
/// aggregate spec and validated plan shape before execution branching.
///

struct AggregateExecutionDescriptor {
    spec: AggregateSpec,
    direction: Direction,
    route_plan: ExecutionRoutePlan,
    strict_index_predicate_program: Option<IndexPredicateProgram>,
    force_materialized_due_to_predicate_uncertainty: bool,
    extrema_streaming_attempt_eligible: bool,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // ------------------------------------------------------------------
    // Terminal wrappers
    // ------------------------------------------------------------------

    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        match self
            .execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::Count))?
        {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate COUNT result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        match self
            .execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::Exists))?
        {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate EXISTS result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::Min))? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::Max))? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match self.execute_aggregate_spec(
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, target_field),
        )? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match self.execute_aggregate_spec(
            plan,
            AggregateSpec::for_target_field(AggregateKind::Max, target_field),
        )? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_nth_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();

        self.execute_nth_field_aggregate(plan, target_field.as_str(), nth)
    }

    pub(in crate::db) fn aggregate_median_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();

        self.execute_median_field_aggregate(plan, target_field.as_str())
    }

    pub(in crate::db) fn aggregate_min_max_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<MinMaxByIds<E>, InternalError> {
        let target_field = target_field.into();

        self.execute_min_max_field_aggregate(plan, target_field.as_str())
    }

    pub(in crate::db) fn aggregate_first(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self
            .execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::First))?
        {
            AggregateOutput::First(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate FIRST result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate_spec(plan, AggregateSpec::for_terminal(AggregateKind::Last))? {
            AggregateOutput::Last(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate LAST result kind mismatch",
            )),
        }
    }

    // ------------------------------------------------------------------
    // Aggregate orchestration
    // ------------------------------------------------------------------

    // Build the canonical aggregate execution descriptor so route/fold
    // boundaries consume one internal aggregate spec shape only.
    fn build_aggregate_execution_descriptor(
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
        let strict_index_predicate_program = Self::compile_index_predicate_program(
            plan.as_inner(),
            predicate_slots.as_ref(),
            IndexPredicateCompileMode::StrictAllOrNone,
        );
        let force_materialized_due_to_predicate_uncertainty = plan.as_inner().predicate.is_some()
            && Self::resolved_index_slots_for_access_path(&plan.as_inner().access).is_some()
            && strict_index_predicate_program.is_none();
        let extrema_streaming_attempt_eligible = Self::extrema_streaming_attempt_eligible(
            plan.as_inner(),
            &spec,
            strict_index_predicate_program.as_ref(),
            force_materialized_due_to_predicate_uncertainty,
        );
        let route_plan =
            Self::build_execution_route_plan_for_aggregate_spec(plan.as_inner(), spec.clone());
        let direction = route_plan.direction();

        Ok(AggregateExecutionDescriptor {
            spec,
            direction,
            route_plan,
            strict_index_predicate_program,
            force_materialized_due_to_predicate_uncertainty,
            extrema_streaming_attempt_eligible,
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
        let descriptor = Self::build_aggregate_execution_descriptor(&plan, spec)?;
        let kind = descriptor.spec.kind();

        // Snapshot route-owned execution mode at the orchestration boundary.
        // This remains immutable for the full aggregate execution lifecycle.
        let execution_mode = descriptor.route_plan.execution_mode;
        if let Some(target_field) = descriptor.spec.target_field() {
            return self.execute_field_target_extrema_aggregate(
                plan,
                kind,
                target_field,
                descriptor.direction,
                &descriptor.route_plan,
                descriptor.extrema_streaming_attempt_eligible,
                descriptor.force_materialized_due_to_predicate_uncertainty,
            );
        }
        let first_last_streaming_attempt_eligible = descriptor.extrema_streaming_attempt_eligible
            && matches!(kind, AggregateKind::First | AggregateKind::Last);
        if matches!(execution_mode, ExecutionMode::Materialized)
            && !first_last_streaming_attempt_eligible
            || descriptor.force_materialized_due_to_predicate_uncertainty
        {
            let response = self.execute(plan)?;
            return Ok(Self::aggregate_from_materialized(response, kind));
        }
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;

        // Direction must be captured before consuming the ExecutablePlan.
        // After `into_inner()`, execution uses logical plan + executor-prepared predicate slots.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into logical + compiled predicate state.
        // After this point, `plan` is consumed.
        let logical_plan = plan.into_inner();
        let predicate_slots = compile_predicate_slots::<E>(&logical_plan);

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Obtain recovered execution context (read-consistency aware).
        let ctx = self.db.recovered_context::<E>()?;

        // Record plan-level metrics before execution begins.
        // This mirrors the load execution path.
        record_plan_metrics(&logical_plan.access);

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &ctx,
            logical_plan: &logical_plan,
            route_plan: &descriptor.route_plan,
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            index_predicate_program: descriptor.strict_index_predicate_program.as_ref(),
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
            ctx: &ctx,
            plan: &logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: index_prefix_specs.as_slice(),
                index_range_specs: index_range_specs.as_slice(),
                index_range_anchor: None,
                direction: descriptor.direction,
            },
            predicate_slots: predicate_slots.as_ref(),
        };

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            &descriptor.route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;

        // Fold via one streaming engine. COUNT pushdown uses key-only mode;
        // other terminals use row-existence mode.
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            &ctx,
            &logical_plan,
            logical_plan.consistency,
            descriptor.direction,
            resolved.key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Execute `min(field)` / `max(field)` via canonical materialized fallback.
    // Route still owns eligibility and hint derivation; this branch currently
    // keeps field-target semantics correctness-first until fast paths are enabled.
    #[expect(clippy::too_many_arguments)]
    fn execute_field_target_extrema_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &ExecutionRoutePlan,
        extrema_streaming_attempt_eligible: bool,
        force_materialized_due_to_predicate_uncertainty: bool,
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

        // Validate user-provided field targets before any scan-budget consumption.
        let field_slot = Self::resolve_orderable_field_slot(target_field)?;
        if force_materialized_due_to_predicate_uncertainty
            || !field_fast_path_eligible
            || !extrema_streaming_attempt_eligible
        {
            // Preserve canonical query semantics by selecting candidates from the
            // fully materialized response window and then applying field-extrema rules.
            let response = self.execute(plan)?;
            return Self::aggregate_field_extrema_from_materialized(
                response,
                kind,
                target_field,
                field_slot,
            );
        }

        // Route-planned streaming path for index-leading field extrema.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        let predicate_slots = compile_predicate_slots::<E>(&logical_plan);
        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;
        record_plan_metrics(&logical_plan.access);
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: index_prefix_specs.as_slice(),
                index_range_specs: index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            predicate_slots: predicate_slots.as_ref(),
        };
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_field_extrema(
            &ctx,
            logical_plan.consistency,
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

    // Keep ordered-extrema streaming coverage explicit and conservative.
    // This gate normalizes when extrema terminals can attempt streaming while
    // preserving fallback-first behavior for DISTINCT, post-sort, and residual
    // predicate uncertainty.
    fn extrema_streaming_attempt_eligible(
        plan: &AccessPlannedQuery<E::Key>,
        spec: &AggregateSpec,
        strict_index_predicate_program: Option<&IndexPredicateProgram>,
        force_materialized_due_to_predicate_uncertainty: bool,
    ) -> bool {
        let first_last_terminal = matches!(
            (spec.kind(), spec.target_field()),
            (AggregateKind::First | AggregateKind::Last, None)
        );
        let field_extrema_terminal = matches!(
            (spec.kind(), spec.target_field()),
            (AggregateKind::Min | AggregateKind::Max, Some(_))
        );
        if !first_last_terminal && !field_extrema_terminal {
            return false;
        }
        if force_materialized_due_to_predicate_uncertainty || plan.distinct {
            return false;
        }

        if plan.predicate.is_some() && strict_index_predicate_program.is_none() {
            return false;
        }
        if first_last_terminal && plan.budget_safety_metadata::<E>().requires_post_access_sort {
            return false;
        }

        true
    }

    // ------------------------------------------------------------------
    // Fast-path dispatch
    // ------------------------------------------------------------------

    // Shared aggregate fast-path eligibility verifier.
    //
    // All aggregate fast-path dispatch must pass through this gate before
    // invoking any `try_execute_*` branch so route eligibility checks, arity
    // guards, and branch preconditions cannot drift across call sites.
    fn verify_aggregate_fast_path_eligibility(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        route: FastPathOrder,
    ) -> Result<Option<VerifiedAggregateFastPathRoute>, InternalError> {
        match route {
            // Primary-key point/batch aggregate fast path is branch-local and
            // intentionally independent of route capability flags.
            FastPathOrder::PrimaryKey => Ok(Some(VerifiedAggregateFastPathRoute { route })),
            FastPathOrder::SecondaryPrefix => {
                ensure_secondary_aggregate_fast_path_arity(
                    inputs.route_plan.secondary_fast_path_eligible(),
                    inputs.index_prefix_specs.len(),
                )?;
                if inputs.route_plan.secondary_fast_path_eligible() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            // Primary-scan aggregate fast path is only attempted when route
            // planning provided a bounded probe hint for this terminal.
            FastPathOrder::PrimaryScan => {
                if inputs.physical_fetch_hint.is_some() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            FastPathOrder::IndexRange => {
                ensure_index_range_aggregate_fast_path_specs(
                    inputs.route_plan.index_range_limit_fast_path_enabled(),
                    inputs.index_prefix_specs.len(),
                    inputs.index_range_specs.len(),
                )?;
                if inputs.route_plan.index_range_limit_fast_path_enabled() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            FastPathOrder::Composite => {
                if inputs.route_plan.composite_aggregate_fast_path_eligible() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    // Execute one aggregate fast-path branch only after route verification has
    // produced a capability marker from the shared eligibility gate.
    fn try_execute_verified_aggregate_fast_path(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        verified_route: VerifiedAggregateFastPathRoute,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        match verified_route.route {
            FastPathOrder::PrimaryKey => {
                // Aggregate-aware fast path for primary-key point/batch access shapes.
                // This keeps semantics identical while avoiding generic stream setup.
                Self::try_execute_primary_key_access_aggregate(
                    inputs.ctx,
                    inputs.logical_plan,
                    inputs.direction,
                    inputs.kind,
                    inputs.fold_mode,
                )
            }
            FastPathOrder::SecondaryPrefix => {
                // Aggregate-aware fast path for secondary index-prefix plans that are
                // eligible for canonical order pushdown.
                Self::try_execute_index_prefix_aggregate(
                    inputs.ctx,
                    inputs,
                    inputs.direction,
                    inputs.kind,
                    inputs.fold_mode,
                )
            }
            FastPathOrder::PrimaryScan => {
                // Aggregate-aware fast path for primary-data range/full scans.
                // This reuses canonical fold logic while skipping generic stream routing.
                Self::try_execute_primary_scan_aggregate(
                    inputs.ctx,
                    inputs.logical_plan,
                    inputs.direction,
                    inputs.physical_fetch_hint,
                    inputs.kind,
                    inputs.fold_mode,
                )
            }
            FastPathOrder::IndexRange => Self::try_execute_index_range_aggregate(inputs),
            FastPathOrder::Composite => Self::try_execute_composite_aggregate(inputs),
        }
    }

    // Attempt aggregate fast-path execution strictly through route-owned
    // fast-path order. Returns `Some` when one branch fully resolves the terminal.
    fn try_fast_path_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        for route in inputs.route_plan.fast_path_order().iter().copied() {
            let Some(verified_route) = Self::verify_aggregate_fast_path_eligibility(inputs, route)?
            else {
                continue;
            };

            if let Some((aggregate_output, rows_scanned)) =
                Self::try_execute_verified_aggregate_fast_path(inputs, verified_route)?
            {
                return Ok(Some((aggregate_output, rows_scanned)));
            }
        }

        // Fast exit: effective limit == 0 has an empty aggregate window and can
        // return terminal defaults without constructing or scanning key streams.
        if inputs.physical_fetch_hint == Some(0) {
            return Ok(Some((Self::aggregate_zero_window_result(inputs.kind), 0)));
        }

        Ok(None)
    }

    // ------------------------------------------------------------------
    // Fast-path implementations
    // ------------------------------------------------------------------

    // Streaming reducer for index-leading field extrema. This keeps execution in
    // key-stream mode and stops once the first non-tie worse field value appears.
    fn fold_streaming_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
        target_field: &str,
        field_slot: FieldSlot,
        kind: AggregateKind,
        direction: Direction,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        if direction != Self::field_extrema_aggregate_direction(kind)? {
            return Err(InternalError::query_executor_invariant(
                "field-extrema fold direction must match aggregate terminal semantics",
            ));
        }

        let mut keys_scanned = 0usize;
        let mut selected: Option<(Id<E>, E)> = None;

        while let Some(data_key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            let Some(entity) = Self::read_entity_for_field_extrema(ctx, consistency, &data_key)?
            else {
                continue;
            };
            let id = Id::from_key(data_key.try_key::<E>()?);
            let selected_was_empty = selected.is_none();
            let candidate_replaces = match selected.as_ref() {
                Some((_, current)) => {
                    compare_entities_for_field_extrema(
                        &entity,
                        current,
                        target_field,
                        field_slot,
                        direction,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?
                        == Ordering::Less
                }
                None => true,
            };
            if candidate_replaces {
                selected = Some((id, entity));
                if selected_was_empty && matches!(kind, AggregateKind::Min) {
                    // MIN(field) under ascending index-leading traversal is resolved
                    // by the first in-window existing row.
                    break;
                }
                continue;
            }

            let Some((_, current)) = selected.as_ref() else {
                continue;
            };
            let field_order =
                compare_entities_by_orderable_field(&entity, current, target_field, field_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            let directional_field_order = apply_aggregate_direction(field_order, direction);

            // Once traversal leaves the winning field-value group, the ordered
            // stream cannot produce a better extrema candidate.
            if directional_field_order == Ordering::Greater {
                break;
            }
        }

        let selected_id = selected.map(|(id, _)| id);
        let output = match kind {
            AggregateKind::Min => AggregateOutput::Min(selected_id),
            AggregateKind::Max => AggregateOutput::Max(selected_id),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "field-extrema fold reached non-extrema terminal",
                ));
            }
        };

        Ok((output, keys_scanned))
    }

    // Resolve aggregate terminals directly for primary-key point/batch plans.
    // This preserves consistency + window semantics without building streams.
    fn try_execute_primary_key_access_aggregate(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        let ordered_keys = match path {
            AccessPath::ByKey(key) => vec![*key],
            AccessPath::ByKeys(keys) => {
                let mut deduped = Context::<E>::dedup_keys(keys.clone());
                if direction == Direction::Desc {
                    deduped.reverse();
                }

                deduped
            }
            _ => return Ok(None),
        };
        if ordered_keys.is_empty() {
            return Ok(None);
        }
        if plan.predicate.is_some() {
            return Ok(None);
        }

        // Phase 1: apply window exhaustion before touching storage.
        let mut window = AggregateWindowState::from_plan(plan);
        if window.exhausted() {
            return Ok(Some((Self::aggregate_zero_window_result(kind), 0)));
        }

        // Phase 2: iterate canonical candidate keys and enforce the same
        // consistency + window semantics used by streaming aggregation.
        let mut keys_scanned = 0usize;
        let mut state = AggregateReducerState::for_kind(kind);
        for key in ordered_keys {
            if window.exhausted() {
                break;
            }

            keys_scanned = keys_scanned.saturating_add(1);
            let data_key = Context::<E>::data_key_from_key(key)?;
            if !Self::key_qualifies_for_fold(ctx, plan.consistency, fold_mode, &data_key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }
            if matches!(
                state.update_from_data_key(kind, direction, &data_key)?,
                FoldControl::Break
            ) {
                break;
            }
        }

        // Phase 3: project one terminal output from the reducer state.
        let aggregate_output = state.into_output();

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-prefix access plans when
    // canonical secondary ordering is pushdown-eligible.
    fn try_execute_index_prefix_aggregate(
        ctx: &Context<'_, E>,
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        // Probe hint selection is route-owned; executor only consumes it.
        let probe_fetch_hint = inputs.route_plan.secondary_extrema_probe_fetch_hint();
        let index_predicate_execution =
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program);
        let Some(mut fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
            probe_fetch_hint,
            index_predicate_execution,
        )?
        else {
            return Ok(None);
        };
        let key_comparator = super::key_stream_comparator_from_plan(inputs.logical_plan, direction);
        fast.ordered_key_stream = Self::maybe_wrap_distinct_stream(
            fast.ordered_key_stream,
            inputs.logical_plan.distinct,
            key_comparator,
        );

        let probe_rows_scanned = fast.rows_scanned;
        if let Some(fetch) = probe_fetch_hint {
            debug_assert!(
                probe_rows_scanned <= fetch,
                "secondary extrema probe rows_scanned must not exceed bounded fetch",
            );
        }
        let (probe_output, _probe_keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            direction,
            fast.ordered_key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        if !Self::secondary_extrema_probe_requires_fallback(
            inputs.logical_plan.consistency,
            kind,
            probe_fetch_hint,
            &probe_output,
            probe_rows_scanned,
        ) {
            return Ok(Some((probe_output, probe_rows_scanned)));
        }

        // MissingOk + bounded secondary probe can under-fetch when leading index
        // entries are stale. Retry unbounded to preserve terminal correctness.
        let Some(mut fallback) = Self::try_execute_secondary_index_order_stream(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
            // Keep native index traversal order for fallback retries.
            Some(usize::MAX),
            index_predicate_execution,
        )?
        else {
            return Ok(None);
        };
        fallback.ordered_key_stream = Self::maybe_wrap_distinct_stream(
            fallback.ordered_key_stream,
            inputs.logical_plan.distinct,
            key_comparator,
        );
        let fallback_rows_scanned = fallback.rows_scanned;
        let (aggregate_output, _fallback_keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            direction,
            fallback.ordered_key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((
            aggregate_output,
            probe_rows_scanned.saturating_add(fallback_rows_scanned),
        )))
    }

    // Resolve aggregate terminals directly for full-scan/key-range access plans.
    // This keeps canonical stream semantics while avoiding generic route assembly.
    fn try_execute_primary_scan_aggregate(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        if !matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. }) {
            return Ok(None);
        }

        let mut key_stream = Self::resolve_routed_key_stream(
            ctx,
            RoutedKeyStreamRequest::AccessPath {
                access: path,
                constraints: IndexStreamConstraints {
                    prefix: None,
                    range: None,
                    anchor: None,
                },
                direction,
                hints: StreamExecutionHints {
                    physical_fetch_hint,
                    predicate_execution: None,
                },
            },
        )?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            plan,
            plan.consistency,
            direction,
            key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-range access plans.
    // This reuses canonical range traversal while preserving one fold engine.
    fn try_execute_index_range_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(index_range_limit_spec) = inputs.route_plan.index_range_limit_spec.as_ref() else {
            return Ok(None);
        };

        let Some(mut fast) = Self::try_execute_index_range_limit_pushdown_stream(
            inputs.ctx,
            inputs.logical_plan,
            inputs.index_range_specs.first(),
            None,
            inputs.direction,
            index_range_limit_spec.fetch,
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program),
        )?
        else {
            return Ok(None);
        };
        let key_comparator =
            super::key_stream_comparator_from_plan(inputs.logical_plan, inputs.direction);
        fast.ordered_key_stream = Self::maybe_wrap_distinct_stream(
            fast.ordered_key_stream,
            inputs.logical_plan.distinct,
            key_comparator,
        );

        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            fast.ordered_key_stream.as_mut(),
            inputs.kind,
            inputs.fold_mode,
        )?;

        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Resolve aggregate terminals directly for composite access plans by
    // reusing canonical composite stream production.
    fn try_execute_composite_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let stream_request = AccessPlanStreamRequest {
            access: &inputs.logical_plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: inputs.index_prefix_specs,
                index_range_specs: inputs.index_range_specs,
                index_range_anchor: None,
                direction: inputs.direction,
            },
            key_comparator: super::key_stream_comparator_from_plan(
                inputs.logical_plan,
                inputs.direction,
            ),
            physical_fetch_hint: inputs.physical_fetch_hint,
            index_predicate_execution: Self::aggregate_index_predicate_execution(
                inputs.index_predicate_program,
            ),
        };
        let mut key_stream = Self::resolve_routed_key_stream(
            inputs.ctx,
            RoutedKeyStreamRequest::AccessPlan(stream_request),
        )?;

        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            key_stream.as_mut(),
            inputs.kind,
            inputs.fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Build one optional index-only predicate execution request for aggregate
    // stream producers from a strict-compiled index predicate program.
    #[expect(clippy::single_option_map)]
    fn aggregate_index_predicate_execution(
        program: Option<&IndexPredicateProgram>,
    ) -> Option<IndexPredicateExecution<'_>> {
        program.map(|program| IndexPredicateExecution {
            program,
            rejected_keys_counter: None,
        })
    }

    // ------------------------------------------------------------------
    // Fallback and terminal utilities
    // ------------------------------------------------------------------
}
