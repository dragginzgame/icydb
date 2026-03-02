//! Module: executor::load
//! Responsibility: load-path execution orchestration, pagination, and trace contracts.
//! Does not own: logical planning semantics or relation/commit mutation policy.
//! Boundary: consumes executable load plans and delegates post-access semantics to kernel.
#![deny(unreachable_patterns)]

mod execute;
mod fast_stream;
mod index_range_limit;
mod page;
mod pk_stream;
mod secondary_index;
mod terminal;

pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
};

use crate::{
    db::{
        Context, Db, GroupedRow,
        contracts::canonical_value_compare,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, GroupedContinuationToken,
            GroupedPlannedCursor, PlannedCursor, decode_pk_cursor_boundary,
        },
        data::DataKey,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionOptimization,
            ExecutionPreparation, ExecutionTrace, KeyOrderComparator, OrderedKeyStreamBox,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, extract_numeric_field_decimal,
                extract_orderable_field_value, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot, resolve_orderable_aggregate_target_slot,
            },
            aggregate::{AggregateKind, AggregateOutput, FoldControl, GroupError},
            group::{
                CanonicalKey, GroupKeySet, KeyCanonicalError, grouped_budget_observability,
                grouped_execution_context_from_planner_config,
            },
            plan_metrics::{
                GroupedPlanMetricsStrategy, record_grouped_plan_metrics, record_plan_metrics,
                record_rows_scanned,
            },
            range_token_anchor_key, range_token_from_cursor_anchor,
            route::aggregate_materialized_fold_direction,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
        predicate::{CompareOp, MissingRowPolicy},
        query::plan::{
            AccessPlannedQuery, GroupAggregateSpec, GroupDistinctPolicyReason, GroupHavingSpec,
            GroupHavingSymbol, LogicalPlan, evaluate_grouped_having_compare_v1,
            grouped_cursor_policy_violation, grouped_executor_handoff,
            resolve_global_distinct_field_aggregate,
        },
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};
use std::{cmp::Ordering, marker::PhantomData};

///
/// PageCursor
///
/// Internal continuation cursor enum for scalar and grouped pagination.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PageCursor {
    Scalar(ContinuationToken),
    Grouped(GroupedContinuationToken),
}

impl PageCursor {
    /// Borrow scalar continuation token when this cursor is scalar-shaped.
    #[must_use]
    pub(in crate::db) const fn as_scalar(&self) -> Option<&ContinuationToken> {
        match self {
            Self::Scalar(token) => Some(token),
            Self::Grouped(_) => None,
        }
    }

    /// Borrow grouped continuation token when this cursor is grouped-shaped.
    #[must_use]
    pub(in crate::db) const fn as_grouped(&self) -> Option<&GroupedContinuationToken> {
        match self {
            Self::Scalar(_) => None,
            Self::Grouped(token) => Some(token),
        }
    }
}

impl From<ContinuationToken> for PageCursor {
    fn from(value: ContinuationToken) -> Self {
        Self::Scalar(value)
    }
}

impl From<GroupedContinuationToken> for PageCursor {
    fn from(value: GroupedContinuationToken) -> Self {
        Self::Grouped(value)
    }
}

///
/// CursorPage
///
/// Internal load page result with continuation cursor payload.
/// Returned by paged executor entrypoints.
///

#[derive(Debug)]
pub(crate) struct CursorPage<E: EntityKind> {
    pub(crate) items: Response<E>,

    pub(crate) next_cursor: Option<PageCursor>,
}

///
/// GroupedCursorPage
///
/// Internal grouped page result with grouped rows and continuation cursor payload.
///
#[derive(Debug)]
pub(in crate::db) struct GroupedCursorPage {
    pub(in crate::db) rows: Vec<GroupedRow>,
    pub(in crate::db) next_cursor: Option<PageCursor>,
}

/// Resolve key-stream comparator contract from runtime direction.
pub(in crate::db::executor) const fn key_stream_comparator_from_direction(
    direction: Direction,
) -> KeyOrderComparator {
    KeyOrderComparator::from_direction(direction)
}

///
/// FastPathKeyResult
///
/// Internal fast-path access result.
/// Carries ordered keys plus observability metadata for shared execution phases.
///

pub(in crate::db::executor) struct FastPathKeyResult {
    pub(in crate::db::executor) ordered_key_stream: OrderedKeyStreamBox,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) optimization: ExecutionOptimization,
}

///
/// LoadExecutor
///
/// Load-plan executor with canonical post-access semantics.
/// Coordinates fast paths, trace hooks, and pagination cursors.
///

#[derive(Clone)]
pub(crate) struct LoadExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

///
/// GroupedRouteStage
///
/// Route-planning stage payload for grouped execution.
/// Owns grouped handoff extraction, grouped route contracts, and grouped
/// execution metadata before runtime stream resolution starts.
///

struct GroupedRouteStage<E: EntityKind + EntityValue> {
    plan: AccessPlannedQuery<E::Key>,
    cursor: GroupedPlannedCursor,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
    grouped_execution: crate::db::query::plan::GroupedExecutionConfig,
    group_fields: Vec<crate::db::query::plan::FieldSlot>,
    grouped_aggregates: Vec<GroupAggregateSpec>,
    grouped_having: Option<GroupHavingSpec>,
    grouped_route_plan: crate::db::executor::ExecutionPlan,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    global_distinct_field_aggregate: Option<(AggregateKind, String)>,
    execution_trace: Option<ExecutionTrace>,
}

///
/// GroupedStreamStage
///
/// Stream-construction stage payload for grouped execution.
/// Owns recovered context, execution preparation, and resolved grouped key
/// stream for fold-phase consumption.
///

struct GroupedStreamStage<'a, E: EntityKind + EntityValue> {
    ctx: Context<'a, E>,
    execution_preparation: ExecutionPreparation,
    resolved: ResolvedExecutionKeyStream,
}

///
/// GroupedFoldStage
///
/// Fold-phase output payload for grouped execution.
/// Owns grouped page materialization plus observability counters consumed by
/// the final output stage.
///

struct GroupedFoldStage {
    page: GroupedCursorPage,
    filtered_rows: usize,
    check_filtered_rows_upper_bound: bool,
    rows_scanned: usize,
    optimization: Option<ExecutionOptimization>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped: u64,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one load executor bound to a database handle and debug mode.
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    /// Recover one canonical read context for kernel-owned execution setup.
    pub(in crate::db::executor) fn recovered_context(
        &self,
    ) -> Result<crate::db::Context<'_, E>, InternalError> {
        self.db.recovered_context::<E>()
    }

    // Resolve one orderable aggregate target field into a stable slot with
    // canonical field-error taxonomy mapping.
    pub(in crate::db::executor) fn resolve_orderable_field_slot(
        target_field: &str,
    ) -> Result<FieldSlot, InternalError> {
        resolve_orderable_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Resolve one aggregate target field into a stable slot with canonical
    // field-error taxonomy mapping.
    pub(in crate::db::executor) fn resolve_any_field_slot(
        target_field: &str,
    ) -> Result<FieldSlot, InternalError> {
        resolve_any_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Resolve one numeric aggregate target field into a stable slot with
    // canonical field-error taxonomy mapping.
    pub(in crate::db::executor) fn resolve_numeric_field_slot(
        target_field: &str,
    ) -> Result<FieldSlot, InternalError> {
        resolve_numeric_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    pub(crate) fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        self.execute_paged_with_cursor(plan, PlannedCursor::none())
            .map(|page| page.items)
    }

    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        self.execute_paged_with_cursor_traced(plan, cursor)
            .map(|(page, _)| page)
    }

    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        if matches!(&plan.as_inner().logical, LogicalPlan::Grouped(_)) {
            return Err(invariant(
                "grouped plans require execute_grouped pagination entrypoints",
            ));
        }

        let cursor: PlannedCursor = plan.revalidate_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_cursor_anchor);

        if !plan.mode().is_load() {
            return Err(invariant("load executor requires load plans"));
        }

        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let route_plan = Self::build_execution_route_plan_for_load(
            plan.as_inner(),
            cursor_boundary.as_ref(),
            index_range_token.as_ref(),
            None,
        )?;
        let continuation_applied = !matches!(
            route_plan.continuation_mode(),
            crate::db::executor::route::ContinuationMode::Initial
        );
        let direction = route_plan.direction();
        debug_assert_eq!(
            route_plan.window().effective_offset,
            ExecutionKernel::effective_page_offset(plan.as_inner(), cursor_boundary.as_ref()),
            "route window effective offset must match logical plan offset semantics",
        );
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let plan = plan.into_inner();
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&plan);

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);

            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;
            let execution_inputs = ExecutionInputs {
                ctx: &ctx,
                plan: &plan,
                stream_bindings: AccessStreamBindings {
                    index_prefix_specs: index_prefix_specs.as_slice(),
                    index_range_specs: index_range_specs.as_slice(),
                    index_range_anchor: index_range_token.as_ref().map(range_token_anchor_key),
                    direction,
                },
                execution_preparation: &execution_preparation,
            };

            record_plan_metrics(&plan.access);
            // Plan execution routing once, then execute in canonical order.
            // Resolve one canonical key stream, then run shared page materialization/finalization.
            let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
                &execution_inputs,
                &route_plan,
                cursor_boundary.as_ref(),
                continuation_signature,
                IndexCompilePolicy::ConservativeSubset,
            )?;
            let page = materialized.page;
            let rows_scanned = materialized.rows_scanned;
            let post_access_rows = materialized.post_access_rows;
            let optimization = materialized.optimization;
            let index_predicate_applied = materialized.index_predicate_applied;
            let index_predicate_keys_rejected = materialized.index_predicate_keys_rejected;
            let distinct_keys_deduped = materialized.distinct_keys_deduped;

            Ok(Self::finalize_execution(
                page,
                optimization,
                rows_scanned,
                post_access_rows,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
                &mut span,
                &mut execution_trace,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }

    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        if !matches!(&plan.as_inner().logical, LogicalPlan::Grouped(_)) {
            return Err(invariant(
                "grouped execution requires grouped logical plans",
            ));
        }

        let cursor = plan.revalidate_grouped_cursor(cursor.into())?;

        self.execute_grouped_path(plan, cursor)
    }

    // Grouped execution spine:
    // 1) resolve grouped route/metadata
    // 2) build grouped key stream
    // 3) execute grouped fold
    // 4) finalize grouped output + observability
    fn execute_grouped_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
        let stream = self.build_grouped_stream(&route)?;
        let folded = Self::execute_group_fold(&route, stream)?;

        Ok(Self::finalize_grouped_output(route, folded))
    }

    // Map route-owned grouped strategy labels into grouped plan-metrics labels.
    pub(in crate::db::executor) const fn grouped_plan_metrics_strategy_for_execution_strategy(
        grouped_execution_strategy: crate::db::executor::route::GroupedExecutionStrategy,
    ) -> GroupedPlanMetricsStrategy {
        match grouped_execution_strategy {
            crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized => {
                GroupedPlanMetricsStrategy::HashMaterialized
            }
            crate::db::executor::route::GroupedExecutionStrategy::OrderedMaterialized => {
                GroupedPlanMetricsStrategy::OrderedMaterialized
            }
        }
    }

    // Resolve grouped handoff/route metadata into one grouped route-stage payload.
    fn resolve_grouped_route(
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
        debug: bool,
    ) -> Result<GroupedRouteStage<E>, InternalError> {
        validate_executor_plan::<E>(plan.as_inner())?;
        let grouped_handoff = grouped_executor_handoff(plan.as_inner())?;
        let grouped_execution = grouped_handoff.execution();
        let group_fields = grouped_handoff.group_fields().to_vec();
        let grouped_aggregates = grouped_handoff.aggregates().to_vec();
        let grouped_having = grouped_handoff.having().cloned();
        let grouped_route_plan =
            Self::build_execution_route_plan_for_grouped_handoff(grouped_handoff);
        let grouped_route_observability =
            grouped_route_plan.grouped_observability().ok_or_else(|| {
                invariant("grouped route planning must emit grouped observability payload")
            })?;
        let grouped_route_outcome = grouped_route_observability.outcome();
        let grouped_route_rejection_reason = grouped_route_observability.rejection_reason();
        let grouped_route_eligible = grouped_route_observability.eligible();
        let grouped_route_execution_mode = grouped_route_observability.execution_mode();
        let grouped_plan_metrics_strategy =
            Self::grouped_plan_metrics_strategy_for_execution_strategy(
                grouped_route_observability.grouped_execution_strategy(),
            );
        debug_assert!(
            grouped_route_eligible == grouped_route_rejection_reason.is_none(),
            "grouped route eligibility and rejection reason must stay aligned",
        );
        debug_assert!(
            grouped_route_outcome
                != crate::db::executor::route::GroupedRouteDecisionOutcome::Rejected
                || grouped_route_rejection_reason.is_some(),
            "grouped rejected outcomes must carry a rejection reason",
        );
        debug_assert!(
            matches!(
                grouped_route_execution_mode,
                crate::db::executor::route::ExecutionMode::Materialized
            ),
            "grouped execution must remain materialized",
        );

        let direction = grouped_route_plan.direction();
        let continuation_applied = !cursor.is_empty();
        let execution_trace =
            debug.then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let global_distinct_field_aggregate = Self::global_distinct_field_aggregate_spec(
            group_fields.as_slice(),
            grouped_aggregates.as_slice(),
            grouped_having.as_ref(),
        )?;
        let plan = plan.into_inner();

        Ok(GroupedRouteStage {
            plan,
            cursor,
            direction,
            continuation_signature,
            index_prefix_specs,
            index_range_specs,
            grouped_execution,
            group_fields,
            grouped_aggregates,
            grouped_having,
            grouped_route_plan,
            grouped_plan_metrics_strategy,
            global_distinct_field_aggregate,
            execution_trace,
        })
    }

    // Build one grouped key stream from route-owned grouped execution metadata.
    fn build_grouped_stream<'a>(
        &'a self,
        route: &'a GroupedRouteStage<E>,
    ) -> Result<GroupedStreamStage<'a, E>, InternalError> {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&route.plan);
        let ctx = self.db.recovered_context::<E>()?;
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &route.plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: route.index_prefix_specs.as_slice(),
                index_range_specs: route.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction: route.direction,
            },
            execution_preparation: &execution_preparation,
        };
        record_grouped_plan_metrics(&route.plan.access, route.grouped_plan_metrics_strategy);
        let resolved = Self::resolve_execution_key_stream_without_distinct(
            &execution_inputs,
            &route.grouped_route_plan,
            IndexCompilePolicy::ConservativeSubset,
        )?;

        Ok(GroupedStreamStage {
            ctx,
            execution_preparation,
            resolved,
        })
    }

    // Execute grouped folding over one resolved grouped key stream.
    #[expect(clippy::too_many_lines)]
    fn execute_group_fold(
        route: &GroupedRouteStage<E>,
        mut stream: GroupedStreamStage<'_, E>,
    ) -> Result<GroupedFoldStage, InternalError> {
        let mut grouped_execution_context =
            grouped_execution_context_from_planner_config(Some(route.grouped_execution));
        let max_groups_bound =
            usize::try_from(grouped_execution_context.config().max_groups()).unwrap_or(usize::MAX);
        let grouped_budget = grouped_budget_observability(&grouped_execution_context);
        debug_assert!(
            grouped_budget.max_groups() >= grouped_budget.groups()
                && grouped_budget.max_group_bytes() >= grouped_budget.estimated_bytes()
                && grouped_execution_context
                    .config()
                    .max_distinct_values_total()
                    >= grouped_budget.distinct_values()
                && grouped_budget.aggregate_states() >= grouped_budget.groups(),
            "grouped budget observability invariants must hold at grouped route entry",
        );

        let (mut grouped_engines, mut short_circuit_keys) =
            if route.global_distinct_field_aggregate.is_none() {
                let grouped_engines = route
                .grouped_aggregates
                .iter()
                .map(|aggregate| {
                    if aggregate.target_field().is_some() {
                        return Err(invariant(format!(
                            "grouped field-target aggregate reached executor after planning: {:?}",
                            aggregate.kind()
                        )));
                    }

                    Ok(grouped_execution_context.create_grouped_engine::<E>(
                        aggregate.kind(),
                        aggregate_materialized_fold_direction(aggregate.kind()),
                        aggregate.distinct(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
                let short_circuit_keys = vec![Vec::<Value>::new(); grouped_engines.len()];

                (grouped_engines, short_circuit_keys)
            } else {
                (Vec::new(), Vec::new())
            };
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        let compiled_predicate = stream.execution_preparation.compiled_predicate();

        if let Some((aggregate_kind, target_field)) = route.global_distinct_field_aggregate.as_ref()
        {
            if let Some(grouped_plan) = route.plan.grouped_plan()
                && let Some(violation) =
                    grouped_cursor_policy_violation(grouped_plan, !route.cursor.is_empty())
            {
                return Err(invariant(violation.invariant_message()));
            }

            let global_row = Self::execute_global_distinct_field_aggregate(
                &route.plan,
                &stream.ctx,
                &mut stream.resolved,
                compiled_predicate,
                &mut grouped_execution_context,
                (*aggregate_kind, target_field.as_str()),
                (&mut scanned_rows, &mut filtered_rows),
            )?;
            let page_rows = Self::page_global_distinct_grouped_row(
                global_row,
                route.plan.scalar_plan().page.as_ref(),
            );
            let rows_scanned = stream
                .resolved
                .rows_scanned_override
                .unwrap_or(scanned_rows);
            let optimization = stream.resolved.optimization;
            let index_predicate_applied = stream.resolved.index_predicate_applied;
            let index_predicate_keys_rejected = stream.resolved.index_predicate_keys_rejected;
            let distinct_keys_deduped = stream
                .resolved
                .distinct_keys_deduped_counter
                .as_ref()
                .map_or(0, |counter| counter.get());

            return Ok(GroupedFoldStage {
                page: GroupedCursorPage {
                    rows: page_rows,
                    next_cursor: None,
                },
                filtered_rows,
                check_filtered_rows_upper_bound: false,
                rows_scanned,
                optimization,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
            });
        }

        // Phase 1: stream key->row reads, decode, predicate filtering, and grouped folding.
        while let Some(key) = stream.resolved.key_stream.next_key()? {
            let row = match route.plan.scalar_plan().consistency {
                MissingRowPolicy::Error => stream.ctx.read_strict(&key),
                MissingRowPolicy::Ignore => stream.ctx.read(&key),
            };
            let row = match row {
                Ok(row) => row,
                Err(err) if err.is_not_found() => continue,
                Err(err) => return Err(err),
            };
            scanned_rows = scanned_rows.saturating_add(1);
            let (id, entity) = Context::<E>::deserialize_row((key, row))?;
            if let Some(compiled_predicate) = compiled_predicate
                && !compiled_predicate.eval(&entity)
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            let group_values = route
                .group_fields
                .iter()
                .map(|field| {
                    entity.get_value_by_index(field.index()).ok_or_else(|| {
                        invariant(format!(
                            "grouped field slot missing on entity: index={}",
                            field.index()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let group_key = Value::List(group_values)
                .canonical_key()
                .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)?;
            let canonical_group_value = group_key.canonical_value().clone();
            let data_key = DataKey::try_new::<E>(id.key())?;

            for (index, engine) in grouped_engines.iter_mut().enumerate() {
                if short_circuit_keys[index].iter().any(|done| {
                    canonical_value_compare(done, &canonical_group_value) == Ordering::Equal
                }) {
                    continue;
                }

                let fold_control = engine
                    .ingest_grouped(group_key.clone(), &data_key, &mut grouped_execution_context)
                    .map_err(Self::map_group_error)?;
                if matches!(fold_control, FoldControl::Break) {
                    short_circuit_keys[index].push(canonical_group_value.clone());
                    debug_assert!(
                        short_circuit_keys[index].len() <= max_groups_bound,
                        "grouped short-circuit key tracking must stay bounded by max_groups",
                    );
                }
            }
        }

        // Phase 2: finalize grouped aggregates per terminal and iterate groups in lock-step.
        //
        // This avoids constructing one additional full grouped `(key, aggregates)` buffer
        // prior to pagination; we page directly while walking finalized grouped outputs.
        let aggregate_count = grouped_engines.len();
        if aggregate_count == 0 {
            return Err(invariant(
                "grouped execution requires at least one aggregate terminal",
            ));
        }
        let mut finalized_iters = grouped_engines
            .into_iter()
            .map(|engine| engine.finalize_grouped().map(Vec::into_iter))
            .collect::<Result<Vec<_>, _>>()?;
        let mut primary_iter = finalized_iters
            .drain(..1)
            .next()
            .ok_or_else(|| invariant("missing grouped primary iterator"))?;

        // Phase 3: apply grouped resume/offset/limit while finalizing grouped outputs.
        let initial_offset = route
            .plan
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| page.offset);
        let resume_initial_offset = if route.cursor.is_empty() {
            initial_offset
        } else {
            route.cursor.initial_offset()
        };
        let resume_boundary = route
            .cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));
        let apply_initial_offset = route.cursor.is_empty();
        let limit = route
            .plan
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .and_then(|limit| usize::try_from(limit).ok());
        let initial_offset_for_page = if apply_initial_offset {
            usize::try_from(initial_offset).unwrap_or(usize::MAX)
        } else {
            0
        };
        let selection_bound = limit.and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });
        let mut grouped_candidate_rows = Vec::<(Value, Vec<Value>)>::new();
        if limit.is_none_or(|limit| limit != 0) {
            for primary_output in primary_iter.by_ref() {
                let group_key_value = primary_output.group_key().canonical_value().clone();
                let mut aggregate_values = Vec::with_capacity(aggregate_count);
                aggregate_values.push(Self::aggregate_output_to_value(primary_output.output()));
                for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                    let sibling_output = sibling_iter.next().ok_or_else(|| {
                        invariant(format!(
                            "grouped finalize alignment missing sibling aggregate row: sibling_index={sibling_index}"
                        ))
                    })?;
                    let sibling_group_key = sibling_output.group_key().canonical_value();
                    if canonical_value_compare(sibling_group_key, &group_key_value)
                        != Ordering::Equal
                    {
                        return Err(invariant(format!(
                            "grouped finalize alignment mismatch at sibling_index={sibling_index}: primary_key={group_key_value:?}, sibling_key={sibling_group_key:?}"
                        )));
                    }
                    aggregate_values.push(Self::aggregate_output_to_value(sibling_output.output()));
                }
                debug_assert_eq!(
                    aggregate_values.len(),
                    aggregate_count,
                    "grouped aggregate value alignment must preserve declared aggregate count",
                );
                if let Some(grouped_having) = route.grouped_having.as_ref()
                    && !Self::group_matches_having(
                        grouped_having,
                        route.group_fields.as_slice(),
                        &group_key_value,
                        aggregate_values.as_slice(),
                    )?
                {
                    continue;
                }

                if let Some(resume_boundary) = resume_boundary.as_ref()
                    && canonical_value_compare(&group_key_value, resume_boundary)
                        != Ordering::Greater
                {
                    continue;
                }

                // Keep only the smallest `offset + limit + 1` canonical grouped keys when
                // paging is bounded so grouped LIMIT does not require one full grouped buffer.
                if let Some(selection_bound) = selection_bound {
                    match grouped_candidate_rows.binary_search_by(|(existing_key, _)| {
                        canonical_value_compare(existing_key, &group_key_value)
                    }) {
                        Ok(_) => {
                            return Err(invariant(format!(
                                "grouped finalize produced duplicate canonical group key: {group_key_value:?}"
                            )));
                        }
                        Err(insert_index) => {
                            grouped_candidate_rows
                                .insert(insert_index, (group_key_value, aggregate_values));
                            if grouped_candidate_rows.len() > selection_bound {
                                let _ = grouped_candidate_rows.pop();
                            }
                            debug_assert!(
                                grouped_candidate_rows.len() <= selection_bound,
                                "bounded grouped candidate rows must stay <= selection_bound",
                            );
                        }
                    }
                } else {
                    grouped_candidate_rows.push((group_key_value, aggregate_values));
                    debug_assert!(
                        grouped_candidate_rows.len() <= max_groups_bound,
                        "grouped candidate rows must stay bounded by max_groups",
                    );
                }
            }
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                if sibling_iter.next().is_some() {
                    return Err(invariant(format!(
                        "grouped finalize alignment has trailing sibling rows: sibling_index={sibling_index}"
                    )));
                }
            }
            if selection_bound.is_none() {
                grouped_candidate_rows
                    .sort_by(|(left, _), (right, _)| canonical_value_compare(left, right));
            }
        }
        if let Some(selection_bound) = selection_bound {
            debug_assert!(
                grouped_candidate_rows.len() <= selection_bound,
                "grouped candidate rows must remain bounded by selection_bound",
            );
        } else {
            debug_assert!(
                grouped_candidate_rows.len() <= max_groups_bound,
                "grouped candidate rows must remain bounded by max_groups",
            );
        }

        let mut page_rows = Vec::<GroupedRow>::new();
        let mut last_emitted_group_key: Option<Vec<Value>> = None;
        let mut has_more = false;
        let mut groups_skipped_for_offset = 0usize;
        for (group_key_value, aggregate_values) in grouped_candidate_rows {
            if groups_skipped_for_offset < initial_offset_for_page {
                groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
                continue;
            }
            if let Some(limit) = limit
                && page_rows.len() >= limit
            {
                has_more = true;
                break;
            }

            let emitted_group_key = match group_key_value {
                Value::List(values) => values,
                value => {
                    return Err(invariant(format!(
                        "grouped canonical key must be Value::List, found {value:?}"
                    )));
                }
            };
            last_emitted_group_key = Some(emitted_group_key.clone());
            page_rows.push(GroupedRow::new(emitted_group_key, aggregate_values));
            debug_assert!(
                limit.is_none_or(|bounded_limit| page_rows.len() <= bounded_limit),
                "grouped page rows must not exceed explicit page limit",
            );
        }

        let next_cursor = if has_more {
            last_emitted_group_key.map(|last_group_key| {
                PageCursor::Grouped(GroupedContinuationToken::new_with_direction(
                    route.continuation_signature,
                    last_group_key,
                    Direction::Asc,
                    resume_initial_offset,
                ))
            })
        } else {
            None
        };
        let rows_scanned = stream
            .resolved
            .rows_scanned_override
            .unwrap_or(scanned_rows);
        let optimization = stream.resolved.optimization;
        let index_predicate_applied = stream.resolved.index_predicate_applied;
        let index_predicate_keys_rejected = stream.resolved.index_predicate_keys_rejected;
        let distinct_keys_deduped = stream
            .resolved
            .distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get());

        Ok(GroupedFoldStage {
            page: GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            filtered_rows,
            check_filtered_rows_upper_bound: true,
            rows_scanned,
            optimization,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        })
    }

    // Finalize grouped output payloads and observability after grouped fold execution.
    fn finalize_grouped_output(
        mut route: GroupedRouteStage<E>,
        folded: GroupedFoldStage,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        let rows_returned = folded.page.rows.len();
        Self::finalize_path_outcome(
            &mut route.execution_trace,
            folded.optimization,
            folded.rows_scanned,
            rows_returned,
            folded.index_predicate_applied,
            folded.index_predicate_keys_rejected,
            folded.distinct_keys_deduped,
        );

        let mut span = Span::<E>::new(ExecKind::Load);
        span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
        if folded.check_filtered_rows_upper_bound {
            debug_assert!(
                folded.filtered_rows >= rows_returned,
                "grouped pagination must return at most filtered row cardinality",
            );
        }

        (folded.page, route.execution_trace)
    }

    // Map grouped reducer errors into executor-owned error classes.
    fn map_group_error(err: GroupError) -> InternalError {
        match err {
            GroupError::MemoryLimitExceeded { .. } | GroupError::DistinctBudgetExceeded { .. } => {
                InternalError::executor_internal(err.to_string())
            }
            GroupError::Internal(inner) => inner,
        }
    }

    // Resolve whether this grouped shape is the supported global DISTINCT
    // field-target aggregate contract (`COUNT` or `SUM` with zero group keys).
    fn global_distinct_field_aggregate_spec(
        group_fields: &[crate::db::query::plan::FieldSlot],
        aggregates: &[GroupAggregateSpec],
        having: Option<&GroupHavingSpec>,
    ) -> Result<Option<(AggregateKind, String)>, InternalError> {
        match resolve_global_distinct_field_aggregate(group_fields, aggregates, having) {
            Ok(Some(aggregate)) => Ok(Some((
                aggregate.kind(),
                aggregate.target_field().to_string(),
            ))),
            Ok(None) => Ok(None),
            Err(reason) => {
                let aggregate = aggregates.first().ok_or_else(|| {
                    invariant("global DISTINCT candidate invariants require at least one aggregate")
                })?;
                Err(Self::group_distinct_policy_invariant(reason, aggregate))
            }
        }
    }

    // Build one canonical invariant error from grouped DISTINCT policy contract reasons.
    fn group_distinct_policy_invariant(
        reason: GroupDistinctPolicyReason,
        aggregate: &GroupAggregateSpec,
    ) -> InternalError {
        match reason {
            GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind => invariant(
                format!("{}: {:?}", reason.invariant_message(), aggregate.kind()),
            ),
            GroupDistinctPolicyReason::DistinctHavingUnsupported
            | GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired
            | GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
            | GroupDistinctPolicyReason::GlobalDistinctRequiresSingleAggregate
            | GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate
            | GroupDistinctPolicyReason::GlobalDistinctRequiresDistinctAggregateTerminal => {
                invariant(reason.invariant_message())
            }
        }
    }

    // Execute one global DISTINCT field-target grouped aggregate with grouped
    // distinct budget accounting and deterministic reducer behavior.
    fn execute_global_distinct_field_aggregate(
        plan: &AccessPlannedQuery<E::Key>,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut crate::db::executor::aggregate::ExecutionContext,
        aggregate_spec: (AggregateKind, &str),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (aggregate_kind, target_field) = aggregate_spec;
        let (scanned_rows, filtered_rows) = row_counters;
        let field_slot = if aggregate_kind.is_sum() {
            Self::resolve_numeric_field_slot(target_field)?
        } else {
            Self::resolve_any_field_slot(target_field)?
        };
        let mut distinct_values = GroupKeySet::new();
        let mut count = 0u32;
        let mut sum = Decimal::ZERO;
        let mut saw_sum_value = false;

        grouped_execution_context
            .record_implicit_single_group::<E>()
            .map_err(Self::map_group_error)?;

        while let Some(key) = resolved.key_stream.next_key()? {
            let row = match plan.scalar_plan().consistency {
                MissingRowPolicy::Error => ctx.read_strict(&key),
                MissingRowPolicy::Ignore => ctx.read(&key),
            };
            let row = match row {
                Ok(row) => row,
                Err(err) if err.is_not_found() => continue,
                Err(err) => return Err(err),
            };
            *scanned_rows = scanned_rows.saturating_add(1);
            let (_, entity) = Context::<E>::deserialize_row((key, row))?;
            if let Some(compiled_predicate) = compiled_predicate
                && !compiled_predicate.eval(&entity)
            {
                continue;
            }
            *filtered_rows = filtered_rows.saturating_add(1);

            let distinct_value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let distinct_key = distinct_value
                .canonical_key()
                .map_err(KeyCanonicalError::into_internal_error)?;
            let distinct_admitted = grouped_execution_context
                .admit_distinct_key(
                    &mut distinct_values,
                    grouped_execution_context
                        .config()
                        .max_distinct_values_per_group(),
                    distinct_key,
                )
                .map_err(Self::map_group_error)?;
            if !distinct_admitted {
                continue;
            }

            if aggregate_kind.is_sum() {
                let numeric_value =
                    extract_numeric_field_decimal(&entity, target_field, field_slot)
                        .map_err(AggregateFieldValueError::into_internal_error)?;
                sum += numeric_value;
                saw_sum_value = true;
            } else {
                count = count.saturating_add(1);
            }
        }

        let aggregate_value = if aggregate_kind.is_sum() {
            if saw_sum_value {
                Value::Decimal(sum)
            } else {
                Value::Null
            }
        } else {
            Value::Uint(u64::from(count))
        };

        Ok(GroupedRow::new(Vec::new(), vec![aggregate_value]))
    }

    // Apply grouped pagination semantics to the singleton global grouped row.
    fn page_global_distinct_grouped_row(
        row: GroupedRow,
        page: Option<&crate::db::query::plan::PageSpec>,
    ) -> Vec<GroupedRow> {
        let Some(page) = page else {
            return vec![row];
        };
        if page.offset > 0 || page.limit == Some(0) {
            return Vec::new();
        }

        vec![row]
    }

    // Convert one aggregate output payload into grouped response value payload.
    fn aggregate_output_to_value(output: &AggregateOutput<E>) -> Value {
        match output {
            AggregateOutput::Count(value) => Value::Uint(u64::from(*value)),
            AggregateOutput::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            AggregateOutput::Exists(value) => Value::Bool(*value),
            AggregateOutput::Min(value)
            | AggregateOutput::Max(value)
            | AggregateOutput::First(value)
            | AggregateOutput::Last(value) => value.map_or(Value::Null, Value::from),
        }
    }

    // Evaluate grouped HAVING clauses on one finalized grouped output row.
    fn group_matches_having(
        having: &GroupHavingSpec,
        group_fields: &[crate::db::query::plan::FieldSlot],
        group_key_value: &Value,
        aggregate_values: &[Value],
    ) -> Result<bool, InternalError> {
        for (index, clause) in having.clauses().iter().enumerate() {
            let actual = match clause.symbol() {
                GroupHavingSymbol::GroupField(field_slot) => {
                    let group_key_list = match group_key_value {
                        Value::List(values) => values,
                        value => {
                            return Err(invariant(format!(
                                "grouped HAVING requires list-shaped grouped keys, found {value:?}"
                            )));
                        }
                    };
                    let Some(group_field_offset) = group_fields
                        .iter()
                        .position(|group_field| group_field.index() == field_slot.index())
                    else {
                        return Err(invariant(format!(
                            "grouped HAVING field is not in grouped key projection: field='{}'",
                            field_slot.field()
                        )));
                    };
                    group_key_list.get(group_field_offset).ok_or_else(|| {
                        invariant(format!(
                            "grouped HAVING group key offset out of bounds: clause_index={index}, offset={group_field_offset}, key_len={}",
                            group_key_list.len()
                        ))
                    })?
                }
                GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                    aggregate_values.get(*aggregate_index).ok_or_else(|| {
                        invariant(format!(
                            "grouped HAVING aggregate index out of bounds: clause_index={index}, aggregate_index={aggregate_index}, aggregate_count={}",
                            aggregate_values.len()
                        ))
                    })?
                }
            };

            if !Self::having_compare_values(actual, clause.op(), clause.value())? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    // Evaluate one grouped HAVING compare operator using strict value semantics.
    fn having_compare_values(
        actual: &Value,
        op: CompareOp,
        expected: &Value,
    ) -> Result<bool, InternalError> {
        let Some(matches) = evaluate_grouped_having_compare_v1(actual, op, expected) else {
            return Err(invariant(format!(
                "unsupported grouped HAVING operator reached executor: {op:?}",
            )));
        };

        Ok(matches)
    }

    // Record shared observability outcome for any execution path.
    fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        rows_returned: usize,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) {
        record_rows_scanned::<E>(rows_scanned);
        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(
                optimization,
                rows_scanned,
                rows_returned,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
            );
        }
    }

    // Preserve PK fast-path cursor-boundary error classification at the executor boundary.
    pub(in crate::db::executor) fn validate_pk_fast_path_boundary_if_applicable(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if !Self::pk_order_stream_fast_path_shape_supported(plan) {
            return Ok(());
        }
        let _ = decode_pk_cursor_boundary::<E>(cursor_boundary)?;

        Ok(())
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
