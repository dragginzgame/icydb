//! Module: executor::load
//! Responsibility: load-path execution orchestration, pagination, and trace contracts.
//! Does not own: logical planning semantics or relation/commit mutation policy.
//! Boundary: consumes executable load plans and delegates post-access semantics to kernel.

mod execute;
mod fast_stream;
mod index_range_limit;
mod page;
mod pk_stream;
mod secondary_index;
mod terminal;
mod trace;

pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
};

use self::trace::{access_path_variant, execution_order_direction};
use crate::{
    db::{
        Context, Db, GroupedRow,
        access::AccessPlan,
        contracts::canonical_value_compare,
        cursor::{
            ContinuationToken, CursorBoundary, GroupedContinuationToken, GroupedPlannedCursor,
            PlannedCursor, decode_pk_cursor_boundary,
        },
        data::DataKey,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            KeyOrderComparator, OrderedKeyStreamBox,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot, resolve_orderable_aggregate_target_slot,
            },
            aggregate::{AggregateOutput, FoldControl, GroupError},
            group::{
                CanonicalKey, grouped_budget_observability,
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
        predicate::{CoercionSpec, CompareOp, MissingRowPolicy, compare_eq, compare_order},
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupHavingSymbol, LogicalPlan, OrderDirection,
            grouped_executor_handoff,
        },
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
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

///
/// ExecutionAccessPathVariant
///
/// Coarse access path shape used by the load execution trace surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAccessPathVariant {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexRange,
    FullScan,
    Union,
    Intersection,
}

///
/// ExecutionOptimization
///
/// Canonical load optimization selected by execution, if any.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOptimization {
    PrimaryKey,
    SecondaryOrderPushdown,
    IndexRangeLimitPushdown,
}

///
/// ExecutionTrace
///
/// Structured, opt-in load execution introspection snapshot.
/// Captures plan-shape and execution decisions without changing semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub access_path_variant: ExecutionAccessPathVariant,
    pub direction: OrderDirection,
    pub optimization: Option<ExecutionOptimization>,
    pub keys_scanned: u64,
    pub rows_returned: u64,
    pub continuation_applied: bool,
    pub index_predicate_applied: bool,
    pub index_predicate_keys_rejected: u64,
    pub distinct_keys_deduped: u64,
}

impl ExecutionTrace {
    fn new<K>(access: &AccessPlan<K>, direction: Direction, continuation_applied: bool) -> Self {
        Self {
            access_path_variant: access_path_variant(access),
            direction: execution_order_direction(direction),
            optimization: None,
            keys_scanned: 0,
            rows_returned: 0,
            continuation_applied,
            index_predicate_applied: false,
            index_predicate_keys_rejected: 0,
            distinct_keys_deduped: 0,
        }
    }

    fn set_path_outcome(
        &mut self,
        optimization: Option<ExecutionOptimization>,
        keys_scanned: usize,
        rows_returned: usize,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) {
        self.optimization = optimization;
        self.keys_scanned = u64::try_from(keys_scanned).unwrap_or(u64::MAX);
        self.rows_returned = u64::try_from(rows_returned).unwrap_or(u64::MAX);
        self.index_predicate_applied = index_predicate_applied;
        self.index_predicate_keys_rejected = index_predicate_keys_rejected;
        self.distinct_keys_deduped = distinct_keys_deduped;
    }
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
            return Err(InternalError::query_executor_invariant(
                "grouped plans require execute_grouped pagination entrypoints",
            ));
        }

        let cursor: PlannedCursor = plan.revalidate_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_cursor_anchor);

        if !plan.mode().is_load() {
            return Err(InternalError::query_executor_invariant(
                "load executor requires load plans",
            ));
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
            return Err(InternalError::query_executor_invariant(
                "grouped execution requires grouped logical plans",
            ));
        }

        let cursor = plan.revalidate_grouped_cursor(cursor.into())?;

        self.execute_grouped_path(plan, cursor)
    }

    // Execute grouped blocking reduction and produce grouped page rows + grouped cursor.
    #[expect(clippy::too_many_lines)]
    fn execute_grouped_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        validate_executor_plan::<E>(plan.as_inner())?;
        let grouped_handoff = grouped_executor_handoff(plan.as_inner())?;
        let grouped_execution = grouped_handoff.execution();
        let group_fields = grouped_handoff.group_fields().to_vec();
        let grouped_having = grouped_handoff.having().cloned();
        let grouped_route_plan =
            Self::build_execution_route_plan_for_grouped_handoff(grouped_handoff);
        let grouped_route_observability =
            grouped_route_plan.grouped_observability().ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "grouped route planning must emit grouped observability payload",
                )
            })?;
        let direction = grouped_route_plan.direction();
        let continuation_applied = !cursor.is_empty();
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        let mut grouped_execution_context =
            grouped_execution_context_from_planner_config(Some(grouped_execution));
        let grouped_budget = grouped_budget_observability(&grouped_execution_context);
        debug_assert!(
            grouped_budget.max_groups() >= grouped_budget.groups()
                && grouped_budget.max_group_bytes() >= grouped_budget.estimated_bytes()
                && grouped_execution_context
                    .config()
                    .max_distinct_values_total()
                    >= grouped_budget.distinct_values()
                && grouped_budget.aggregate_states() >= grouped_budget.groups(),
            "grouped budget observability invariants must hold at grouped route entry"
        );

        // Observe grouped route outcome/rejection once at grouped runtime entry.
        let grouped_route_outcome = grouped_route_observability.outcome();
        let grouped_route_rejection_reason = grouped_route_observability.rejection_reason();
        let grouped_route_eligible = grouped_route_observability.eligible();
        let grouped_route_execution_mode = grouped_route_observability.execution_mode();
        let grouped_plan_metrics_strategy =
            match grouped_route_observability.grouped_execution_strategy() {
                crate::db::executor::route::GroupedExecutionStrategy::HashGroup => {
                    GroupedPlanMetricsStrategy::HashMaterialized
                }
                crate::db::executor::route::GroupedExecutionStrategy::OrderedGroup => {
                    GroupedPlanMetricsStrategy::OrderedStreaming
                }
            };
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
            "grouped execution route must remain blocking/materialized",
        );
        let mut grouped_engines = grouped_handoff
            .aggregates()
            .iter()
            .map(|aggregate| {
                if aggregate.target_field().is_some() {
                    return Err(InternalError::query_executor_invariant(format!(
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
        let mut short_circuit_keys = vec![Vec::<Value>::new(); grouped_engines.len()];
        let plan = plan.into_inner();
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&plan);

        let mut span = Span::<E>::new(ExecKind::Load);
        let ctx = self.db.recovered_context::<E>()?;
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: index_prefix_specs.as_slice(),
                index_range_specs: index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            execution_preparation: &execution_preparation,
        };
        record_grouped_plan_metrics(&plan.access, grouped_plan_metrics_strategy);
        let mut resolved = Self::resolve_execution_key_stream_without_distinct(
            &execution_inputs,
            &grouped_route_plan,
            IndexCompilePolicy::ConservativeSubset,
        )?;
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        let compiled_predicate = execution_preparation.compiled_predicate();

        // Phase 1: stream key->row reads, decode, predicate filtering, and grouped folding.
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
            scanned_rows = scanned_rows.saturating_add(1);
            let (id, entity) = Context::<E>::deserialize_row((key, row))?;
            if let Some(compiled_predicate) = compiled_predicate
                && !compiled_predicate.eval(&entity)
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            let group_values = group_fields
                .iter()
                .map(|field| {
                    entity.get_value_by_index(field.index()).ok_or_else(|| {
                        InternalError::query_executor_invariant(format!(
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
                }
            }
        }

        // Phase 2: finalize grouped aggregates per terminal and iterate groups in lock-step.
        //
        // This avoids constructing one additional full grouped `(key, aggregates)` buffer
        // prior to pagination; we page directly while walking finalized grouped outputs.
        let aggregate_count = grouped_engines.len();
        if aggregate_count == 0 {
            return Err(InternalError::query_executor_invariant(
                "grouped execution requires at least one aggregate terminal",
            ));
        }
        let mut finalized_iters = grouped_engines
            .into_iter()
            .map(|engine| engine.finalize_grouped().map(Vec::into_iter))
            .collect::<Result<Vec<_>, _>>()?;
        let mut primary_iter = finalized_iters.drain(..1).next().ok_or_else(|| {
            InternalError::query_executor_invariant("missing grouped primary iterator")
        })?;

        // Phase 3: apply grouped resume/offset/limit while finalizing grouped outputs.
        let initial_offset = plan
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| page.offset);
        let resume_initial_offset = if cursor.is_empty() {
            initial_offset
        } else {
            cursor.initial_offset()
        };
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));
        let apply_initial_offset = cursor.is_empty();
        let limit = plan
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
                        InternalError::query_executor_invariant(format!(
                            "grouped finalize alignment missing sibling aggregate row: sibling_index={sibling_index}"
                        ))
                    })?;
                    let sibling_group_key = sibling_output.group_key().canonical_value();
                    if canonical_value_compare(sibling_group_key, &group_key_value)
                        != Ordering::Equal
                    {
                        return Err(InternalError::query_executor_invariant(format!(
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
                if let Some(grouped_having) = grouped_having.as_ref()
                    && !Self::group_matches_having(
                        grouped_having,
                        group_fields.as_slice(),
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
                            return Err(InternalError::query_executor_invariant(format!(
                                "grouped finalize produced duplicate canonical group key: {group_key_value:?}"
                            )));
                        }
                        Err(insert_index) => {
                            grouped_candidate_rows
                                .insert(insert_index, (group_key_value, aggregate_values));
                            if grouped_candidate_rows.len() > selection_bound {
                                let _ = grouped_candidate_rows.pop();
                            }
                        }
                    }
                } else {
                    grouped_candidate_rows.push((group_key_value, aggregate_values));
                }
            }
            for (sibling_index, sibling_iter) in finalized_iters.iter_mut().enumerate() {
                if sibling_iter.next().is_some() {
                    return Err(InternalError::query_executor_invariant(format!(
                        "grouped finalize alignment has trailing sibling rows: sibling_index={sibling_index}"
                    )));
                }
            }
            if selection_bound.is_none() {
                grouped_candidate_rows
                    .sort_by(|(left, _), (right, _)| canonical_value_compare(left, right));
            }
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
                    return Err(InternalError::query_executor_invariant(format!(
                        "grouped canonical key must be Value::List, found {value:?}"
                    )));
                }
            };
            last_emitted_group_key = Some(emitted_group_key.clone());
            page_rows.push(GroupedRow::new(emitted_group_key, aggregate_values));
        }

        let next_cursor = if has_more {
            last_emitted_group_key.map(|last_group_key| {
                PageCursor::Grouped(GroupedContinuationToken::new_with_direction(
                    continuation_signature,
                    last_group_key,
                    Direction::Asc,
                    resume_initial_offset,
                ))
            })
        } else {
            None
        };
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(scanned_rows);
        let optimization = resolved.optimization;
        let index_predicate_applied = resolved.index_predicate_applied;
        let index_predicate_keys_rejected = resolved.index_predicate_keys_rejected;
        let distinct_keys_deduped = resolved
            .distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get());
        let rows_returned = page_rows.len();

        Self::finalize_path_outcome(
            &mut execution_trace,
            optimization,
            rows_scanned,
            rows_returned,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        );
        span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
        debug_assert!(
            filtered_rows >= rows_returned,
            "grouped pagination must return at most filtered row cardinality",
        );

        Ok((
            GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            execution_trace,
        ))
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

    // Convert one aggregate output payload into grouped response value payload.
    fn aggregate_output_to_value(output: &AggregateOutput<E>) -> Value {
        match output {
            AggregateOutput::Count(value) => Value::Uint(u64::from(*value)),
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
                            return Err(InternalError::query_executor_invariant(format!(
                                "grouped HAVING requires list-shaped grouped keys, found {value:?}"
                            )));
                        }
                    };
                    let Some(group_field_offset) = group_fields
                        .iter()
                        .position(|group_field| group_field.index() == field_slot.index())
                    else {
                        return Err(InternalError::query_executor_invariant(format!(
                            "grouped HAVING field is not in grouped key projection: field='{}'",
                            field_slot.field()
                        )));
                    };
                    group_key_list.get(group_field_offset).ok_or_else(|| {
                        InternalError::query_executor_invariant(format!(
                            "grouped HAVING group key offset out of bounds: clause_index={index}, offset={group_field_offset}, key_len={}",
                            group_key_list.len()
                        ))
                    })?
                }
                GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                    aggregate_values.get(*aggregate_index).ok_or_else(|| {
                        InternalError::query_executor_invariant(format!(
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
        let strict = CoercionSpec::default();
        let matches = match op {
            CompareOp::Eq => compare_eq(actual, expected, &strict).unwrap_or(false),
            CompareOp::Ne => compare_eq(actual, expected, &strict).is_some_and(|equal| !equal),
            CompareOp::Lt => compare_order(actual, expected, &strict).is_some_and(Ordering::is_lt),
            CompareOp::Lte => compare_order(actual, expected, &strict).is_some_and(Ordering::is_le),
            CompareOp::Gt => compare_order(actual, expected, &strict).is_some_and(Ordering::is_gt),
            CompareOp::Gte => compare_order(actual, expected, &strict).is_some_and(Ordering::is_ge),
            CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => {
                return Err(InternalError::query_executor_invariant(format!(
                    "unsupported grouped HAVING operator reached executor: {op:?}"
                )));
            }
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
            debug_assert_eq!(
                execution_trace.keys_scanned,
                u64::try_from(rows_scanned).unwrap_or(u64::MAX),
                "execution trace keys_scanned must match rows_scanned metrics input",
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
