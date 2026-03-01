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
            aggregate::{
                AggregateOutput, FoldControl, GroupError,
                ensure_grouped_spec_supported_for_execution,
            },
            group::{
                CanonicalKey, grouped_budget_observability,
                grouped_execution_context_from_planner_config,
            },
            plan_metrics::{record_plan_metrics, record_rows_scanned},
            range_token_anchor_key, range_token_from_cursor_anchor, validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::{
            plan::{AccessPlannedQuery, LogicalPlan, OrderDirection, grouped_executor_handoff},
            policy,
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
        debug_assert!(
            policy::validate_plan_shape(&plan.as_inner().logical).is_ok(),
            "load executor received a plan shape that bypassed planning validation",
        );

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

        ensure_grouped_spec_supported_for_execution(
            grouped_handoff.group_fields(),
            grouped_handoff.aggregates(),
        )
        .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;

        let mut grouped_execution_context =
            grouped_execution_context_from_planner_config(Some(grouped_execution));
        let grouped_budget = grouped_budget_observability(&grouped_execution_context);
        debug_assert!(
            grouped_budget.max_groups() >= grouped_budget.groups()
                && grouped_budget.max_group_bytes() >= grouped_budget.estimated_bytes()
                && grouped_budget.aggregate_states() >= grouped_budget.groups(),
            "grouped budget observability invariants must hold at grouped route entry"
        );

        // Observe grouped route outcome/rejection once at grouped runtime entry.
        let grouped_route_outcome = grouped_route_observability.outcome();
        let grouped_route_rejection_reason = grouped_route_observability.rejection_reason();
        let grouped_route_eligible = grouped_route_observability.eligible();
        let grouped_route_execution_mode = grouped_route_observability.execution_mode();
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
                    aggregate.kind().materialized_fold_direction(),
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
        record_plan_metrics(&plan.access);
        let mut resolved = Self::resolve_execution_key_stream_without_distinct(
            &execution_inputs,
            &grouped_route_plan,
            IndexCompilePolicy::ConservativeSubset,
        )?;
        let data_rows = ctx.rows_from_ordered_key_stream(
            resolved.key_stream.as_mut(),
            plan.scalar_plan().consistency,
        )?;
        let scanned_rows = data_rows.len();
        let mut rows = Context::<E>::deserialize_rows(data_rows)?;
        if let Some(compiled_predicate) = execution_preparation.compiled_predicate() {
            rows.retain(|row| compiled_predicate.eval(&row.1));
        }
        let filtered_rows = rows.len();

        // Phase 1: fold every filtered row into per-group aggregate states.
        for (id, entity) in &rows {
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

        // Phase 2: finalize grouped aggregate states and align outputs by declared aggregate order.
        let aggregate_count = grouped_engines.len();
        let mut grouped_rows_by_key = Vec::<(Value, Vec<Value>)>::new();
        for (index, engine) in grouped_engines.into_iter().enumerate() {
            let finalized = engine.finalize_grouped()?;
            for output in finalized {
                let group_key = output.group_key().canonical_value().clone();
                let aggregate_value = Self::aggregate_output_to_value(output.output());
                if let Some((_, existing_aggregates)) =
                    grouped_rows_by_key
                        .iter_mut()
                        .find(|(existing_group_key, _)| {
                            canonical_value_compare(existing_group_key, &group_key)
                                == Ordering::Equal
                        })
                {
                    if let Some(slot) = existing_aggregates.get_mut(index) {
                        *slot = aggregate_value;
                    }
                } else {
                    let mut aggregates = vec![Value::Null; aggregate_count];
                    if let Some(slot) = aggregates.get_mut(index) {
                        *slot = aggregate_value;
                    }
                    grouped_rows_by_key.push((group_key, aggregates));
                }
            }
        }
        grouped_rows_by_key.sort_by(|(left, _), (right, _)| canonical_value_compare(left, right));

        // Phase 3: apply grouped resume/offset/limit and build grouped continuation token.
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
        let mut groups_skipped_for_offset = 0u32;
        let limit = plan
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .and_then(|limit| usize::try_from(limit).ok());
        let mut page_rows = Vec::<GroupedRow>::new();
        let mut last_emitted_group_key: Option<Vec<Value>> = None;
        let mut has_more = false;
        for (group_key_value, aggregate_values) in grouped_rows_by_key {
            if let Some(resume_boundary) = resume_boundary.as_ref()
                && canonical_value_compare(&group_key_value, resume_boundary) != Ordering::Greater
            {
                continue;
            }
            if apply_initial_offset && groups_skipped_for_offset < initial_offset {
                groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
                continue;
            }
            if limit.is_some_and(|limit| limit == 0) {
                break;
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
            GroupError::MemoryLimitExceeded { .. } => {
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
