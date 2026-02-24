mod aggregate;
mod execute;
mod fast_stream;
mod index_range_limit;
mod page;
mod pk_stream;
mod secondary_index;
mod terminal;
mod trace;

use self::{
    execute::{ExecutionInputs, IndexPredicateCompileMode},
    trace::{access_path_variant, execution_order_direction},
};
use crate::{
    db::{
        Db,
        executor::{
            AccessStreamBindings, KeyOrderComparator, OrderedKeyStreamBox,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot, resolve_orderable_aggregate_target_slot,
            },
            plan::{record_plan_metrics, record_rows_scanned},
            route::ExecutionRoutePlan,
        },
        query::policy,
        query::{
            contracts::cursor::CursorBoundary,
            cursor::continuation::decode_pk_cursor_boundary,
            plan::{
                AccessPlan, Direction, ExecutablePlan, LogicalPlan, OrderDirection, PlannedCursor,
                SlotSelectionPolicy, compute_page_window, derive_scan_direction,
                validate::validate_executor_plan,
            },
        },
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;

///
/// CursorPage
///
/// Internal load page result with continuation cursor payload.
/// Returned by paged executor entrypoints.
///

#[derive(Debug)]
pub(crate) struct CursorPage<E: EntityKind> {
    pub(crate) items: Response<E>,

    pub(crate) next_cursor: Option<Vec<u8>>,
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

fn key_stream_comparator_from_plan<K>(
    plan: &LogicalPlan<K>,
    fallback_direction: Direction,
) -> KeyOrderComparator {
    let derived_direction = plan.order.as_ref().map_or(fallback_direction, |order| {
        derive_scan_direction(order, SlotSelectionPolicy::Last)
    });

    // Comparator and child-stream monotonicity must stay aligned until access-path
    // stream production can emit keys under an order-spec-derived comparator.
    let comparator_direction = if derived_direction == fallback_direction {
        derived_direction
    } else {
        fallback_direction
    };

    KeyOrderComparator::from_direction(comparator_direction)
}

///
/// FastPathKeyResult
///
/// Internal fast-path access result.
/// Carries ordered keys plus observability metadata for shared execution phases.
///

struct FastPathKeyResult {
    ordered_key_stream: OrderedKeyStreamBox,
    rows_scanned: usize,
    optimization: ExecutionOptimization,
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
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    // Resolve one orderable aggregate target field into a stable slot with
    // canonical field-error taxonomy mapping.
    fn resolve_orderable_field_slot(target_field: &str) -> Result<FieldSlot, InternalError> {
        resolve_orderable_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Resolve one aggregate target field into a stable slot with canonical
    // field-error taxonomy mapping.
    fn resolve_any_field_slot(target_field: &str) -> Result<FieldSlot, InternalError> {
        resolve_any_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Resolve one numeric aggregate target field into a stable slot with
    // canonical field-error taxonomy mapping.
    fn resolve_numeric_field_slot(target_field: &str) -> Result<FieldSlot, InternalError> {
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

    #[expect(clippy::too_many_lines)]
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let cursor: PlannedCursor = plan.revalidate_planned_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_anchor = cursor.index_range_anchor().cloned();

        if !plan.mode().is_load() {
            return Err(InternalError::query_executor_invariant(
                "load executor requires load plans",
            ));
        }
        debug_assert!(
            policy::validate_plan_shape(plan.as_inner()).is_ok(),
            "load executor received a plan shape that bypassed planning validation",
        );

        let direction = plan.direction();
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let continuation_applied = cursor_boundary.is_some() || index_range_anchor.is_some();
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let (plan, predicate_slots) = plan.into_parts();

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
                    index_range_anchor: index_range_anchor.as_ref(),
                    direction,
                },
                predicate_slots: predicate_slots.as_ref(),
            };

            record_plan_metrics(&plan.access);
            // Plan execution routing once, then execute in canonical order.
            let route_plan = Self::build_execution_route_plan_for_load(
                &plan,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
                None,
                direction,
            )?;

            // Resolve one canonical key stream, then run shared page materialization/finalization.
            let mut resolved = Self::resolve_execution_key_stream(
                &execution_inputs,
                &route_plan,
                IndexPredicateCompileMode::ConservativeSubset,
            )?;
            let (mut page, keys_scanned, mut post_access_rows) =
                Self::materialize_key_stream_into_page(
                    &ctx,
                    &plan,
                    predicate_slots.as_ref(),
                    resolved.key_stream.as_mut(),
                    route_plan.scan_hints.load_scan_budget_hint,
                    route_plan.streaming_access_shape_safe(),
                    cursor_boundary.as_ref(),
                    direction,
                    continuation_signature,
                )?;
            let mut rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
            let mut optimization = resolved.optimization;
            let mut index_predicate_applied = resolved.index_predicate_applied;
            let mut index_predicate_keys_rejected = resolved.index_predicate_keys_rejected;
            let mut distinct_keys_deduped = resolved
                .distinct_keys_deduped_counter
                .as_ref()
                .map_or(0, |counter| counter.get());

            if Self::index_range_limited_residual_retry_required(
                &plan,
                cursor_boundary.as_ref(),
                &route_plan,
                rows_scanned,
                post_access_rows,
            ) {
                let mut fallback_route_plan = route_plan;
                fallback_route_plan.index_range_limit_spec = None;
                let mut fallback_resolved = Self::resolve_execution_key_stream(
                    &execution_inputs,
                    &fallback_route_plan,
                    IndexPredicateCompileMode::ConservativeSubset,
                )?;
                let (fallback_page, fallback_keys_scanned, fallback_post_access_rows) =
                    Self::materialize_key_stream_into_page(
                        &ctx,
                        &plan,
                        predicate_slots.as_ref(),
                        fallback_resolved.key_stream.as_mut(),
                        fallback_route_plan.scan_hints.load_scan_budget_hint,
                        fallback_route_plan.streaming_access_shape_safe(),
                        cursor_boundary.as_ref(),
                        direction,
                        continuation_signature,
                    )?;
                let fallback_rows_scanned = fallback_resolved
                    .rows_scanned_override
                    .unwrap_or(fallback_keys_scanned);
                let fallback_distinct_keys_deduped = fallback_resolved
                    .distinct_keys_deduped_counter
                    .as_ref()
                    .map_or(0, |counter| counter.get());

                // Retry accounting keeps observability faithful to actual work.
                rows_scanned = rows_scanned.saturating_add(fallback_rows_scanned);
                optimization = fallback_resolved.optimization;
                index_predicate_applied =
                    index_predicate_applied || fallback_resolved.index_predicate_applied;
                index_predicate_keys_rejected = index_predicate_keys_rejected
                    .saturating_add(fallback_resolved.index_predicate_keys_rejected);
                distinct_keys_deduped =
                    distinct_keys_deduped.saturating_add(fallback_distinct_keys_deduped);
                page = fallback_page;
                post_access_rows = fallback_post_access_rows;
            }

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

    // Retry index-range limit pushdown when a bounded residual-filter pass may
    // have under-filled the requested page window.
    fn index_range_limited_residual_retry_required(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        route_plan: &ExecutionRoutePlan,
        rows_scanned: usize,
        post_access_rows: usize,
    ) -> bool {
        let Some(limit_spec) = route_plan.index_range_limit_spec else {
            return false;
        };
        if plan.predicate.is_none() {
            return false;
        }
        if limit_spec.fetch == 0 {
            return false;
        }
        let Some(limit) = plan.page.as_ref().and_then(|page| page.limit) else {
            return false;
        };
        let keep_count =
            compute_page_window(plan.effective_page_offset(cursor_boundary), limit, false)
                .keep_count;
        if keep_count == 0 {
            return false;
        }
        if rows_scanned < limit_spec.fetch {
            return false;
        }

        post_access_rows < keep_count
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
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if !Self::pk_order_stream_fast_path_shape_supported(plan) {
            return Ok(());
        }
        let _ = decode_pk_cursor_boundary::<E>(cursor_boundary)?;

        Ok(())
    }
}
