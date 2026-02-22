mod aggregate;
mod aggregate_guard;
mod execute;
mod index_range_limit;
mod page;
mod pk_stream;
mod route;
mod secondary_index;
mod trace;

use self::{
    execute::ExecutionInputs,
    trace::{access_path_variant, execution_order_direction},
};
use crate::{
    db::{
        Db,
        executor::{
            AccessStreamBindings, KeyOrderComparator, OrderedKeyStreamBox,
            plan::{record_plan_metrics, record_rows_scanned},
        },
        query::plan::{
            AccessPlan, CursorBoundary, Direction, ExecutablePlan, LogicalPlan, OrderDirection,
            PlannedCursor, SlotSelectionPolicy, decode_pk_cursor_boundary, derive_scan_direction,
            validate::validate_executor_plan,
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
        }
    }

    fn set_path_outcome(
        &mut self,
        optimization: Option<ExecutionOptimization>,
        keys_scanned: usize,
        rows_returned: usize,
    ) {
        self.optimization = optimization;
        self.keys_scanned = u64::try_from(keys_scanned).unwrap_or(u64::MAX);
        self.rows_returned = u64::try_from(rows_returned).unwrap_or(u64::MAX);
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
/// IndexRangeLimitSpec
///
/// Canonical executor decision payload for index-range limit pushdown.
/// Encodes the bounded fetch size after all eligibility gates pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexRangeLimitSpec {
    fetch: usize,
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
        let cursor: PlannedCursor = plan.revalidate_planned_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_anchor = cursor.index_range_anchor().cloned();

        if !plan.mode().is_load() {
            return Err(InternalError::query_executor_invariant(
                "load executor requires load plans",
            ));
        }

        let direction = plan.direction();
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let continuation_applied = cursor_boundary.is_some() || index_range_anchor.is_some();
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            let plan = plan.into_inner();

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
            let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, &route_plan)?;
            let (page, keys_scanned, post_access_rows) = Self::materialize_key_stream_into_page(
                &ctx,
                &plan,
                resolved.key_stream.as_mut(),
                route_plan.scan_hints.load_scan_budget_hint,
                cursor_boundary.as_ref(),
                direction,
                continuation_signature,
            )?;
            let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);

            Ok(Self::finalize_execution(
                page,
                resolved.optimization,
                rows_scanned,
                post_access_rows,
                &mut span,
                &mut execution_trace,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }

    // Record shared observability outcome for any execution path.
    fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        rows_returned: usize,
    ) {
        record_rows_scanned::<E>(rows_scanned);
        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(optimization, rows_scanned, rows_returned);
            debug_assert_eq!(
                execution_trace.keys_scanned,
                u64::try_from(rows_scanned).unwrap_or(u64::MAX),
                "execution trace keys_scanned must match rows_scanned metrics input",
            );
        }
    }

    // Preserve PK fast-path cursor-boundary error classification at the executor boundary.
    fn validate_pk_fast_path_boundary_if_applicable(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if !Self::is_pk_order_stream_eligible(plan) {
            return Ok(());
        }
        let _ = decode_pk_cursor_boundary::<E>(cursor_boundary)?;

        Ok(())
    }
}
