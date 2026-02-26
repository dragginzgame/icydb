use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, KeyOrderComparator,
            load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
            route::{RouteOrderSlotPolicy, derive_scan_direction, supports_pk_stream_access_path},
        },
        query::plan::{AccessPath, AccessPlannedQuery},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Fast path for canonical primary-key ordering over full scans.
    // Produces ordered keys only; shared row materialization happens in load/mod.rs.
    pub(super) fn try_execute_pk_order_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: validate that the routed access shape is PK-stream compatible.
        Self::pk_fast_path_access(plan)?;
        let stream_direction = Self::pk_stream_direction(plan);

        // Phase 2: lower through the canonical access-stream resolver boundary.
        let stream_request = AccessPlanStreamRequest {
            access: &plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: &[],
                index_range_specs: &[],
                index_range_anchor: None,
                direction: stream_direction,
            },
            key_comparator: KeyOrderComparator::from_direction(stream_direction),
            physical_fetch_hint: probe_fetch_hint,
            index_predicate_execution: None,
        };
        Ok(Some(Self::execute_fast_stream_request(
            ctx,
            stream_request,
            ExecutionOptimization::PrimaryKey,
        )?))
    }

    // Validate routed access-path shape for PK stream fast-path execution.
    fn pk_fast_path_access(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> Result<&AccessPath<E::Key>, InternalError> {
        let access = plan.access.as_path().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "pk stream fast-path requires direct access-path execution",
            )
        })?;
        if !supports_pk_stream_access_path(access) {
            return Err(InternalError::query_executor_invariant(
                "pk stream fast-path requires full-scan/key-range access path",
            ));
        }

        Ok(access)
    }

    fn pk_stream_direction(plan: &AccessPlannedQuery<E::Key>) -> Direction {
        plan.order.as_ref().map_or(Direction::Asc, |order| {
            derive_scan_direction(order, RouteOrderSlotPolicy::First)
        })
    }
}
