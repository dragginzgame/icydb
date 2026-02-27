use crate::{
    db::{
        Context,
        access::AccessPath,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings,
            load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
            route::supports_pk_stream_access_path,
        },
        plan::{AccessPlannedQuery, derive_primary_scan_direction},
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
        let stream_direction = derive_primary_scan_direction(plan.order.as_ref());

        // Phase 2: lower through the canonical access-stream resolver boundary.
        let stream_request = AccessPlanStreamRequest::from_bindings(
            &plan.access,
            AccessStreamBindings::no_index(stream_direction),
            probe_fetch_hint,
            None,
        );
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
}
