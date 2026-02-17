use crate::{
    db::{
        Context,
        executor::load::{FastLoadResult, LoadExecutor},
        query::plan::{
            AccessPath, ContinuationSignature, CursorBoundary, LogicalPlan,
            validate::PushdownApplicability,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Fast path for secondary-index traversal when planner pushdown eligibility
    // proves canonical ORDER BY parity with raw index-key order.
    pub(super) fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        secondary_pushdown_applicability: &PushdownApplicability,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        if !secondary_pushdown_applicability.is_eligible() {
            return Ok(None);
        }

        let Some(AccessPath::IndexPrefix { index, values }) = plan.access.as_path() else {
            return Ok(None);
        };

        // Phase 1: resolve candidate keys using canonical index traversal order.
        let ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values::<E>(index, values.as_slice())
                })
            })
        })?;
        let rows_scanned = ordered_keys.len();

        // Phase 2: load rows while preserving traversal order.
        let data_rows = ctx.rows_from_ordered_data_keys(&ordered_keys, plan.consistency)?;
        let mut rows = Context::deserialize_rows(data_rows)?;

        // Phase 3: apply canonical post-access semantics (predicate/cursor/page) and continuation.
        let page = Self::finalize_rows_into_page(
            plan,
            &mut rows,
            cursor_boundary,
            continuation_signature,
        )?;

        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned,
        }))
    }
}
