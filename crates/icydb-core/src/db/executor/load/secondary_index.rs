use crate::{
    db::{
        Context,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{VecOrderedKeyStream, normalize_ordered_keys},
        query::plan::{
            Direction, LogicalPlan, SlotSelectionPolicy, derive_scan_direction,
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
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        if !secondary_pushdown_applicability.is_eligible() {
            return Ok(None);
        }

        let Some((index, values)) = plan.access.as_index_prefix_path() else {
            return Ok(None);
        };
        let stream_direction = Self::secondary_index_stream_direction(plan);

        // Phase 1: resolve candidate keys using canonical index traversal order.
        // When a probe hint is present (EXISTS), use a bounded resolver so
        // candidate production can short-circuit earlier.
        let mut ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| match probe_fetch_hint {
                    Some(fetch) => index_store.resolve_data_values_limited::<E>(
                        index,
                        values,
                        stream_direction,
                        fetch,
                    ),
                    None => index_store.resolve_data_values::<E>(index, values),
                })
            })
        })?;

        // The bounded resolver already returns keys in requested order.
        if probe_fetch_hint.is_none() {
            normalize_ordered_keys(&mut ordered_keys, stream_direction, true);
        }
        let rows_scanned = ordered_keys.len();

        Ok(Some(FastPathKeyResult {
            ordered_key_stream: Box::new(VecOrderedKeyStream::new(ordered_keys)),
            rows_scanned,
            optimization: ExecutionOptimization::SecondaryOrderPushdown,
        }))
    }

    fn secondary_index_stream_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        plan.order.as_ref().map_or(Direction::Asc, |order| {
            derive_scan_direction(order, SlotSelectionPolicy::Last)
        })
    }
}
