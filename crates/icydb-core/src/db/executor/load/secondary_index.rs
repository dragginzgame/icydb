use crate::{
    db::{
        Context,
        executor::load::{
            ExecutionFastPath, ExecutionPushdownType, FastPathKeyResult, LoadExecutor,
        },
        query::plan::{Direction, LogicalPlan, OrderDirection, validate::PushdownApplicability},
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
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        if !secondary_pushdown_applicability.is_eligible() {
            return Ok(None);
        }

        let Some((index, values)) = plan.access.as_index_prefix_path() else {
            return Ok(None);
        };

        // Phase 1: resolve candidate keys using canonical index traversal order.
        let mut ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| index_store.resolve_data_values::<E>(index, values))
            })
        })?;
        if matches!(
            Self::secondary_index_stream_direction(plan),
            Direction::Desc
        ) {
            ordered_keys.reverse();
        }
        let rows_scanned = ordered_keys.len();

        Ok(Some(FastPathKeyResult {
            ordered_keys,
            rows_scanned,
            fast_path_used: ExecutionFastPath::SecondaryIndex,
            pushdown_type: Some(ExecutionPushdownType::SecondaryOrder),
        }))
    }

    fn secondary_index_stream_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        let Some(order) = plan.order.as_ref() else {
            return Direction::Asc;
        };

        match order.fields.last().map(|(_, direction)| direction) {
            Some(OrderDirection::Desc) => Direction::Desc,
            _ => Direction::Asc,
        }
    }
}
