use crate::{
    db::{
        Context,
        executor::load::{
            ExecutionFastPath, ExecutionPushdownType, FastPathKeyResult, LoadExecutor,
        },
        index::RawIndexKey,
        query::plan::{Direction, LogicalPlan},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Limited IndexRange pushdown for semantically safe plan shapes.
    pub(super) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        effective_fetch: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        let Some(effective_fetch) = effective_fetch else {
            return Ok(None);
        };

        let Some((index, prefix, lower, upper)) = plan.access.as_index_range_path() else {
            return Ok(None);
        };

        // Phase 1: resolve candidate keys via bounded range traversal with early termination.
        let ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values_in_range_limited::<E>(
                        index,
                        prefix,
                        (lower, upper),
                        index_range_anchor,
                        direction,
                        effective_fetch,
                    )
                })
            })
        })?;
        let rows_scanned = ordered_keys.len();

        Ok(Some(FastPathKeyResult {
            ordered_keys,
            rows_scanned,
            fast_path_used: ExecutionFastPath::IndexRange,
            pushdown_type: Some(ExecutionPushdownType::IndexRangeLimit),
        }))
    }

    pub(super) fn is_index_range_limit_pushdown_shape_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        let Some((index, prefix, _, _)) = plan.access.as_index_range_path() else {
            return false;
        };
        let index_fields = index.fields;
        let prefix_len = prefix.len();
        if plan.predicate.is_some() {
            return false;
        }

        if let Some(order) = plan.order.as_ref()
            && !order.fields.is_empty()
        {
            let Some(expected_direction) = order.fields.last().map(|(_, direction)| *direction)
            else {
                return false;
            };
            if order
                .fields
                .iter()
                .any(|(_, direction)| *direction != expected_direction)
            {
                return false;
            }

            let mut expected =
                Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
            expected.extend(index_fields.iter().skip(prefix_len).copied());
            expected.push(E::MODEL.primary_key.name);
            if order.fields.len() != expected.len() {
                return false;
            }
            if !order
                .fields
                .iter()
                .map(|(field, _)| field.as_str())
                .eq(expected)
            {
                return false;
            }
        }

        true
    }
}
