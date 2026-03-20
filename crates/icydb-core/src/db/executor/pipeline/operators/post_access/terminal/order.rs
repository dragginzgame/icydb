use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, OrderReadableRow,
            pipeline::operators::post_access::order_cursor::{
                apply_order_spec as apply_post_access_order_spec,
                apply_order_spec_bounded as apply_post_access_order_spec_bounded,
            },
            route::access_order_satisfied_by_route_contract_for_model,
        },
        query::plan::{AccessPlannedQuery, OrderSpec},
    },
    error::InternalError,
    traits::EntityKind,
};

// Return whether the resolved access stream already satisfies ORDER BY semantics.
fn order_satisfied_by_access_path(
    model: &crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
) -> bool {
    access_order_satisfied_by_route_contract_for_model(model, plan)
}

// Apply ordering with bounded first-page optimization when available.
pub(in crate::db::executor::pipeline::operators::post_access) fn apply_order_phase<E, R, K>(
    plan: &AccessPlannedQuery,
    order_spec: Option<&OrderSpec>,
    has_predicate: bool,
    rows: &mut Vec<R>,
    cursor: Option<&CursorBoundary>,
    filtered: bool,
) -> Result<(bool, usize), InternalError>
where
    E: EntityKind<Key = K>,
    R: OrderReadableRow,
{
    let bounded_order_keep = ExecutionKernel::bounded_order_keep_count(plan, cursor);
    if let Some(order) = order_spec
        && !order.fields.is_empty()
    {
        if has_predicate && !filtered {
            return Err(crate::db::error::query_executor_invariant(
                "ordering must run after filtering",
            ));
        }

        // If access traversal already satisfies requested ORDER BY
        // semantics, preserve stream order and skip in-memory sorting.
        if order_satisfied_by_access_path(E::MODEL, plan) {
            return Ok((true, rows.len()));
        }

        let ordered_total = rows.len();
        if rows.len() > 1 {
            if let Some(keep_count) = bounded_order_keep {
                apply_post_access_order_spec_bounded(rows, E::MODEL, order, keep_count);
            } else {
                apply_post_access_order_spec(rows, E::MODEL, order);
            }
        }

        // Keep logical post-order cardinality even when bounded ordering
        // trims the working set for load-page execution.
        return Ok((true, ordered_total));
    }

    Ok((false, rows.len()))
}
