use crate::{
    db::{
        executor::pipeline::operators::post_access::window,
        query::plan::{DeleteLimitSpec, OrderSpec, QueryMode},
    },
    error::InternalError,
};

// Apply delete row cap after ordering for delete-mode plans.
pub(in crate::db::executor::pipeline::operators::post_access) fn apply_delete_limit_phase<R>(
    mode: QueryMode,
    order_spec: Option<&OrderSpec>,
    delete_limit_spec: Option<&DeleteLimitSpec>,
    rows: &mut Vec<R>,
    ordered: bool,
) -> Result<(bool, usize), InternalError> {
    let delete_was_limited = if mode.is_delete()
        && let Some(limit) = delete_limit_spec
    {
        if order_spec.is_some() && !ordered {
            return Err(crate::db::error::query_executor_invariant(
                "delete limit must run after ordering",
            ));
        }
        window::apply_delete_limit(rows, limit.max_rows);
        true
    } else {
        false
    };

    Ok((delete_was_limited, rows.len()))
}
