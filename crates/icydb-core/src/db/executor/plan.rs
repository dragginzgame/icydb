use crate::{
    Error,
    db::{
        Db,
        primitives::FilterExpr,
        query::{QueryPlan, QueryPlanner},
        store::DataKey,
    },
    deserialize,
    obs::metrics::Span,
    traits::EntityKind,
};
use std::ops::{Bound, ControlFlow};

/// Plan a query for an entity given an optional filter.
#[must_use]
pub fn plan_for<E: EntityKind>(filter: Option<&FilterExpr>) -> QueryPlan {
    QueryPlanner::new(filter).plan::<E>()
}

/// Convenience: set span rows from a usize length.
pub const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}

/// Iterate a query plan and deserialize rows, delegating row handling to `on_row`.
pub fn scan_plan<E, F>(db: &Db<E::Canister>, plan: QueryPlan, mut on_row: F) -> Result<(), Error>
where
    E: EntityKind,
    F: FnMut(DataKey, E) -> ControlFlow<()>,
{
    let ctx = db.context::<E>();

    match plan {
        QueryPlan::Keys(keys) => {
            let data_keys: Vec<DataKey> = keys.into_iter().map(DataKey::new::<E>).collect();

            ctx.with_store(|s| {
                for dk in data_keys {
                    let Some(bytes) = s.get(&dk) else {
                        continue;
                    };

                    let Ok(entity) = deserialize::<E>(&bytes) else {
                        continue;
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, Error>(())
            })??;
        }

        QueryPlan::Index(index_plan) => {
            let keys = ctx.candidates_from_plan(QueryPlan::Index(index_plan))?;

            ctx.with_store(|s| {
                for dk in keys {
                    let Some(bytes) = s.get(&dk) else {
                        continue;
                    };

                    let Ok(entity) = deserialize::<E>(&bytes) else {
                        continue;
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, Error>(())
            })??;
        }

        QueryPlan::Range(start, end) => {
            let start_key = DataKey::new::<E>(start);
            let end_key = DataKey::new::<E>(end);

            ctx.with_store(|s| {
                for entry in s.range((Bound::Included(start_key), Bound::Included(end_key))) {
                    let dk = entry.key().clone();
                    let Ok(entity) = deserialize::<E>(&entry.value()) else {
                        continue;
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, Error>(())
            })??;
        }

        QueryPlan::FullScan => {
            let start = DataKey::lower_bound::<E>();
            let end = DataKey::upper_bound::<E>();

            ctx.with_store(|s| {
                for entry in s.range((Bound::Included(start.clone()), Bound::Included(end))) {
                    let dk = entry.key().clone();
                    let Ok(entity) = deserialize::<E>(&entry.value()) else {
                        continue;
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, Error>(())
            })??;
        }
    }

    Ok(())
}
