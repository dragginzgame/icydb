use crate::{
    db::{
        Db,
        executor::ExecutorError,
        primitives::FilterExpr,
        query::{QueryPlan, QueryPlanner},
        store::{DataKey, RawDataKey},
    },
    error::{ErrorOrigin, InternalError},
    obs::sink::{self, MetricsEvent, PlanKind, Span},
    traits::EntityKind,
};
use std::ops::{Bound, ControlFlow};

/// Plan a query for an entity given an optional filter.
#[must_use]
pub fn plan_for<E: EntityKind>(filter: Option<&FilterExpr>) -> QueryPlan {
    QueryPlanner::new(filter).plan::<E>()
}

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
/// Planning remains side-effect free.
pub fn record_plan_metrics(plan: &QueryPlan) {
    let kind = match plan {
        QueryPlan::Keys(_) => PlanKind::Keys,
        QueryPlan::Index(_) => PlanKind::Index,
        QueryPlan::Range(_, _) => PlanKind::Range,
        QueryPlan::FullScan => PlanKind::FullScan,
    };

    sink::record(MetricsEvent::Plan { kind });
}

///
/// ReadMode
/// Read behavior policy for scan operations
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadMode {
    /// Skip missing or malformed rows.
    BestEffort,

    /// Skip missing rows, but treat decode failures as corruption.
    ///
    /// This keeps idempotent reads tolerant of absence while still surfacing
    /// malformed persisted bytes as corruption.
    MissingOk,

    /// Treat missing or malformed rows as corruption.
    Strict,
}

/// Convenience: set span rows from a usize length.
pub const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}

/// Iterate a query plan and deserialize rows, delegating row handling to `on_row`.
///
/// In `ReadMode::Strict`, missing rows or deserialization failures are treated as
/// corruption and returned as errors. In `ReadMode::BestEffort`, such rows are
/// silently skipped. In `ReadMode::MissingOk`, missing rows are skipped while
/// decode failures still surface as corruption.
#[allow(clippy::too_many_lines)]
pub fn scan_plan<E, F>(
    db: &Db<E::Canister>,
    plan: QueryPlan,
    mode: ReadMode,
    mut on_row: F,
) -> Result<(), InternalError>
where
    E: EntityKind,
    F: FnMut(DataKey, E) -> ControlFlow<()>,
{
    let ctx = db.context::<E>();

    match plan {
        QueryPlan::Keys(keys) => match mode {
            ReadMode::MissingOk => {
                for dk in keys.into_iter().map(DataKey::new::<E>) {
                    let row = match ctx.read(&dk) {
                        Ok(row) => row,
                        Err(err) if err.is_not_found() => continue,
                        Err(err) => return Err(err),
                    };

                    let Ok(entity) = row.try_decode::<E>() else {
                        return Err(bad_row(&dk));
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }
            }
            _ => {
                ctx.with_store(|s| {
                    for dk in keys.into_iter().map(DataKey::new::<E>) {
                        let raw = dk.to_raw();
                        let Some(row) = s.get(&raw) else {
                            if mode == ReadMode::Strict {
                                return Err(missing_row(&dk));
                            }
                            continue;
                        };

                        let entity = match row.try_decode::<E>() {
                            Ok(entity) => entity,
                            Err(_) if mode == ReadMode::BestEffort => continue,
                            Err(_) => return Err(bad_row(&dk)),
                        };

                        if on_row(dk, entity) == ControlFlow::Break(()) {
                            break;
                        }
                    }

                    Ok::<_, InternalError>(())
                })??;
            }
        },

        QueryPlan::Index(index_plan) => {
            let keys = ctx.candidates_from_plan(QueryPlan::Index(index_plan))?;

            match mode {
                ReadMode::MissingOk => {
                    for dk in keys {
                        let row = match ctx.read(&dk) {
                            Ok(row) => row,
                            Err(err) if err.is_not_found() => continue,
                            Err(err) => return Err(err),
                        };

                        let Ok(entity) = row.try_decode::<E>() else {
                            return Err(bad_row(&dk));
                        };

                        if on_row(dk, entity) == ControlFlow::Break(()) {
                            break;
                        }
                    }
                }
                _ => {
                    ctx.with_store(|s| {
                        for dk in keys {
                            let raw = dk.to_raw();
                            let Some(row) = s.get(&raw) else {
                                if mode == ReadMode::Strict {
                                    return Err(missing_row(&dk));
                                }
                                continue;
                            };

                            let entity = match row.try_decode::<E>() {
                                Ok(entity) => entity,
                                Err(_) if mode == ReadMode::BestEffort => continue,
                                Err(_) => return Err(bad_row(&dk)),
                            };

                            if on_row(dk, entity) == ControlFlow::Break(()) {
                                break;
                            }
                        }

                        Ok::<_, InternalError>(())
                    })??;
                }
            }
        }

        QueryPlan::Range(start, end) => {
            let start_key = DataKey::new::<E>(start);
            let end_key = DataKey::new::<E>(end);
            let start_raw = start_key.to_raw();
            let end_raw = end_key.to_raw();

            ctx.with_store(|s| {
                for entry in s.range((Bound::Included(start_raw), Bound::Included(end_raw))) {
                    let dk = decode_data_key(entry.key())?;

                    let row = entry.value();
                    let entity = match row.try_decode::<E>() {
                        Ok(entity) => entity,
                        Err(_) if mode == ReadMode::BestEffort => continue,
                        Err(_) => return Err(bad_row(&dk)),
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, InternalError>(())
            })??;
        }

        QueryPlan::FullScan => {
            let start = DataKey::lower_bound::<E>();
            let end = DataKey::upper_bound::<E>();
            let start_raw = start.to_raw();
            let end_raw = end.to_raw();

            ctx.with_store(|s| {
                for entry in s.range((Bound::Included(start_raw), Bound::Included(end_raw))) {
                    let dk = decode_data_key(entry.key())?;

                    let row = entry.value();
                    let entity = match row.try_decode::<E>() {
                        Ok(entity) => entity,
                        Err(_) if mode == ReadMode::BestEffort => continue,
                        Err(_) => return Err(bad_row(&dk)),
                    };

                    if on_row(dk, entity) == ControlFlow::Break(()) {
                        break;
                    }
                }

                Ok::<_, InternalError>(())
            })??;
        }
    }

    Ok(())
}

/// Strict scan that surfaces missing rows or deserialization failures as corruption.
pub fn scan_strict<E, F>(
    db: &Db<E::Canister>,
    plan: QueryPlan,
    on_row: F,
) -> Result<(), InternalError>
where
    E: EntityKind,
    F: FnMut(DataKey, E) -> ControlFlow<()>,
{
    scan_plan::<E, F>(db, plan, ReadMode::Strict, on_row)
}

/// Best-effort scan that skips missing or malformed rows.
#[expect(dead_code)]
pub fn scan_best_effort<E, F>(
    db: &Db<E::Canister>,
    plan: QueryPlan,
    on_row: F,
) -> Result<(), InternalError>
where
    E: EntityKind,
    F: FnMut(DataKey, E) -> ControlFlow<()>,
{
    scan_plan::<E, F>(db, plan, ReadMode::BestEffort, on_row)
}

/// Missing-ok scan that skips absent rows but treats decode failures as corruption.
pub fn scan_missing_ok<E, F>(
    db: &Db<E::Canister>,
    plan: QueryPlan,
    on_row: F,
) -> Result<(), InternalError>
where
    E: EntityKind,
    F: FnMut(DataKey, E) -> ControlFlow<()>,
{
    scan_plan::<E, F>(db, plan, ReadMode::MissingOk, on_row)
}

#[inline]
/// Missing row in a strict scan is an invariant violation, not a benign NotFound.
fn missing_row(dk: &DataKey) -> InternalError {
    ExecutorError::corruption(ErrorOrigin::Store, format!("missing row: {dk}")).into()
}

#[inline]
fn bad_row(dk: &DataKey) -> InternalError {
    ExecutorError::corruption(
        ErrorOrigin::Serialize,
        format!("failed to deserialize row: {dk}"),
    )
    .into()
}

#[inline]
fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
    DataKey::try_from_raw(raw)
        .map_err(|msg| ExecutorError::corruption(ErrorOrigin::Store, msg).into())
}
