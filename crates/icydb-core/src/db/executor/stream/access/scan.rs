//! Module: executor::stream::access::scan
//! Responsibility: low-level primary/index scan adapters over store/index handles.
//! Does not own: access routing decisions or planner spec construction.
//! Boundary: direct scan primitives used by access-stream resolver.

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::LoweredKey,
        executor::{Context, LoweredIndexPrefixSpec, LoweredIndexRangeSpec},
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
};
use std::ops::Bound;

///
/// PrimaryScan
///
/// Executor-owned adapter for primary data-store iteration.
/// The physical stream resolver must request scans through this boundary instead of
/// traversing store handles directly.
///

pub(in crate::db::executor) struct PrimaryScan;

impl PrimaryScan {
    /// Resolve one inclusive primary-key range into data keys.
    pub(in crate::db::executor) fn range<E>(
        ctx: &Context<'_, E>,
        start: &DataKey,
        end: &DataKey,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match limit {
            Some(limit) => Self::range_limited::<E>(ctx, start, end, direction, limit),
            None => Self::range_unbounded::<E>(ctx, start, end),
        }
    }

    // Resolve one bounded range probe with direction-aware early stop.
    fn range_limited<E>(
        ctx: &Context<'_, E>,
        start: &DataKey,
        end: &DataKey,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let keys = ctx.with_store(|store| -> Result<Vec<DataKey>, InternalError> {
            let mut out = Vec::new();
            if limit == 0 {
                return Ok(out);
            }

            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;

            match direction {
                Direction::Asc => {
                    for entry in store.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    {
                        out.push(Context::<E>::decode_data_key(entry.key())?);
                        if out.len() == limit {
                            break;
                        }
                    }
                }
                Direction::Desc => {
                    for entry in store
                        .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                        .rev()
                    {
                        out.push(Context::<E>::decode_data_key(entry.key())?);
                        if out.len() == limit {
                            break;
                        }
                    }
                }
            }

            Ok(out)
        })??;

        Ok(keys)
    }

    // Resolve one unbounded range scan in canonical ascending storage order.
    fn range_unbounded<E>(
        ctx: &Context<'_, E>,
        start: &DataKey,
        end: &DataKey,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let keys = ctx.with_store(|store| {
            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;

            store
                .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .map(|entry| Context::<E>::decode_data_key(entry.key()))
                .collect::<Result<Vec<_>, _>>()
        })??;

        Ok(keys)
    }
}

///
/// IndexScan
///
/// Executor-owned adapter for secondary-index iteration.
/// The physical stream resolver must request index traversal via this adapter so routing
/// stays decoupled from store-registry/index-handle internals.
///

pub(in crate::db::executor) struct IndexScan;

impl IndexScan {
    /// Resolve one lowered index-prefix envelope into data keys.
    pub(in crate::db::executor) fn prefix<E>(
        ctx: &Context<'_, E>,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        Self::resolve_limited::<E>(
            ctx,
            spec.index().store,
            spec.index(),
            spec.lower(),
            spec.upper(),
            None,
            direction,
            limit,
            predicate_execution,
        )
    }

    /// Resolve one lowered index-range envelope (plus optional anchor) into data keys.
    pub(in crate::db::executor) fn range<E>(
        ctx: &Context<'_, E>,
        spec: &LoweredIndexRangeSpec,
        anchor: Option<&LoweredKey>,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        Self::resolve_limited::<E>(
            ctx,
            spec.index().store,
            spec.index(),
            spec.lower(),
            spec.upper(),
            anchor,
            direction,
            limit,
            predicate_execution,
        )
    }

    // Resolve one index range via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_limited<E>(
        ctx: &Context<'_, E>,
        store_path: &'static str,
        index: &IndexModel,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        anchor: Option<&LoweredKey>,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let store = ctx
            .db
            .with_store_registry(|registry| registry.try_get_store(store_path))?;
        let keys = store.with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited::<E>(
                index,
                (lower, upper),
                anchor,
                direction,
                limit,
                predicate_execution,
            )
        })?;

        Ok(keys)
    }
}
