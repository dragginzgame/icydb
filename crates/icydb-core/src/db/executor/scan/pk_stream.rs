//! Module: executor::scan::pk_stream
//! Responsibility: primary-key order fast-path stream execution helpers.
//! Does not own: planner route precedence or post-access materialization semantics.
//! Boundary: validates PK-stream-compatible access shapes then emits ordered key streams.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            pipeline::contracts::{FastPathKeyResult, LoadExecutor},
            scan::{FastStreamRouteKind, FastStreamRouteRequest},
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Try one primary-key order fast path and return ordered keys when eligible.
    pub(in crate::db::executor) fn try_execute_pk_order_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        Self::execute_fast_stream_route(
            ctx,
            FastStreamRouteKind::PrimaryKey,
            FastStreamRouteRequest::PrimaryKey {
                plan,
                stream_direction,
                probe_fetch_hint,
            },
        )
    }
}
