use crate::traits::{EntityKind, View};

///
/// PagedResponse
///
/// Public cursor-pagination payload for load queries.
/// The `next_cursor` token is opaque and must be treated as an uninterpreted string.
/// Ordering is deterministic for each request under the query's canonical order.
/// Continuation is best-effort and forward-only over live state.
/// Pagination is not snapshot-isolated: no snapshot/version is pinned across requests, so
/// concurrent writes may shift boundaries.
///

pub struct PagedResponse<E: EntityKind> {
    pub items: Vec<View<E>>,
    pub next_cursor: Option<String>,
}
