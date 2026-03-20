use crate::traits::EntityKind;

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
    items: Vec<E>,
    next_cursor: Option<String>,
}

impl<E: EntityKind> PagedResponse<E> {
    pub(crate) const fn new(items: Vec<E>, next_cursor: Option<String>) -> Self {
        Self { items, next_cursor }
    }

    #[must_use]
    pub fn items(&self) -> &[E] {
        &self.items
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    #[must_use]
    pub fn into_items(self) -> Vec<E> {
        self.items
    }

    #[must_use]
    pub fn into_next_cursor(self) -> Option<String> {
        self.next_cursor
    }
}

///
/// PagedGroupedResponse
///
/// Public grouped pagination payload.
/// Grouped rows are returned as core grouped rows to preserve grouped value fidelity.
///
pub struct PagedGroupedResponse {
    items: Vec<icydb_core::db::GroupedRow>,
    next_cursor: Option<String>,
    execution_trace: Option<icydb_core::db::ExecutionTrace>,
}

impl PagedGroupedResponse {
    pub(crate) const fn new(
        items: Vec<icydb_core::db::GroupedRow>,
        next_cursor: Option<String>,
        execution_trace: Option<icydb_core::db::ExecutionTrace>,
    ) -> Self {
        Self {
            items,
            next_cursor,
            execution_trace,
        }
    }

    #[must_use]
    pub fn items(&self) -> &[icydb_core::db::GroupedRow] {
        &self.items
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    #[must_use]
    pub const fn execution_trace(&self) -> Option<icydb_core::db::ExecutionTrace> {
        self.execution_trace
    }

    #[must_use]
    pub fn into_items(self) -> Vec<icydb_core::db::GroupedRow> {
        self.items
    }

    #[must_use]
    pub fn into_next_cursor(self) -> Option<String> {
        self.next_cursor
    }

    #[must_use]
    pub fn into_execution_trace(self) -> Option<icydb_core::db::ExecutionTrace> {
        self.execution_trace
    }
}
