//! Module: db::response::paged
//!
//! Responsibility: public database response payloads.
//! Does not own: query execution, storage mutation, or core response construction.
//! Boundary: adapts core response shapes to facade-facing Candid-friendly types.

use crate::{
    db::response::{ExecutionTrace, GroupedRow},
    traits::EntityKind,
};
use icydb_core::db::ReadIntentKind;

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
    read_intent: ReadIntentKind,
}

impl<E: EntityKind> PagedResponse<E> {
    pub(crate) const fn new(
        items: Vec<E>,
        next_cursor: Option<String>,
        read_intent: ReadIntentKind,
    ) -> Self {
        Self {
            items,
            next_cursor,
            read_intent,
        }
    }

    #[must_use]
    pub fn items(&self) -> &[E] {
        &self.items
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    /// Return diagnostic read-intent metadata for this paged response.
    ///
    /// This is reporting metadata only. It does not configure admission,
    /// planning, cursor encoding, or execution semantics.
    #[must_use]
    pub const fn read_intent(&self) -> ReadIntentKind {
        self.read_intent
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
/// Grouped rows stay in the core grouped-row representation so grouped value
/// fidelity and execution tracing remain intact at the facade boundary.
///

#[derive(Debug)]
pub struct PagedGroupedResponse {
    items: Vec<GroupedRow>,
    next_cursor: Option<String>,
    execution_trace: Option<ExecutionTrace>,
}

impl PagedGroupedResponse {
    pub(crate) const fn new(
        items: Vec<GroupedRow>,
        next_cursor: Option<String>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            items,
            next_cursor,
            execution_trace,
        }
    }

    #[must_use]
    pub fn items(&self) -> &[GroupedRow] {
        &self.items
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    #[must_use]
    pub const fn execution_trace(&self) -> Option<ExecutionTrace> {
        self.execution_trace
    }

    #[must_use]
    pub fn into_items(self) -> Vec<GroupedRow> {
        self.items
    }

    #[must_use]
    pub fn into_next_cursor(self) -> Option<String> {
        self.next_cursor
    }

    #[must_use]
    pub fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_trace
    }
}
