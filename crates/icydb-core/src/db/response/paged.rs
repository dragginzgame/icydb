//! Module: response::paged
//! Responsibility: paged load response payload contracts.
//! Does not own: query execution, pagination planning, or cursor token protocol.
//! Boundary: response DTOs returned by session/query execution APIs.

use crate::{
    db::{
        diagnostics::{ExecutionMetrics, ExecutionTrace},
        query::read_intent::ReadIntentKind,
        response::{EntityResponse, Row},
    },
    entity::EntityKind,
};

///
/// PagedLoadExecution
///
/// Cursor-paged load response with optional continuation cursor bytes.
///

#[derive(Debug)]
pub struct PagedLoadExecution<E: EntityKind> {
    response: EntityResponse<E>,
    continuation_cursor: Option<Vec<u8>>,
    read_intent: ReadIntentKind,
}

impl<E: EntityKind> PagedLoadExecution<E> {
    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &EntityResponse<E> {
        &self.response
    }

    /// Borrow an iterator over paged rows in response order.
    pub fn iter(&self) -> std::slice::Iter<'_, Row<E>> {
        self.response.iter()
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Return diagnostic read-intent metadata for this paged execution.
    ///
    /// This is reporting metadata only. It does not configure admission,
    /// planning, cursor encoding, or execution semantics.
    #[must_use]
    pub const fn read_intent(&self) -> ReadIntentKind {
        self.read_intent
    }

    /// Consume this payload and return response rows plus continuation cursor.
    #[must_use]
    pub fn into_response_and_cursor(self) -> (EntityResponse<E>, Option<Vec<u8>>) {
        (self.response, self.continuation_cursor)
    }
}

impl<'a, E: EntityKind> IntoIterator for &'a PagedLoadExecution<E> {
    type Item = &'a Row<E>;
    type IntoIter = std::slice::Iter<'a, Row<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

///
/// PagedLoadExecutionWithTrace
///
/// Cursor-paged load response plus optional execution trace details.
///

#[derive(Debug)]
pub struct PagedLoadExecutionWithTrace<E: EntityKind> {
    response: EntityResponse<E>,
    continuation_cursor: Option<Vec<u8>>,
    execution_trace: Option<ExecutionTrace>,
    read_intent: ReadIntentKind,
}

impl<E: EntityKind> PagedLoadExecutionWithTrace<E> {
    /// Create a traced paged load execution payload.
    #[must_use]
    pub(in crate::db) const fn new(
        response: EntityResponse<E>,
        continuation_cursor: Option<Vec<u8>>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            response,
            continuation_cursor,
            execution_trace,
            read_intent: ReadIntentKind::Unspecified,
        }
    }

    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &EntityResponse<E> {
        &self.response
    }

    /// Borrow an iterator over paged rows in response order.
    pub fn iter(&self) -> std::slice::Iter<'_, Row<E>> {
        self.response.iter()
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Borrow optional execution trace details.
    #[must_use]
    pub const fn execution_trace(&self) -> Option<&ExecutionTrace> {
        self.execution_trace.as_ref()
    }

    /// Borrow compact execution metrics derived from the optional execution trace.
    #[must_use]
    pub fn execution_metrics(&self) -> Option<ExecutionMetrics> {
        self.execution_trace.as_ref().map(ExecutionTrace::metrics)
    }

    /// Return diagnostic read-intent metadata for this paged execution.
    ///
    /// This is reporting metadata only. It does not configure admission,
    /// planning, cursor encoding, or execution semantics.
    #[must_use]
    pub const fn read_intent(&self) -> ReadIntentKind {
        self.read_intent
    }

    /// Attach diagnostic read-intent metadata.
    #[must_use]
    pub(in crate::db) const fn with_read_intent(mut self, read_intent: ReadIntentKind) -> Self {
        self.read_intent = read_intent;
        self
    }

    /// Consume this payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedLoadExecution<E> {
        PagedLoadExecution {
            response: self.response,
            continuation_cursor: self.continuation_cursor,
            read_intent: self.read_intent,
        }
    }

    /// Consume this payload and return response rows, continuation cursor, and trace.
    #[must_use]
    pub fn into_response_cursor_and_trace(
        self,
    ) -> (EntityResponse<E>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        (
            self.response,
            self.continuation_cursor,
            self.execution_trace,
        )
    }
}

impl<'a, E: EntityKind> IntoIterator for &'a PagedLoadExecutionWithTrace<E> {
    type Item = &'a Row<E>;
    type IntoIter = std::slice::Iter<'a, Row<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
