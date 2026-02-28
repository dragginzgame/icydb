//! Module: response::paged
//! Responsibility: paged load response payload contracts.
//! Does not own: query execution, pagination planning, or cursor token protocol.
//! Boundary: response DTOs returned by session/query execution APIs.

use crate::{
    db::{executor::ExecutionTrace, response::Response},
    traits::EntityKind,
};

///
/// PagedLoadExecution
///
/// Cursor-paged load response with optional continuation cursor bytes.
///

#[derive(Debug)]
pub struct PagedLoadExecution<E: EntityKind> {
    response: Response<E>,
    continuation_cursor: Option<Vec<u8>>,
}

impl<E: EntityKind> PagedLoadExecution<E> {
    /// Create a paged load execution payload.
    #[must_use]
    pub const fn new(response: Response<E>, continuation_cursor: Option<Vec<u8>>) -> Self {
        Self {
            response,
            continuation_cursor,
        }
    }

    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &Response<E> {
        &self.response
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Consume this payload and return `(response, continuation_cursor)`.
    #[must_use]
    pub fn into_parts(self) -> (Response<E>, Option<Vec<u8>>) {
        (self.response, self.continuation_cursor)
    }
}

impl<E: EntityKind> From<(Response<E>, Option<Vec<u8>>)> for PagedLoadExecution<E> {
    fn from(value: (Response<E>, Option<Vec<u8>>)) -> Self {
        let (response, continuation_cursor) = value;

        Self::new(response, continuation_cursor)
    }
}

impl<E: EntityKind> From<PagedLoadExecution<E>> for (Response<E>, Option<Vec<u8>>) {
    fn from(value: PagedLoadExecution<E>) -> Self {
        value.into_parts()
    }
}

///
/// PagedLoadExecutionWithTrace
///
/// Cursor-paged load response plus optional execution trace details.
///

#[derive(Debug)]
pub struct PagedLoadExecutionWithTrace<E: EntityKind> {
    execution: PagedLoadExecution<E>,
    execution_trace: Option<ExecutionTrace>,
}

impl<E: EntityKind> PagedLoadExecutionWithTrace<E> {
    /// Create a traced paged load execution payload.
    #[must_use]
    pub const fn new(
        response: Response<E>,
        continuation_cursor: Option<Vec<u8>>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            execution: PagedLoadExecution::new(response, continuation_cursor),
            execution_trace,
        }
    }

    /// Borrow the paged execution payload.
    #[must_use]
    pub const fn execution(&self) -> &PagedLoadExecution<E> {
        &self.execution
    }

    /// Borrow the paged response rows.
    #[must_use]
    pub const fn response(&self) -> &Response<E> {
        self.execution.response()
    }

    /// Borrow the optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.execution.continuation_cursor()
    }

    /// Borrow optional execution trace details.
    #[must_use]
    pub const fn execution_trace(&self) -> Option<&ExecutionTrace> {
        self.execution_trace.as_ref()
    }

    /// Consume this payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedLoadExecution<E> {
        self.execution
    }

    /// Consume this payload and return `(response, continuation_cursor, trace)`.
    #[must_use]
    pub fn into_parts(self) -> (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        let (response, continuation_cursor) = self.execution.into_parts();

        (response, continuation_cursor, self.execution_trace)
    }
}

impl<E: EntityKind> From<(Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)>
    for PagedLoadExecutionWithTrace<E>
{
    fn from(value: (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)) -> Self {
        let (response, continuation_cursor, execution_trace) = value;

        Self::new(response, continuation_cursor, execution_trace)
    }
}

impl<E: EntityKind> From<PagedLoadExecutionWithTrace<E>>
    for (Response<E>, Option<Vec<u8>>, Option<ExecutionTrace>)
{
    fn from(value: PagedLoadExecutionWithTrace<E>) -> Self {
        value.into_parts()
    }
}
