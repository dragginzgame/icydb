//! Module: response::grouped
//! Responsibility: grouped paged response payload contracts.
//! Does not own: grouped execution evaluation, route policy, or cursor token protocol.
//! Boundary: grouped DTOs returned by session/query execution APIs.

use crate::{db::executor::ExecutionTrace, value::Value};

///
/// GroupedRow
///
/// One grouped result row: ordered grouping key values plus ordered aggregate outputs.
/// Group/aggregate vectors preserve query declaration order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedRow {
    group_key: Vec<Value>,
    aggregate_values: Vec<Value>,
}

impl GroupedRow {
    /// Construct one grouped row payload.
    #[must_use]
    pub const fn new(group_key: Vec<Value>, aggregate_values: Vec<Value>) -> Self {
        Self {
            group_key,
            aggregate_values,
        }
    }

    /// Borrow grouped key values.
    #[must_use]
    pub const fn group_key(&self) -> &[Value] {
        self.group_key.as_slice()
    }

    /// Borrow aggregate output values.
    #[must_use]
    pub const fn aggregate_values(&self) -> &[Value] {
        self.aggregate_values.as_slice()
    }
}

///
/// PagedGroupedExecution
///
/// Cursor-paged grouped execution payload with optional grouped continuation cursor bytes.
///

#[derive(Debug)]
pub struct PagedGroupedExecution {
    rows: Vec<GroupedRow>,
    continuation_cursor: Option<Vec<u8>>,
}

impl PagedGroupedExecution {
    /// Construct one grouped paged execution payload.
    #[must_use]
    pub const fn new(rows: Vec<GroupedRow>, continuation_cursor: Option<Vec<u8>>) -> Self {
        Self {
            rows,
            continuation_cursor,
        }
    }

    /// Borrow grouped rows.
    #[must_use]
    pub const fn rows(&self) -> &[GroupedRow] {
        self.rows.as_slice()
    }

    /// Borrow optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.continuation_cursor.as_deref()
    }

    /// Consume into grouped rows and continuation cursor bytes.
    #[must_use]
    pub fn into_parts(self) -> (Vec<GroupedRow>, Option<Vec<u8>>) {
        (self.rows, self.continuation_cursor)
    }
}

///
/// PagedGroupedExecutionWithTrace
///
/// Cursor-paged grouped execution payload plus optional route/execution trace.
///

#[derive(Debug)]
pub struct PagedGroupedExecutionWithTrace {
    execution: PagedGroupedExecution,
    execution_trace: Option<ExecutionTrace>,
}

impl PagedGroupedExecutionWithTrace {
    /// Construct one traced grouped paged execution payload.
    #[must_use]
    pub const fn new(
        rows: Vec<GroupedRow>,
        continuation_cursor: Option<Vec<u8>>,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            execution: PagedGroupedExecution::new(rows, continuation_cursor),
            execution_trace,
        }
    }

    /// Borrow grouped execution payload.
    #[must_use]
    pub const fn execution(&self) -> &PagedGroupedExecution {
        &self.execution
    }

    /// Borrow grouped rows.
    #[must_use]
    pub const fn rows(&self) -> &[GroupedRow] {
        self.execution.rows()
    }

    /// Borrow optional continuation cursor bytes.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&[u8]> {
        self.execution.continuation_cursor()
    }

    /// Borrow optional execution trace details.
    #[must_use]
    pub const fn execution_trace(&self) -> Option<&ExecutionTrace> {
        self.execution_trace.as_ref()
    }

    /// Consume payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedGroupedExecution {
        self.execution
    }

    /// Consume into grouped rows, continuation cursor bytes, and optional trace.
    #[must_use]
    pub fn into_parts(self) -> (Vec<GroupedRow>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        let (rows, continuation_cursor) = self.execution.into_parts();

        (rows, continuation_cursor, self.execution_trace)
    }
}
