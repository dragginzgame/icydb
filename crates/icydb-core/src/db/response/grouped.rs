//! Module: response::grouped
//! Responsibility: grouped paged response payload contracts.
//! Does not own: grouped execution evaluation, route policy, or cursor token protocol.
//! Boundary: grouped DTOs returned by session/query execution APIs.

use crate::{
    db::diagnostics::{ExecutionMetrics, ExecutionTrace},
    value::Value,
};

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

    #[must_use]
    pub fn from_parts<I, J>(group_key: I, aggregate_values: J) -> Self
    where
        I: IntoIterator<Item = Value>,
        J: IntoIterator<Item = Value>,
    {
        Self {
            group_key: group_key.into_iter().collect(),
            aggregate_values: aggregate_values.into_iter().collect(),
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
/// Cursor-paged grouped execution payload with optional continuation cursor bytes.
///

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct PagedGroupedExecutionWithTrace {
    rows: Vec<GroupedRow>,
    continuation_cursor: Option<Vec<u8>>,
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
            rows,
            continuation_cursor,
            execution_trace,
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

    /// Consume payload and drop trace details.
    #[must_use]
    pub fn into_execution(self) -> PagedGroupedExecution {
        PagedGroupedExecution {
            rows: self.rows,
            continuation_cursor: self.continuation_cursor,
        }
    }

    /// Consume into grouped rows, continuation cursor bytes, and optional trace.
    #[must_use]
    pub fn into_parts(self) -> (Vec<GroupedRow>, Option<Vec<u8>>, Option<ExecutionTrace>) {
        (self.rows, self.continuation_cursor, self.execution_trace)
    }
}

// Internal grouped page payload that carries an already-encoded outward cursor.
// This keeps the direct text-cursor bridge explicit without widening the public
// grouped response DTO surface.
pub(in crate::db) type GroupedTextCursorPageWithTrace =
    (Vec<GroupedRow>, Option<String>, Option<ExecutionTrace>);
