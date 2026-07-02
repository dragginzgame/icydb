//! Module: db::executor::projection::materialize::structural
//! Responsibility: structural projection page orchestration.
//! Does not own: scalar expression evaluation or DISTINCT key semantics.
//! Boundary: the only materialize module that knows `StructuralCursorPage`.

mod dispatch;
mod distinct_entrypoints;
mod identity;

use crate::{db::executor::projection::materialize::row_view::RowView, value::Value};

pub(in crate::db) use dispatch::project;
pub(in crate::db::executor::projection) use distinct_entrypoints::project_distinct;

///
/// MaterializedProjectionRows
///
/// MaterializedProjectionRows is the executor-owned transport wrapper for one
/// structurally projected page. It keeps nested value-row storage an executor
/// implementation detail until an adapter consumes the page for DTO
/// shaping.
///

#[derive(Debug)]
pub(in crate::db) struct MaterializedProjectionRows(Vec<Vec<Value>>);

impl MaterializedProjectionRows {
    /// Build structural projection rows from executor-owned value rows.
    pub(in crate::db::executor) const fn from_value_rows(rows: Vec<Vec<Value>>) -> Self {
        Self(rows)
    }

    /// Build structural projection rows from local row views at the final
    /// response materialization boundary.
    pub(in crate::db::executor::projection::materialize::structural) fn from_row_views(
        rows: Vec<RowView<'_>>,
    ) -> Self {
        Self(rows.into_iter().map(RowView::into_owned).collect())
    }

    /// Build an empty structural projection row payload.
    pub(in crate::db::executor) const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Return the number of materialized structural projection rows.
    #[must_use]
    pub(in crate::db::executor) const fn len(&self) -> usize {
        self.0.len()
    }

    /// Borrow the materialized structural projection row values.
    #[must_use]
    pub(in crate::db) const fn value_rows(&self) -> &[Vec<Value>] {
        self.0.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.0
    }
}

pub(super) struct RowViewCollector {
    rows: Vec<Vec<Value>>,
}

impl RowViewCollector {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    pub(super) fn push(&mut self, row_view: RowView<'_>) {
        self.rows.push(row_view.into_owned());
    }

    pub(super) fn finish(self) -> MaterializedProjectionRows {
        MaterializedProjectionRows::from_value_rows(self.rows)
    }
}
