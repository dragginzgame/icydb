//! Module: db::executor::projection::materialize::row_view
//! Responsibility: projected-row transport before final response materialization.
//! Does not own: projection evaluation, DISTINCT key storage, or DTO shaping.
//! Boundary: owns projected values retained by DISTINCT or final output.

use crate::{db::executor::budget::runtime_value_work, value::Value};

///
/// RowView
///
/// RowView is the compact owned projection-materialization transport used by
/// blocking DISTINCT state before the structural boundary builds the public
/// row matrix. Current-row evaluation may borrow from its raw-row owner, but
/// crossing this boundary always requires explicit value ownership.
///

pub(in crate::db::executor::projection::materialize) struct RowView(Vec<Value>);

impl RowView {
    #[must_use]
    pub(in crate::db::executor::projection::materialize) const fn owned(
        values: Vec<Value>,
    ) -> Self {
        Self(values)
    }

    #[inline]
    pub(in crate::db::executor::projection::materialize) fn get(&self, idx: usize) -> &Value {
        &self.0[idx]
    }

    pub(in crate::db::executor::projection::materialize) fn into_owned(self) -> Vec<Value> {
        self.0
    }

    #[inline]
    pub(in crate::db::executor::projection::materialize) const fn values(&self) -> &[Value] {
        self.0.as_slice()
    }

    /// Estimate the complete owned value backing retained by this projected row.
    #[must_use]
    pub(in crate::db::executor::projection::materialize) fn estimated_backing_bytes(&self) -> u64 {
        self.0.iter().fold(0_u64, |total, value| {
            total.saturating_add(runtime_value_work(value).0)
        })
    }
}
