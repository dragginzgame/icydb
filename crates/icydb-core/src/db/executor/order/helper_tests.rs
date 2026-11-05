//! Test-only constructors and adapters for the parent module.

use super::*;

impl ProjectionDataRow {
    /// Borrow the canonical row without discarding its ordering evidence.
    pub(in crate::db::executor) const fn data_row(&self) -> &DataRow {
        self.row.data_row()
    }
}
