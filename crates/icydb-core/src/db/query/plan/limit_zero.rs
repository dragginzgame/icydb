//! Module: query::plan::limit_zero
//! Responsibility: planner-owned zero-window (`LIMIT 0`) detection.
//! Does not own: pagination policy validation or access dispatch.
//! Boundary: identifies load-mode windows that must return no rows.

use crate::db::query::plan::QueryMode;

/// Return true when a query mode declares an explicit load `LIMIT 0` window.
#[must_use]
pub(in crate::db::query) fn is_limit_zero_load_window(mode: QueryMode) -> bool {
    matches!(mode, QueryMode::Load(spec) if spec.limit() == Some(0))
}
