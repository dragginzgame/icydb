//! Query-error adaptation for the shared construction text sink.

use crate::db::resource::construction::ConstructionBudget;

#[cfg(test)]
mod tests;

use crate::db::{QueryError, query::preparation::PreparationWork};
use std::fmt;

impl PreparationWork<'_> {
    /// Render against the current request, preserving its typed exhaustion.
    pub(in crate::db) fn render_text(
        &self,
        render: impl FnOnce(&mut dyn std::fmt::Write) -> fmt::Result,
    ) -> Result<String, QueryError> {
        (self as &dyn ConstructionBudget)
            .render_text(render)
            .map_err(QueryError::execute)
    }
}
