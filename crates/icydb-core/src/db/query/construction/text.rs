//! Abortable text construction against the caller's existing budget owner.

use std::fmt::{self, Write};

use crate::{
    db::query::construction::ConstructionBudget, error::InternalError,
    value::decimal::ValueFormatWriter,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl dyn ConstructionBudget + '_ {
    /// Render incrementally without retaining partial output on failure. The
    /// sink's typed budget error takes precedence even if a formatter swallows
    /// its fmt::Error; formatting alone never establishes a new budget scope.
    pub(in crate::db) fn render_text(
        &self,
        render: impl FnOnce(&mut dyn ValueFormatWriter) -> fmt::Result,
    ) -> Result<String, InternalError> {
        let mut output = ConstructionText {
            budget: self,
            text: String::new(),
            error: None,
        };
        let result = render(&mut output);
        if let Some(error) = output.error {
            return Err(error);
        }
        result.map_err(|_| InternalError::query_executor_invariant())?;
        Ok(output.text)
    }
}

struct ConstructionText<'a> {
    budget: &'a dyn ConstructionBudget,
    text: String,
    error: Option<InternalError>,
}

impl ValueFormatWriter for ConstructionText<'_> {
    fn admit_scratch(&mut self, bytes: u64, steps: u64) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let admission = self.budget.admit_format_scratch(bytes, steps);
        if let Err(error) = admission {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        Ok(())
    }
}

impl Write for ConstructionText<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let admission = self
            .budget
            .charge(Resource::PredicateExpressionSteps, text.len() as u64)
            .and_then(|()| self.budget.reserve_string(&mut self.text, text.len()));
        if let Err(error) = admission {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}
