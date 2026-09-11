//! Abortable text construction against the existing preparation request.

#[cfg(test)]
mod tests;

use std::fmt::{self, Write};

use crate::db::{QueryError, query::preparation::PreparationWork};
use crate::value::decimal::ValueFormatWriter;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl PreparationWork<'_> {
    /// Render incrementally without retaining partial output on failure. The
    /// sink's typed budget error takes precedence even if a formatter swallows
    /// its fmt::Error; formatting alone never establishes a new budget scope.
    pub(in crate::db) fn render_text(
        &self,
        render: impl FnOnce(&mut dyn ValueFormatWriter) -> fmt::Result,
    ) -> Result<String, QueryError> {
        let mut output = PreparationText {
            work: self,
            text: String::new(),
            error: None,
        };
        let result = render(&mut output);
        if let Some(error) = output.error {
            return Err(error);
        }
        result.map_err(|_| QueryError::invariant())?;
        Ok(output.text)
    }
}

struct PreparationText<'a, 'scope> {
    work: &'a PreparationWork<'scope>,
    text: String,
    error: Option<QueryError>,
}

impl ValueFormatWriter for PreparationText<'_, '_> {
    fn admit_scratch(&mut self, bytes: u64, steps: u64) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let admission = self
            .work
            // Check prior instruction use before entering this pre-admitted
            // conversion. The enclosing preparation scope also checks on exit.
            .check_instruction_watermark()
            .map_err(QueryError::execute)
            .and_then(|()| self.work.charge(Resource::TemporaryBytes, bytes))
            .and_then(|()| self.work.charge(Resource::PredicateExpressionSteps, steps));
        if let Err(error) = admission {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        Ok(())
    }
}

impl Write for PreparationText<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let admission = self
            .work
            .charge(Resource::PredicateExpressionSteps, text.len() as u64)
            .and_then(|()| self.work.reserve_string(&mut self.text, text.len()));
        if let Err(error) = admission {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}
