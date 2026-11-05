//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db) fn compile_sql_query_for_tests(
        &self,
        sql: &str,
    ) -> Result<(SqlCompiledCommandExecutionContext, String), QueryError> {
        self.compile_sql_query_text_execution_context(sql)
    }
}
