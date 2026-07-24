//! Module: db::sql::parser::statement::integrity
//! Responsibility: parse the bounded `CHECK INTEGRITY` administrative grammar.
//! Does not own: entity resolution, job decoding, authorization, or execution.
//! Boundary: SQL tokens -> parser-owned integrity intent.

use crate::db::{
    sql::parser::{Parser, SqlIntegrityStatement},
    sql_shared::SqlIntegerLiteralClause,
};

impl Parser {
    pub(in crate::db::sql::parser) fn parse_integrity_statement(
        &mut self,
    ) -> Result<SqlIntegrityStatement, crate::db::sql::parser::SqlParseError> {
        self.expect_identifier_keyword("CHECK")?;
        self.expect_identifier_keyword("INTEGRITY")?;

        if self.eat_identifier_keyword("DEEP") {
            return self.parse_integrity_deep_job_statement();
        }

        let entity = self.expect_identifier()?;
        if self.eat_identifier_keyword("QUICK") {
            return Ok(SqlIntegrityStatement::Quick { entity });
        }

        self.expect_identifier_keyword("DEEP")?;
        self.expect_identifier_keyword("START")?;
        let submission_key = self.expect_string_literal()?;

        Ok(SqlIntegrityStatement::DeepStart {
            entity,
            submission_key,
        })
    }

    fn parse_integrity_deep_job_statement(
        &mut self,
    ) -> Result<SqlIntegrityStatement, crate::db::sql::parser::SqlParseError> {
        if self.eat_identifier_keyword("CONTINUE") {
            let job_id = self.expect_string_literal()?;
            self.expect_identifier_keyword("AFTER")?;
            let acknowledged_sequence =
                self.parse_u64_literal(SqlIntegerLiteralClause::IntegrityPageSequence)?;

            return Ok(SqlIntegrityStatement::DeepContinue {
                job_id,
                acknowledged_sequence,
            });
        }

        self.expect_identifier_keyword("ABORT")?;
        let job_id = self.expect_string_literal()?;

        Ok(SqlIntegrityStatement::DeepAbort { job_id })
    }
}
