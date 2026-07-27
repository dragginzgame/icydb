//! Module: index::expression_contract
//! Responsibility: accepted index-expression identity shared by rebuild and query paths.
//! Does not own: SQL parsing, planner access selection, or index-value encoding.
//! Boundary: carries one accepted scalar expression independently of query features.

use crate::db::schema::PersistedIndexExpressionOp;

///
/// SemanticIndexExpression
///
/// Accepted scalar index expression used by both index maintenance and query
/// planning without requiring the query access module in non-SQL builds.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SemanticIndexExpression {
    op: PersistedIndexExpressionOp,
    field: String,
}

impl SemanticIndexExpression {
    #[must_use]
    pub(in crate::db) const fn new(op: PersistedIndexExpressionOp, field: String) -> Self {
        Self { op, field }
    }

    #[must_use]
    pub(in crate::db) const fn field(&self) -> &str {
        self.field.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn supports_text_casefold_lookup(&self) -> bool {
        matches!(
            self.op,
            PersistedIndexExpressionOp::Lower | PersistedIndexExpressionOp::Upper
        )
    }

    #[must_use]
    pub(in crate::db) fn canonical_order_text(&self) -> String {
        let field = self.field();

        match self.op {
            PersistedIndexExpressionOp::Lower => format!("LOWER({field})"),
            PersistedIndexExpressionOp::Upper => format!("UPPER({field})"),
            PersistedIndexExpressionOp::Trim => format!("TRIM({field})"),
            PersistedIndexExpressionOp::LowerTrim => format!("LOWER(TRIM({field}))"),
            PersistedIndexExpressionOp::Date => format!("DATE({field})"),
            PersistedIndexExpressionOp::Year => format!("YEAR({field})"),
            PersistedIndexExpressionOp::Month => format!("MONTH({field})"),
            PersistedIndexExpressionOp::Day => format!("DAY({field})"),
        }
    }
}
