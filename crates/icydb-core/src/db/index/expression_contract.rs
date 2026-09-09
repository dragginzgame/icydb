//! Module: index::expression_contract
//! Responsibility: accepted index-expression identity shared by rebuild and query paths.
//! Does not own: SQL parsing, planner access selection, or index-value encoding.
//! Boundary: carries one accepted scalar expression independently of query frontends.

use crate::db::schema::PersistedIndexExpressionOp;

/// Return whether an accepted expression key has exactly the same transform
/// as the current text-casefold predicate contract.
#[must_use]
pub(in crate::db) const fn index_expression_supports_text_casefold_lookup(
    op: PersistedIndexExpressionOp,
) -> bool {
    matches!(op, PersistedIndexExpressionOp::Lower)
}

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
    pub(in crate::db) const fn supports_text_casefold_lookup(&self) -> bool {
        index_expression_supports_text_casefold_lookup(self.op)
    }

    #[must_use]
    pub(in crate::db) fn canonical_order_text(&self) -> String {
        self.canonical_order_parts().concat()
    }

    /// Compare the canonical label without constructing a temporary string.
    #[must_use]
    pub(in crate::db) fn matches_canonical_order_text(&self, text: &str) -> bool {
        let mut remaining = text;
        for part in self.canonical_order_parts() {
            let Some(suffix) = remaining.strip_prefix(part) else {
                return false;
            };
            remaining = suffix;
        }
        remaining.is_empty()
    }

    // Rendering and comparison use exactly the same accepted label grammar.
    const fn canonical_order_parts(&self) -> [&str; 3] {
        let (prefix, suffix) = match self.op {
            PersistedIndexExpressionOp::Lower => ("LOWER(", ")"),
            PersistedIndexExpressionOp::Upper => ("UPPER(", ")"),
            PersistedIndexExpressionOp::Trim => ("TRIM(", ")"),
            PersistedIndexExpressionOp::LowerTrim => ("LOWER(TRIM(", "))"),
            PersistedIndexExpressionOp::Date => ("DATE(", ")"),
            PersistedIndexExpressionOp::Year => ("YEAR(", ")"),
            PersistedIndexExpressionOp::Month => ("MONTH(", ")"),
            PersistedIndexExpressionOp::Day => ("DAY(", ")"),
        };
        [prefix, self.field(), suffix]
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(SemanticIndexExpression {
Self{op,field} => [op,field],
});

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_expression_comparison_matches_exact_rendered_bytes() {
        use PersistedIndexExpressionOp::{Date, Day, Lower, LowerTrim, Month, Trim, Upper, Year};
        for (op, prefix, suffix) in [
            (Lower, "LOWER(", ")"),
            (Upper, "UPPER(", ")"),
            (Trim, "TRIM(", ")"),
            (LowerTrim, "LOWER(TRIM(", "))"),
            (Date, "DATE(", ")"),
            (Year, "YEAR(", ")"),
            (Month, "MONTH(", ")"),
            (Day, "DAY(", ")"),
        ] {
            for field in ["name", "账户.名", "odd)field(", ""] {
                let expression = SemanticIndexExpression::new(op, field.to_string());
                let expected = format!("{prefix}{field}{suffix}");
                assert_eq!(expression.canonical_order_text(), expected);
                assert!(expression.matches_canonical_order_text(&expected));
                for boundary in 0..expected.len() {
                    if let Some(truncated) = expected.get(..boundary) {
                        assert!(!expression.matches_canonical_order_text(truncated));
                    }
                }
                assert!(!expression.matches_canonical_order_text(&format!("{expected}x")));
                assert!(!expression.matches_canonical_order_text(&expected.to_lowercase()));
            }
        }
    }
}
