//! Module: index::expression_contract
//! Responsibility: accepted index-expression types and labels shared by schema and query paths.
//! Does not own: SQL parsing, planner access selection, or index-value encoding.
//! Boundary: carries one accepted scalar expression independently of query frontends.

use crate::db::schema::{AcceptedFieldKind, PersistedIndexExpressionOp};

/// Resolve an accepted expression's result type, retaining text length bounds.
#[must_use]
pub(in crate::db) fn index_expression_output_kind(
    op: PersistedIndexExpressionOp,
    source: &AcceptedFieldKind,
) -> Option<AcceptedFieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim
            if matches!(source, AcceptedFieldKind::Text { .. }) =>
        {
            Some(source.clone())
        }
        PersistedIndexExpressionOp::Date
            if matches!(
                source,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) =>
        {
            Some(AcceptedFieldKind::Date)
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day
            if matches!(
                source,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) =>
        {
            Some(AcceptedFieldKind::Int64)
        }
        _ => None,
    }
}

/// Render the current persisted label from the same grammar used for ordering.
#[must_use]
pub(in crate::db) fn index_expression_text(op: PersistedIndexExpressionOp, field: &str) -> String {
    let [prefix, field, suffix] = canonical_order_parts(op, field);
    ["expr:v1:", prefix, field, suffix].concat()
}

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

    /// Borrow the shared label grammar for rendering and lexical comparison.
    pub(in crate::db) const fn canonical_order_parts(&self) -> [&str; 3] {
        canonical_order_parts(self.op, self.field())
    }
}

// Keep persisted rendering, order labels and allocation-free comparisons on one grammar.
const fn canonical_order_parts(op: PersistedIndexExpressionOp, field: &str) -> [&str; 3] {
    let (prefix, suffix) = match op {
        PersistedIndexExpressionOp::Lower => ("LOWER(", ")"),
        PersistedIndexExpressionOp::Upper => ("UPPER(", ")"),
        PersistedIndexExpressionOp::Trim => ("TRIM(", ")"),
        PersistedIndexExpressionOp::LowerTrim => ("LOWER(TRIM(", "))"),
        PersistedIndexExpressionOp::Date => ("DATE(", ")"),
        PersistedIndexExpressionOp::Year => ("YEAR(", ")"),
        PersistedIndexExpressionOp::Month => ("MONTH(", ")"),
        PersistedIndexExpressionOp::Day => ("DAY(", ")"),
    };
    [prefix, field, suffix]
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
    fn expression_result_types_preserve_bounds_and_reject_incompatible_sources() {
        use PersistedIndexExpressionOp::{Date, Day, Lower, LowerTrim, Month, Trim, Upper, Year};

        for op in [Lower, Upper, Trim, LowerTrim] {
            for max_len in [None, Some(0), Some(256), Some(u32::MAX)] {
                let text = AcceptedFieldKind::Text { max_len };
                assert_eq!(index_expression_output_kind(op, &text), Some(text));
            }
            for source in [AcceptedFieldKind::Date, AcceptedFieldKind::Timestamp] {
                assert_eq!(index_expression_output_kind(op, &source), None);
            }
        }
        for (op, expected) in [
            (Date, AcceptedFieldKind::Date),
            (Year, AcceptedFieldKind::Int64),
            (Month, AcceptedFieldKind::Int64),
            (Day, AcceptedFieldKind::Int64),
        ] {
            for source in [AcceptedFieldKind::Date, AcceptedFieldKind::Timestamp] {
                assert_eq!(
                    index_expression_output_kind(op, &source),
                    Some(expected.clone())
                );
            }
            assert_eq!(
                index_expression_output_kind(op, &AcceptedFieldKind::Text { max_len: None }),
                None
            );
        }
        for op in [Lower, Upper, Trim, LowerTrim, Date, Year, Month, Day] {
            for source in [
                AcceptedFieldKind::Bool,
                AcceptedFieldKind::Int64,
                AcceptedFieldKind::Blob { max_len: None },
                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: None })),
            ] {
                assert_eq!(index_expression_output_kind(op, &source), None);
            }
        }
    }

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
                assert_eq!(
                    index_expression_text(op, field),
                    format!("expr:v1:{expected}")
                );
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
