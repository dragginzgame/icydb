//! Module: db::query::plan::order_term
//! Responsibility: canonical ORDER BY term helpers shared by parser/lowering/planner boundaries.
//! Does not own: SQL statement parsing or executor slot resolution.
//! Boundary: keeps supported expression-order term parsing and index-key canonicalization in one place.

use crate::model::index::{IndexKeyItem, IndexKeyItemsRef, IndexModel};

///
/// ExpressionOrderTerm
///
/// Canonical reduced-SQL expression ORDER BY term supported in the current release.
/// This keeps expression ordering intentionally narrow until executor fallback
/// ordering grows beyond raw field-slot comparison.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExpressionOrderTerm<'a> {
    Lower(&'a str),
    Upper(&'a str),
}

impl<'a> ExpressionOrderTerm<'a> {
    /// Parse one supported expression ORDER BY term from its canonical text form.
    #[must_use]
    pub(in crate::db) fn parse(term: &'a str) -> Option<Self> {
        if let Some(field) = parse_expression_order_term_inner(term, "LOWER") {
            return Some(Self::Lower(field));
        }
        if let Some(field) = parse_expression_order_term_inner(term, "UPPER") {
            return Some(Self::Upper(field));
        }

        None
    }

    /// Borrow the referenced field within this expression order term.
    #[must_use]
    pub(in crate::db) const fn field(self) -> &'a str {
        match self {
            Self::Lower(field) | Self::Upper(field) => field,
        }
    }

    /// Rebuild this term with one replacement field identifier.
    #[must_use]
    pub(in crate::db) fn canonical_text_with_field(self, field: &str) -> String {
        match self {
            Self::Lower(_) => format!("LOWER({field})"),
            Self::Upper(_) => format!("UPPER({field})"),
        }
    }
}

/// Return one canonical ORDER BY term list for an index key sequence.
#[must_use]
pub(in crate::db) fn index_order_terms(index: &IndexModel) -> Vec<String> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            fields.iter().map(|field| (*field).to_string()).collect()
        }
        IndexKeyItemsRef::Items(items) => items.iter().map(IndexKeyItem::canonical_text).collect(),
    }
}

fn parse_expression_order_term_inner<'a>(term: &'a str, function: &str) -> Option<&'a str> {
    let open_index = term.find('(')?;
    if !term[..open_index].eq_ignore_ascii_case(function) || !term.ends_with(')') {
        return None;
    }

    Some(&term[open_index.saturating_add(1)..term.len().saturating_sub(1)])
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::{ExpressionOrderTerm, index_order_terms},
        model::index::{IndexExpression, IndexKeyItem, IndexModel},
    };

    const EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
    const EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
    const EXPRESSION_INDEX_MODEL: IndexModel = IndexModel::new_with_key_items(
        "order_term_tests::idx_name_lower",
        "order_term_tests::Store",
        &EXPRESSION_INDEX_FIELDS,
        &EXPRESSION_INDEX_KEY_ITEMS,
        false,
    );

    #[test]
    fn expression_order_term_parse_recovers_supported_casefold_functions() {
        assert_eq!(
            ExpressionOrderTerm::parse("LOWER(name)"),
            Some(ExpressionOrderTerm::Lower("name")),
        );
        assert_eq!(
            ExpressionOrderTerm::parse("UPPER(name)"),
            Some(ExpressionOrderTerm::Upper("name")),
        );
        assert_eq!(ExpressionOrderTerm::parse("TRIM(name)"), None);
    }

    #[test]
    fn index_order_terms_use_canonical_key_item_text() {
        assert_eq!(
            index_order_terms(&EXPRESSION_INDEX_MODEL),
            vec!["LOWER(name)".to_string()]
        );
    }
}
