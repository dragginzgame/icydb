//! Module: db::query::plan::order_term
//! Responsibility: canonical index-order term rendering shared by planner boundaries.
//! Does not own: query expression parsing or executor slot resolution.
//! Boundary: keeps index-key canonicalization in one place.

use crate::model::index::{IndexKeyItem, IndexKeyItemsRef, IndexModel};

/// Return one canonical ORDER BY term list for an index key sequence.
#[must_use]
pub(in crate::db) fn index_order_terms(index: &IndexModel) -> Vec<String> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            canonical_index_order_terms(fields.iter().copied().map(IndexKeyItem::Field))
        }
        IndexKeyItemsRef::Items(items) => canonical_index_order_terms(items.iter().copied()),
    }
}

// Field-only indexes and mixed key-item indexes share the same canonical
// ORDER BY rendering contract; only the source iterator for key items differs.
fn canonical_index_order_terms<I>(key_items: I) -> Vec<String>
where
    I: IntoIterator<Item = IndexKeyItem>,
{
    key_items
        .into_iter()
        .map(|key_item| key_item.canonical_text())
        .collect()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::{
            expr::{
                parse_supported_computed_order_expr, parse_supported_order_expr,
                render_supported_order_expr, supported_order_expr_field,
                supported_order_expr_is_plain_field,
            },
            index_order_terms,
        },
        model::index::{IndexExpression, IndexKeyItem, IndexModel},
    };

    const EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
    const EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
    const EXPRESSION_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
        "order_term_tests::idx_name_lower",
        "order_term_tests::Store",
        &EXPRESSION_INDEX_FIELDS,
        &EXPRESSION_INDEX_KEY_ITEMS,
        false,
    );

    #[test]
    fn supported_order_expr_helpers_round_trip_casefold_terms() {
        let lower = parse_supported_order_expr("LOWER(name)")
            .expect("lower(name) should parse onto the canonical expression tree");
        assert_eq!(
            supported_order_expr_field(&lower)
                .expect("lower(name) should preserve one field leaf")
                .as_str(),
            "name"
        );
        assert_eq!(
            render_supported_order_expr(&lower),
            Some("LOWER(name)".to_string())
        );

        let upper = parse_supported_order_expr("UPPER(email)")
            .expect("upper(email) should parse onto the canonical expression tree");
        assert_eq!(
            render_supported_order_expr(&upper),
            Some("UPPER(email)".to_string())
        );

        let trim = parse_supported_order_expr("TRIM(name)")
            .expect("trim(name) should parse onto the canonical expression tree");
        assert_eq!(
            render_supported_order_expr(&trim),
            Some("TRIM(name)".to_string())
        );

        let ltrim = parse_supported_order_expr("LTRIM(name)")
            .expect("ltrim(name) should parse onto the canonical expression tree");
        assert_eq!(
            render_supported_order_expr(&ltrim),
            Some("LTRIM(name)".to_string())
        );

        let rtrim = parse_supported_order_expr("RTRIM(name)")
            .expect("rtrim(name) should parse onto the canonical expression tree");
        assert_eq!(
            render_supported_order_expr(&rtrim),
            Some("RTRIM(name)".to_string())
        );

        let length = parse_supported_order_expr("LENGTH(name)")
            .expect("length(name) should parse onto the canonical expression tree");
        assert_eq!(
            render_supported_order_expr(&length),
            Some("LENGTH(name)".to_string())
        );
    }

    #[test]
    fn supported_order_expr_helpers_round_trip_bounded_numeric_terms() {
        let arithmetic = parse_supported_order_expr("age + rank")
            .expect("bounded field-to-field arithmetic should parse onto the canonical tree");
        assert_eq!(
            render_supported_order_expr(&arithmetic),
            Some("age + rank".to_string())
        );

        let rounded = parse_supported_order_expr("ROUND(age + rank, 2)")
            .expect("bounded ROUND(arithmetic, scale) should parse onto the canonical tree");
        assert_eq!(
            render_supported_order_expr(&rounded),
            Some("ROUND(age + rank, 2)".to_string())
        );

        let nested = parse_supported_order_expr("ROUND((age + rank) / (age + 1), 2)").expect(
            "bounded ROUND(parenthesized arithmetic, scale) should parse onto the canonical tree",
        );
        assert_eq!(
            render_supported_order_expr(&nested),
            Some("ROUND((age + rank) / (age + 1), 2)".to_string())
        );
    }

    #[test]
    fn supported_order_expr_helpers_distinguish_plain_fields_from_computed_terms() {
        let field = parse_supported_order_expr("id")
            .expect("plain field order terms should parse onto the canonical expression tree");
        assert!(
            supported_order_expr_is_plain_field(&field),
            "plain field order terms must stay on the schema-field path",
        );
        assert_eq!(
            parse_supported_computed_order_expr("id"),
            None,
            "plain field order terms must not re-enter computed-expression paths",
        );

        let lower = parse_supported_order_expr("LOWER(name)")
            .expect("lower(name) should parse onto the canonical expression tree");
        assert!(
            !supported_order_expr_is_plain_field(&lower),
            "computed order terms must stay on the expression path",
        );
        assert_eq!(
            parse_supported_computed_order_expr("LOWER(name)"),
            Some(lower),
            "computed order terms must stay parseable through the shared helper",
        );
    }

    #[test]
    fn index_order_terms_use_canonical_key_item_text() {
        assert_eq!(
            index_order_terms(&EXPRESSION_INDEX_MODEL),
            vec!["LOWER(name)".to_string()]
        );
    }
}
