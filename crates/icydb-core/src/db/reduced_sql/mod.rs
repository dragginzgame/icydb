//! Module: db::reduced_sql
//! Responsibility: reduced SQL tokenization, shared parse errors, and token-cursor primitives.
//! Does not own: predicate semantics, statement AST lowering, or executor behavior.
//! Boundary: predicate parsing and the SQL frontend both build on this shared lexical layer.

mod cursor;
mod lexer;
pub(in crate::db::reduced_sql) mod types;

pub(crate) use cursor::SqlTokenCursor;
pub(crate) use lexer::tokenize_sql;
pub(crate) use types::{Keyword, SqlParseError, Token, TokenKind};

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{Keyword, TokenKind, tokenize_sql};

    #[test]
    fn tokenize_sql_classifies_mixed_case_keywords_without_normalization_changes() {
        let tokens = tokenize_sql("SeLeCt id FrOm Customer OrDeR By id aSc LiMiT 1")
            .expect("mixed-case keyword SQL should tokenize");

        let kinds = tokens
            .into_iter()
            .map(|token| token.kind)
            .collect::<Vec<_>>();

        assert!(matches!(
            kinds.first(),
            Some(TokenKind::Keyword(Keyword::Select))
        ));
        assert!(matches!(
            kinds.get(2),
            Some(TokenKind::Keyword(Keyword::From))
        ));
        assert!(matches!(
            kinds.get(4),
            Some(TokenKind::Keyword(Keyword::Order))
        ));
        assert!(matches!(
            kinds.get(5),
            Some(TokenKind::Keyword(Keyword::By))
        ));
        assert!(matches!(
            kinds.get(7),
            Some(TokenKind::Keyword(Keyword::Asc))
        ));
        assert!(matches!(
            kinds.get(8),
            Some(TokenKind::Keyword(Keyword::Limit))
        ));
    }

    #[test]
    fn tokenize_sql_preserves_non_keyword_identifiers() {
        let tokens = tokenize_sql("selectivity customer_order order_total")
            .expect("identifiers should tokenize");

        let kinds = tokens
            .into_iter()
            .map(|token| token.kind)
            .collect::<Vec<_>>();

        assert!(matches!(
            kinds.first(),
            Some(TokenKind::Identifier(value)) if value == "selectivity"
        ));
        assert!(matches!(
            kinds.get(1),
            Some(TokenKind::Identifier(value)) if value == "customer_order"
        ));
        assert!(matches!(
            kinds.get(2),
            Some(TokenKind::Identifier(value)) if value == "order_total"
        ));
    }

    #[test]
    fn tokenize_sql_preserves_qualified_identifier_segments() {
        let tokens = tokenize_sql("public.Customer").expect("qualified identifier should tokenize");

        let kinds = tokens
            .into_iter()
            .map(|token| token.kind)
            .collect::<Vec<_>>();

        assert!(matches!(
            kinds.first(),
            Some(TokenKind::Identifier(value)) if value == "public"
        ));
        assert!(matches!(kinds.get(1), Some(TokenKind::Dot)));
        assert!(matches!(
            kinds.get(2),
            Some(TokenKind::Identifier(value)) if value == "Customer"
        ));
    }
}
