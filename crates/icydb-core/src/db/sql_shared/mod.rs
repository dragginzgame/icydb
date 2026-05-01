//! Module: db::sql_shared
//! Responsibility: shared SQL lexical utilities: tokenization, parse errors, and token-cursor primitives.
//! Does not own: predicate semantics, statement policy, AST lowering, command DTOs, or executor behavior.
//! Boundary: predicate parsing and the feature-gated SQL frontend both build on
//! this ungated lexical layer.

mod cursor;
mod lexer;
pub(in crate::db::sql_shared) mod types;

pub(crate) use cursor::SqlTokenCursor;
pub(crate) use lexer::tokenize_sql;
pub(crate) use types::{Keyword, SqlParseError, TokenKind};

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

    #[test]
    fn tokenize_sql_decodes_hex_blob_literals() {
        let tokens = tokenize_sql("X'0A0b' x'FF'").expect("blob literals should tokenize");

        let kinds = tokens
            .into_iter()
            .map(|token| token.kind)
            .collect::<Vec<_>>();

        assert!(matches!(
            kinds.first(),
            Some(TokenKind::BlobLiteral(value)) if value == &[0x0A, 0x0B]
        ));
        assert!(matches!(
            kinds.get(1),
            Some(TokenKind::BlobLiteral(value)) if value == &[0xFF]
        ));
    }

    #[test]
    fn tokenize_sql_rejects_malformed_hex_blob_literals() {
        let err = tokenize_sql("X'ABC'").expect_err("odd-length blob literal should fail");

        assert_eq!(
            err,
            crate::db::sql_shared::SqlParseError::InvalidSyntax {
                message: "blob literal must contain an even number of hex digits".to_string()
            }
        );

        let err = tokenize_sql("X'ABCG'").expect_err("non-hex blob literal should fail");

        assert_eq!(
            err,
            crate::db::sql_shared::SqlParseError::InvalidSyntax {
                message: "blob literal must contain only hexadecimal digits".to_string()
            }
        );
    }

    #[test]
    fn tokenize_sql_rejects_oversized_hex_blob_literals() {
        let oversized_hex = "00".repeat(1_048_577);
        let sql = format!("X'{oversized_hex}'");

        let err = tokenize_sql(sql.as_str()).expect_err("oversized blob literal should fail");

        assert_eq!(
            err,
            crate::db::sql_shared::SqlParseError::InvalidSyntax {
                message: "blob literal exceeds maximum decoded byte length of 1048576".to_string()
            }
        );
    }
}
