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
