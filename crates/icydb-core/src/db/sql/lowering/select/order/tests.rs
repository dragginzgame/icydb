use crate::db::sql::parser::{SqlExpr, SqlOrderDirection, SqlOrderTerm, SqlScalarFunction};

use super::lower_order_term;

#[test]
fn lowered_order_term_carries_expression_analysis_for_distinct_projection_proof() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let lowered = lower_order_term(
            SqlOrderTerm {
                field: SqlExpr::FunctionCall {
                    function: SqlScalarFunction::Lower,
                    args: vec![SqlExpr::Field("name".to_string())],
                },
                direction: SqlOrderDirection::Asc,
            },
            work,
        )
        .expect("order term should lower");

        assert!(
            lowered.analysis.references_only_direct_fields(&["name"]),
            "order analysis should prove derived expressions over projected direct fields",
        );
        assert!(
            !lowered.analysis.references_only_direct_fields(&["age"]),
            "order analysis should reject hidden direct fields",
        );

        let field_path = lower_order_term(
            SqlOrderTerm {
                field: SqlExpr::FieldPath {
                    root: "profile".to_string(),
                    segments: vec!["name".to_string()],
                },
                direction: SqlOrderDirection::Asc,
            },
            work,
        )
        .expect("field-path order term should lower");

        assert!(
            !field_path
                .analysis
                .references_only_direct_fields(&["profile"]),
            "field paths must not satisfy DISTINCT direct-field projection proof",
        );
    });
}
