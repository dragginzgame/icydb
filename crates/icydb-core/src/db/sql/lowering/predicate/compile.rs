use crate::db::{
    predicate::{Predicate, compile_bool_expr_to_predicate},
    query::plan::expr::Expr,
};

pub(super) fn compile_where_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    compile_bool_expr_to_predicate(expr)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        predicate::{CompareOp, Predicate},
        query::plan::expr::{BinaryOp, Expr, FieldId, Function},
    };
    use crate::value::Value;

    #[test]
    #[should_panic(expected = "normalized boolean expression")]
    fn compile_where_bool_expr_requires_normalized_shape() {
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Literal(Value::Int(5))),
            right: Box::new(Expr::Field(FieldId::new("age"))),
        };

        let _ = super::compile_where_bool_expr_to_predicate(&expr);
    }

    #[test]
    fn compile_where_bool_expr_keeps_bare_bool_fields_structural() {
        let expr = Expr::Field(FieldId::new("active"));

        let Predicate::Compare(compare) = super::compile_where_bool_expr_to_predicate(&expr) else {
            panic!("bare bool field should compile to compare predicate");
        };

        assert_eq!(compare.field(), "active");
        assert_eq!(compare.op(), CompareOp::Eq);
        assert_eq!(compare.value(), &Value::Bool(true));
    }

    #[test]
    fn compile_where_bool_expr_keeps_bool_not_false_branch_structural() {
        let expr = Expr::Unary {
            op: crate::db::query::plan::expr::UnaryOp::Not,
            expr: Box::new(Expr::Field(FieldId::new("active"))),
        };

        let Predicate::Compare(compare) = super::compile_where_bool_expr_to_predicate(&expr) else {
            panic!("NOT bool field should compile to compare predicate");
        };

        assert_eq!(compare.field(), "active");
        assert_eq!(compare.op(), CompareOp::Eq);
        assert_eq!(compare.value(), &Value::Bool(false));
    }

    #[test]
    fn compile_where_bool_expr_keeps_lowered_casefold_compare_structural() {
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(FieldId::new("name"))],
            }),
            right: Box::new(Expr::Literal(Value::Text("alice".into()))),
        };

        let Predicate::Compare(compare) = super::compile_where_bool_expr_to_predicate(&expr) else {
            panic!("LOWER(field) compare should compile to compare predicate");
        };

        assert_eq!(compare.field(), "name");
        assert_eq!(compare.op(), CompareOp::Eq);
        assert_eq!(compare.value(), &Value::Text("alice".into()));
        assert_eq!(
            compare.coercion().id(),
            crate::db::predicate::CoercionId::TextCasefold
        );
    }

    #[test]
    fn compile_where_bool_expr_supports_missing_empty_and_collection_contains_functions() {
        let missing = Expr::FunctionCall {
            function: Function::IsMissing,
            args: vec![Expr::Field(FieldId::new("nickname"))],
        };
        let empty = Expr::FunctionCall {
            function: Function::IsEmpty,
            args: vec![Expr::Field(FieldId::new("tags"))],
        };
        let contains = Expr::FunctionCall {
            function: Function::CollectionContains,
            args: vec![
                Expr::Field(FieldId::new("tags")),
                Expr::Literal(Value::Text("mage".into())),
            ],
        };

        assert!(matches!(
            super::compile_where_bool_expr_to_predicate(&missing),
            Predicate::IsMissing { field } if field == "nickname"
        ));
        assert!(matches!(
            super::compile_where_bool_expr_to_predicate(&empty),
            Predicate::IsEmpty { field } if field == "tags"
        ));
        assert!(matches!(
            super::compile_where_bool_expr_to_predicate(&contains),
            Predicate::Compare(_)
        ));
    }
}
