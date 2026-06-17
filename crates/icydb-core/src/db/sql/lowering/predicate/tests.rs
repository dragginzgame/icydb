use crate::{
    db::{
        predicate::{CompareOp, Predicate},
        query::plan::expr::{
            Expr, FieldId, Function, UnaryOp, compile_normalized_bool_expr_to_predicate,
            derive_normalized_bool_expr_predicate_subset,
        },
        sql::{
            lowering::predicate::{
                derive_sql_where_expr_predicate_only_subset, lower_sql_where_bool_expr,
                lower_sql_where_expr,
            },
            parser::parse_sql,
        },
    },
    value::Value,
};

fn parse_where_expr(sql: &str) -> crate::db::sql::parser::SqlExpr {
    let statement = parse_sql(sql).expect("SQL WHERE test statement should parse");
    let crate::db::sql::parser::SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };

    select
        .predicate
        .expect("SQL WHERE test statement should carry one predicate")
}

#[test]
fn lower_sql_where_bool_expr_validates_before_normalization_for_casefold_targets() {
    let expr = parse_where_expr(
        "SELECT * FROM users WHERE UPPER(name) LIKE 'AL%' ORDER BY id ASC LIMIT 1",
    );

    let lowered =
        lower_sql_where_bool_expr(&expr).expect("UPPER(...) prefix LIKE should be admitted");
    let Expr::FunctionCall {
        function: Function::StartsWith,
        args,
    } = lowered
    else {
        panic!("UPPER(...) prefix LIKE should normalize onto STARTS_WITH(...)");
    };
    let [left, right] = args.as_slice() else {
        panic!("normalized STARTS_WITH(...) should keep two arguments");
    };
    let Expr::FunctionCall {
        function: Function::Lower,
        args,
    } = left
    else {
        panic!("casefold target should normalize onto LOWER(...)");
    };
    let [Expr::Field(field)] = args.as_slice() else {
        panic!("normalized LOWER(...) should keep the original field");
    };

    assert_eq!(field, &FieldId::new("name"));
    assert_eq!(right, &Expr::Literal(Value::Text("AL".to_string())));
}

#[test]
fn derive_where_predicate_subset_returns_none_for_admitted_expression_only_shapes() {
    let expr = parse_where_expr(
        "SELECT * FROM users WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
    );
    let lowered = lower_sql_where_bool_expr(&expr)
        .expect("admitted expression-only WHERE shape should lower successfully");

    assert!(
        derive_normalized_bool_expr_predicate_subset(&lowered).is_none(),
        "predicate extraction should stay subset-only for admitted expression-owned WHERE shapes",
    );
}

#[test]
fn lower_sql_where_expr_rejects_expression_only_shapes_on_strict_predicate_path() {
    let expr = parse_where_expr(
        "SELECT * FROM users WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
    );

    let err = lower_sql_where_expr(&expr)
        .expect_err("strict predicate-only WHERE lowering should reject expression-only shapes");

    std::assert_matches!(
        err,
        crate::db::sql::lowering::SqlLoweringError::UnsupportedWhereExpression
    );
}

#[test]
fn lower_sql_where_expr_keeps_top_level_membership_compact() {
    let expr = parse_where_expr("SELECT * FROM users WHERE age IN (10, 20, 10)");

    let predicate =
        lower_sql_where_expr(&expr).expect("strict top-level membership WHERE should lower");
    let Predicate::Compare(compare) = predicate else {
        panic!("top-level membership should lower to one compact compare predicate");
    };

    assert_eq!(compare.field(), "age");
    assert_eq!(compare.op(), CompareOp::In);
    let Value::List(values) = compare.value() else {
        panic!("compact membership predicate should carry a list literal");
    };
    assert_eq!(
        values.len(),
        2,
        "compact membership predicate should canonicalize duplicate members",
    );
}

#[test]
fn derive_where_predicate_only_subset_keeps_compare_and_membership_compact() {
    let expr = parse_where_expr(
        "SELECT * FROM users \
         WHERE collection_id = '01KV5N439P0000000000000000' \
           AND stage IN ('Draft', 'Review', 'Draft')",
    );

    let predicate = derive_sql_where_expr_predicate_only_subset(&expr)
        .expect("simple compare plus membership WHERE should stay predicate-only");
    let Predicate::And(children) = predicate else {
        panic!("predicate-only conjunction should lower to one AND predicate");
    };

    assert!(
        children.iter().any(|child| matches!(
            child,
            Predicate::Compare(compare)
                if compare.field() == "collection_id"
                    && compare.op() == CompareOp::Eq
                    && compare.value()
                        == &Value::Text("01KV5N439P0000000000000000".to_string())
        )),
        "predicate-only conjunction should retain the fixed equality prefix",
    );
    assert!(
        children.iter().any(|child| matches!(
            child,
            Predicate::Compare(compare)
                if compare.field() == "stage"
                    && compare.op() == CompareOp::In
                    && matches!(
                        compare.value(),
                        Value::List(values)
                            if values.as_slice()
                                == [
                                    Value::Text("Draft".to_string()),
                                    Value::Text("Review".to_string())
                                ]
                    )
        )),
        "predicate-only conjunction should keep membership compact and deduplicated",
    );
}

#[test]
fn derive_where_predicate_subset_recovers_folded_constant_compare_shapes() {
    let expr = parse_where_expr(
        "SELECT * FROM users WHERE name = TRIM('alpha') AND NULLIF('alpha', 'alpha') IS NULL",
    );
    let lowered = lower_sql_where_bool_expr(&expr)
        .expect("foldable compare WHERE shape should lower successfully");
    let subset = derive_normalized_bool_expr_predicate_subset(&lowered)
        .expect("foldable compare WHERE shape should recover one predicate subset");

    assert!(
        matches!(
            subset,
            crate::db::predicate::Predicate::Compare(ref compare)
                if compare.field() == "name"
                    && compare.op() == crate::db::predicate::CompareOp::Eq
                    && compare.value() == &Value::Text("alpha".to_string())
        ),
        "predicate subset derivation should stay available after legality is decided earlier",
    );
}

#[test]
fn compile_where_bool_expr_requires_normalized_shape() {
    let expr = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Eq,
        left: Box::new(Expr::Literal(Value::Int64(5))),
        right: Box::new(Expr::Field(FieldId::new("age"))),
    };

    assert!(
        std::panic::catch_unwind(|| {
            let _ = compile_normalized_bool_expr_to_predicate(&expr);
        })
        .is_err(),
        "non-normalized predicate shape should panic",
    );
}

#[test]
fn compile_where_bool_expr_keeps_bare_bool_fields_structural() {
    let expr = Expr::Field(FieldId::new("active"));

    let Predicate::Compare(compare) = compile_normalized_bool_expr_to_predicate(&expr) else {
        panic!("bare bool field should compile to compare predicate");
    };

    assert_eq!(compare.field(), "active");
    assert_eq!(compare.op(), CompareOp::Eq);
    assert_eq!(compare.value(), &Value::Bool(true));
}

#[test]
fn compile_where_bool_expr_keeps_bool_not_false_branch_structural() {
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("active"))),
    };

    let Predicate::Compare(compare) = compile_normalized_bool_expr_to_predicate(&expr) else {
        panic!("NOT bool field should compile to compare predicate");
    };

    assert_eq!(compare.field(), "active");
    assert_eq!(compare.op(), CompareOp::Eq);
    assert_eq!(compare.value(), &Value::Bool(false));
}

#[test]
fn compile_where_bool_expr_keeps_lowered_casefold_compare_structural() {
    let expr = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Eq,
        left: Box::new(Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new("name"))],
        }),
        right: Box::new(Expr::Literal(Value::Text("alice".into()))),
    };

    let Predicate::Compare(compare) = compile_normalized_bool_expr_to_predicate(&expr) else {
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

    std::assert_matches!(
        compile_normalized_bool_expr_to_predicate(&missing),
        Predicate::IsMissing { field } if field == "nickname"
    );
    std::assert_matches!(
        compile_normalized_bool_expr_to_predicate(&empty),
        Predicate::IsEmpty { field } if field == "tags"
    );
    std::assert_matches!(
        compile_normalized_bool_expr_to_predicate(&contains),
        Predicate::Compare(_)
    );
}
