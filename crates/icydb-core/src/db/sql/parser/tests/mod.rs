//! Module: db::sql::parser::tests
//! Covers SQL parser behavior, error classification, and syntax edge cases.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    SqlAggregateCall, SqlAggregateInputExpr, SqlAggregateKind, SqlArithmeticProjectionCall,
    SqlArithmeticProjectionOp, SqlAssignment, SqlCaseArm, SqlDeleteStatement, SqlDescribeStatement,
    SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlExpr, SqlExprBinaryOp,
    SqlInsertSource, SqlInsertStatement, SqlOrderDirection, SqlOrderTerm, SqlParseError,
    SqlProjection, SqlProjectionOperand, SqlReturningProjection, SqlRoundProjectionCall,
    SqlRoundProjectionInput, SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement,
    SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement, SqlTextFunction,
    SqlTextFunctionCall, SqlUpdateStatement, parse_sql,
};
use crate::{
    db::predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
    value::Value,
};

macro_rules! option_sql_pred {
    ($predicate:expr) => {
        Some(sql_expr_from_runtime_predicate($predicate))
    };
}

fn sql_order_expr(term: &str) -> SqlExpr {
    let sql = format!("SELECT id FROM ParserOrderEntity ORDER BY {term}");
    let SqlStatement::Select(statement) =
        parse_sql(&sql).expect("ORDER BY term helper SQL should parse")
    else {
        unreachable!("ORDER BY term helper should always produce one SELECT");
    };

    statement
        .order_by
        .into_iter()
        .next()
        .expect("ORDER BY term helper SQL should carry one ORDER BY term")
        .field
}

fn sql_expr_from_runtime_predicate(predicate: Predicate) -> SqlExpr {
    match predicate {
        Predicate::True => SqlExpr::Literal(Value::Bool(true)),
        Predicate::False => SqlExpr::Literal(Value::Bool(false)),
        Predicate::And(children) => fold_predicate_children(children, SqlExprBinaryOp::And),
        Predicate::Or(children) => fold_predicate_children(children, SqlExprBinaryOp::Or),
        Predicate::Not(inner) => SqlExpr::Unary {
            op: super::SqlExprUnaryOp::Not,
            expr: Box::new(sql_expr_from_runtime_predicate(*inner)),
        },
        Predicate::Compare(compare) => sql_expr_from_compare(compare),
        Predicate::CompareFields(compare) => SqlExpr::Binary {
            op: sql_binary_from_compare(compare.op()),
            left: Box::new(SqlExpr::Field(compare.left_field().to_string())),
            right: Box::new(SqlExpr::Field(compare.right_field().to_string())),
        },
        Predicate::IsNull { field } => SqlExpr::NullTest {
            expr: Box::new(SqlExpr::Field(field)),
            negated: false,
        },
        Predicate::IsNotNull { field } => SqlExpr::NullTest {
            expr: Box::new(SqlExpr::Field(field)),
            negated: true,
        },
        Predicate::IsMissing { field } => SqlExpr::FunctionCall {
            function: SqlTextFunction::Contains,
            args: vec![SqlExpr::Field(field), SqlExpr::Literal(Value::Null)],
        },
        Predicate::IsEmpty { field } => SqlExpr::FunctionCall {
            function: SqlTextFunction::Length,
            args: vec![SqlExpr::Field(field)],
        },
        Predicate::IsNotEmpty { field } => SqlExpr::Unary {
            op: super::SqlExprUnaryOp::Not,
            expr: Box::new(SqlExpr::FunctionCall {
                function: SqlTextFunction::Length,
                args: vec![SqlExpr::Field(field)],
            }),
        },
        Predicate::TextContains { field, value } => SqlExpr::FunctionCall {
            function: SqlTextFunction::Contains,
            args: vec![SqlExpr::Field(field), SqlExpr::Literal(value)],
        },
        Predicate::TextContainsCi { field, value } => SqlExpr::FunctionCall {
            function: SqlTextFunction::Contains,
            args: vec![
                SqlExpr::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Lower,
                    field,
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlExpr::Literal(value),
            ],
        },
    }
}

fn sql_expr_from_compare(compare: ComparePredicate) -> SqlExpr {
    match compare.op() {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = compare.value().clone() else {
                panic!("IN/NOT IN compare expects list literal in parser tests");
            };

            SqlExpr::Membership {
                expr: Box::new(SqlExpr::Field(compare.field().to_string())),
                values,
                negated: compare.op() == CompareOp::NotIn,
            }
        }
        CompareOp::StartsWith | CompareOp::EndsWith | CompareOp::Contains => {
            SqlExpr::FunctionCall {
                function: match compare.op() {
                    CompareOp::StartsWith => SqlTextFunction::StartsWith,
                    CompareOp::EndsWith => SqlTextFunction::EndsWith,
                    CompareOp::Contains => SqlTextFunction::Contains,
                    _ => unreachable!(),
                },
                args: vec![
                    if compare.coercion().id() == CoercionId::TextCasefold {
                        SqlExpr::TextFunction(SqlTextFunctionCall {
                            function: SqlTextFunction::Lower,
                            field: compare.field().to_string(),
                            literal: None,
                            literal2: None,
                            literal3: None,
                        })
                    } else {
                        SqlExpr::Field(compare.field().to_string())
                    },
                    SqlExpr::Literal(compare.value().clone()),
                ],
            }
        }
        op => SqlExpr::Binary {
            op: sql_binary_from_compare(op),
            left: Box::new(match compare.coercion().id() {
                CoercionId::TextCasefold => SqlExpr::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Lower,
                    field: compare.field().to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                _ => SqlExpr::Field(compare.field().to_string()),
            }),
            right: Box::new(SqlExpr::Literal(compare.value().clone())),
        },
    }
}

fn fold_predicate_children(children: Vec<Predicate>, op: SqlExprBinaryOp) -> SqlExpr {
    fold_exprs(
        children
            .into_iter()
            .map(sql_expr_from_runtime_predicate)
            .collect(),
        op,
    )
}

fn fold_exprs(mut exprs: Vec<SqlExpr>, op: SqlExprBinaryOp) -> SqlExpr {
    let first = exprs.remove(0);
    exprs
        .into_iter()
        .fold(first, |left, right| SqlExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
}

const fn sql_binary_from_compare(op: CompareOp) -> SqlExprBinaryOp {
    match op {
        CompareOp::Eq
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => SqlExprBinaryOp::Eq,
        CompareOp::Ne => SqlExprBinaryOp::Ne,
        CompareOp::Lt => SqlExprBinaryOp::Lt,
        CompareOp::Lte => SqlExprBinaryOp::Lte,
        CompareOp::Gt => SqlExprBinaryOp::Gt,
        CompareOp::Gte => SqlExprBinaryOp::Gte,
    }
}

#[test]
fn parse_select_statement_with_predicate_order_and_window() {
    let sql = "  SeLeCt DISTINCT name, COUNT(*) FROM users \
               WHERE age >= 21 AND active = TRUE \
               ORDER BY age DESC, name ASC LIMIT 10 OFFSET 5;  ";
    let statement = parse_sql(sql).expect("select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("name".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: option_sql_pred!(Predicate::And(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                )),
                Predicate::Compare(ComparePredicate::with_coercion(
                    "active",
                    CompareOp::Eq,
                    Value::Bool(true),
                    CoercionId::Strict,
                )),
            ])),
            distinct: true,
            group_by: vec![],
            having: vec![],
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("age"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("name"),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(10),
            offset: Some(5),
        }),
    );
}

#[test]
fn parse_select_statement_with_trim_ltrim_rtrim_lower_upper_and_length_projection_items() {
    let statement = parse_sql(
        "SELECT TRIM(name), LTRIM(name), RTRIM(name), LOWER(name), UPPER(name), LENGTH(name), age FROM users",
    )
    .expect("text-function projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Trim,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Ltrim,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Rtrim,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Lower,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Upper,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Length,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::Field("age".to_string()),
            ]),
            projection_aliases: vec![None, None, None, None, None, None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_scalar_add_projection_item() {
    let statement = parse_sql("SELECT age + 1 FROM users")
        .expect("scalar arithmetic projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Arithmetic(
                SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("age".to_string()),
                    op: SqlArithmeticProjectionOp::Add,
                    right: SqlProjectionOperand::Literal(Value::Int(1)),
                },
            )]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_scalar_sub_mul_div_projection_items() {
    for (sql, op, literal, context) in [
        (
            "SELECT age - 1 FROM users",
            SqlArithmeticProjectionOp::Sub,
            Value::Int(1),
            "subtraction projection",
        ),
        (
            "SELECT age * 2 FROM users",
            SqlArithmeticProjectionOp::Mul,
            Value::Int(2),
            "multiplication projection",
        ),
        (
            "SELECT age / 2 FROM users",
            SqlArithmeticProjectionOp::Div,
            Value::Int(2),
            "division projection",
        ),
    ] {
        let statement =
            parse_sql(sql).unwrap_or_else(|err| panic!("{context} should parse: {err:?}"));

        assert_eq!(
            statement,
            SqlStatement::Select(SqlSelectStatement {
                entity: "users".to_string(),
                projection: SqlProjection::Items(vec![SqlSelectItem::Arithmetic(
                    SqlArithmeticProjectionCall {
                        left: SqlProjectionOperand::Field("age".to_string()),
                        op,
                        right: SqlProjectionOperand::Literal(literal),
                    },
                )]),
                projection_aliases: vec![None],
                predicate: None,
                distinct: false,
                group_by: vec![],
                having: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            "{context} should lower to one bounded arithmetic projection item",
        );
    }
}

#[test]
fn parse_select_statement_with_scalar_field_to_field_projection_item() {
    let statement = parse_sql("SELECT dexterity + charisma AS total FROM users")
        .expect("field-to-field arithmetic projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Arithmetic(
                SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("dexterity".to_string()),
                    op: SqlArithmeticProjectionOp::Add,
                    right: SqlProjectionOperand::Field("charisma".to_string()),
                },
            )]),
            projection_aliases: vec![Some("total".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_chained_scalar_projection_item_preserves_precedence() {
    let statement = parse_sql("SELECT age + 1 * 2 FROM users")
        .expect("chained scalar arithmetic projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Arithmetic(
                SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("age".to_string()),
                    op: SqlArithmeticProjectionOp::Add,
                    right: SqlProjectionOperand::Arithmetic(Box::new(
                        SqlArithmeticProjectionCall {
                            left: SqlProjectionOperand::Literal(Value::Int(1)),
                            op: SqlArithmeticProjectionOp::Mul,
                            right: SqlProjectionOperand::Literal(Value::Int(2)),
                        },
                    )),
                },
            )]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        "chained scalar arithmetic projection should preserve operator precedence in the parser model",
    );
}

#[test]
fn parse_select_statement_with_parenthesized_round_projection_item() {
    let statement = parse_sql("SELECT ROUND((age + salary) / 2, 2) FROM users")
        .expect("parenthesized ROUND projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Round(SqlRoundProjectionCall {
                input: SqlRoundProjectionInput::Arithmetic(SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Arithmetic(Box::new(SqlArithmeticProjectionCall {
                        left: SqlProjectionOperand::Field("age".to_string()),
                        op: SqlArithmeticProjectionOp::Add,
                        right: SqlProjectionOperand::Field("salary".to_string()),
                    },)),
                    op: SqlArithmeticProjectionOp::Div,
                    right: SqlProjectionOperand::Literal(Value::Int(2)),
                }),
                scale: Value::Int(2),
            },)]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        "parenthesized ROUND projection should preserve nested arithmetic structure in the parser model",
    );
}

#[test]
fn parse_select_statement_with_searched_case_projection_item() {
    let statement =
        parse_sql("SELECT CASE WHEN age >= 21 THEN 'adult' ELSE 'minor' END AS cohort FROM users")
            .expect("searched CASE projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Expr(SqlExpr::Case {
                arms: vec![SqlCaseArm {
                    condition: SqlExpr::Binary {
                        op: SqlExprBinaryOp::Gte,
                        left: Box::new(SqlExpr::Field("age".to_string())),
                        right: Box::new(SqlExpr::Literal(Value::Int(21))),
                    },
                    result: SqlExpr::Literal(Value::Text("adult".to_string())),
                }],
                else_expr: Some(Box::new(SqlExpr::Literal(
                    Value::Text("minor".to_string(),)
                ))),
            })]),
            projection_aliases: vec![Some("cohort".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        "searched CASE projection should stay on the shared SQL-expression boundary",
    );
}

#[test]
fn parse_select_statement_with_searched_case_is_null_condition_projection_item() {
    let statement = parse_sql(
        "SELECT CASE WHEN guild_rank IS NULL THEN 'unguilded' ELSE guild_rank END AS guild_label FROM users",
    )
    .expect("searched CASE projection with IS NULL condition should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Expr(SqlExpr::Case {
                arms: vec![SqlCaseArm {
                    condition: SqlExpr::NullTest {
                        expr: Box::new(SqlExpr::Field("guild_rank".to_string())),
                        negated: false,
                    },
                    result: SqlExpr::Literal(Value::Text("unguilded".to_string())),
                }],
                else_expr: Some(Box::new(SqlExpr::Field("guild_rank".to_string()))),
            })]),
            projection_aliases: vec![Some("guild_label".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        "searched CASE projection should keep IS NULL conditions on the shared SQL-expression boundary",
    );
}

#[test]
fn parse_select_statement_with_searched_case_aggregate_input_expression() {
    let statement = parse_sql("SELECT SUM(CASE WHEN age >= 21 THEN 1 ELSE 0 END) FROM users")
        .expect("searched CASE aggregate-input select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Aggregate(SqlAggregateCall {
                kind: SqlAggregateKind::Sum,
                input: Some(Box::new(SqlAggregateInputExpr::Expr(SqlExpr::Case {
                    arms: vec![SqlCaseArm {
                        condition: SqlExpr::Binary {
                            op: SqlExprBinaryOp::Gte,
                            left: Box::new(SqlExpr::Field("age".to_string())),
                            right: Box::new(SqlExpr::Literal(Value::Int(21))),
                        },
                        result: SqlExpr::Literal(Value::Int(1)),
                    }],
                    else_expr: Some(Box::new(SqlExpr::Literal(Value::Int(0)))),
                }))),
                filter_expr: None,
                distinct: false,
            })]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        "searched CASE aggregate inputs should stay on the shared SQL-expression boundary",
    );
}

#[test]
fn parse_select_statement_with_searched_case_where_expression() {
    let statement = parse_sql(
        "SELECT name FROM users \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC",
    )
    .expect("searched CASE WHERE select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Field("name".to_string())]),
            projection_aliases: vec![None],
            predicate: Some(SqlExpr::Case {
                arms: vec![SqlCaseArm {
                    condition: SqlExpr::Binary {
                        op: SqlExprBinaryOp::Gte,
                        left: Box::new(SqlExpr::Field("age".to_string())),
                        right: Box::new(SqlExpr::Literal(Value::Int(30))),
                    },
                    result: SqlExpr::Literal(Value::Bool(true)),
                }],
                else_expr: Some(Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(20))),
                })),
            }),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: None,
            offset: None,
        }),
        "searched CASE WHERE should stay on the shared pre-aggregate SQL-expression boundary",
    );
}

#[test]
fn parse_select_statement_distinguishes_is_null_from_eq_null() {
    let is_null = parse_sql("SELECT * FROM users WHERE age IS NULL ORDER BY age ASC")
        .expect("IS NULL select statement should parse");
    let eq_null = parse_sql("SELECT * FROM users WHERE age = NULL ORDER BY age ASC")
        .expect("= NULL select statement should parse");

    let SqlStatement::Select(is_null) = is_null else {
        panic!("expected parsed IS NULL select statement");
    };
    let SqlStatement::Select(eq_null) = eq_null else {
        panic!("expected parsed = NULL select statement");
    };

    assert_eq!(
        is_null.predicate,
        Some(SqlExpr::NullTest {
            expr: Box::new(SqlExpr::Field("age".to_string())),
            negated: false,
        }),
        "IS NULL should preserve one dedicated null-test SQL expression node",
    );
    assert_eq!(
        eq_null.predicate,
        Some(SqlExpr::Binary {
            op: SqlExprBinaryOp::Eq,
            left: Box::new(SqlExpr::Field("age".to_string())),
            right: Box::new(SqlExpr::Literal(Value::Null)),
        }),
        "= NULL should stay one ordinary equality expression instead of collapsing into the IS NULL node",
    );
}

#[test]
fn parse_select_statement_with_field_to_field_predicate() {
    let statement =
        parse_sql("SELECT * FROM users WHERE age > rank AND name = label ORDER BY age ASC")
            .expect("field-to-field predicate select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: vec![],
            predicate: option_sql_pred!(Predicate::And(vec![
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "age",
                    CompareOp::Gt,
                    "rank",
                    CoercionId::NumericWiden,
                )),
                Predicate::eq_fields("name".to_string(), "label".to_string()),
            ])),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_symmetric_predicate_forms() {
    let statement =
        parse_sql("SELECT * FROM users WHERE 5 < age AND dexterity = strength ORDER BY age ASC")
            .expect("symmetric predicate forms should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: vec![],
            predicate: option_sql_pred!(Predicate::And(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gt,
                    Value::Int(5),
                    CoercionId::NumericWiden,
                )),
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "strength",
                    CompareOp::Eq,
                    "dexterity",
                    CoercionId::Strict,
                )),
            ])),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_round_projection_items() {
    for (sql, expected_item, context) in [
        (
            "SELECT ROUND(age, 2) FROM users",
            SqlSelectItem::Round(SqlRoundProjectionCall {
                input: SqlRoundProjectionInput::Operand(SqlProjectionOperand::Field(
                    "age".to_string(),
                )),
                scale: Value::Int(2),
            }),
            "round over plain field",
        ),
        (
            "SELECT ROUND(age / 3, 2) FROM users",
            SqlSelectItem::Round(SqlRoundProjectionCall {
                input: SqlRoundProjectionInput::Arithmetic(SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("age".to_string()),
                    op: SqlArithmeticProjectionOp::Div,
                    right: SqlProjectionOperand::Literal(Value::Int(3)),
                }),
                scale: Value::Int(2),
            }),
            "round over bounded arithmetic expression",
        ),
        (
            "SELECT ROUND(age + salary, 2) FROM users",
            SqlSelectItem::Round(SqlRoundProjectionCall {
                input: SqlRoundProjectionInput::Arithmetic(SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("age".to_string()),
                    op: SqlArithmeticProjectionOp::Add,
                    right: SqlProjectionOperand::Field("salary".to_string()),
                }),
                scale: Value::Int(2),
            }),
            "round over bounded field-to-field arithmetic expression",
        ),
    ] {
        let statement =
            parse_sql(sql).unwrap_or_else(|err| panic!("{context} should parse: {err:?}"));

        assert_eq!(
            statement,
            SqlStatement::Select(SqlSelectStatement {
                entity: "users".to_string(),
                projection: SqlProjection::Items(vec![expected_item]),
                projection_aliases: vec![None],
                predicate: None,
                distinct: false,
                group_by: vec![],
                having: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            "{context} should lower to one bounded ROUND projection item",
        );
    }
}

#[test]
fn parse_select_statement_with_scalar_field_plus_field_projection_item() {
    let statement = parse_sql("SELECT age + salary FROM users")
        .expect("field-plus-field projection should parse in the bounded projection slice");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Arithmetic(
                SqlArithmeticProjectionCall {
                    left: SqlProjectionOperand::Field("age".to_string()),
                    op: SqlArithmeticProjectionOp::Add,
                    right: SqlProjectionOperand::Field("salary".to_string()),
                },
            )]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_rejects_round_without_integer_scale() {
    let err = parse_sql("SELECT ROUND(age, name) FROM users")
        .expect_err("ROUND scale should remain literal-only in the bounded slice");

    assert!(matches!(err, SqlParseError::InvalidSyntax { .. }));
}

#[test]
fn parse_select_statement_accepts_arithmetic_predicates_for_shared_where_expr_lowering() {
    let statement = parse_sql("SELECT * FROM users WHERE age + 1 > 10")
        .expect("arithmetic WHERE predicates should parse through the shared SqlExpr seam");

    let SqlStatement::Select(statement) = statement else {
        panic!("expected SELECT statement");
    };

    assert!(
        matches!(
            statement.predicate,
            Some(SqlExpr::Binary {
                op: SqlExprBinaryOp::Gt,
                ..
            })
        ),
        "arithmetic WHERE predicate should stay parser-owned syntax and leave admission to lowering",
    );
}

#[test]
fn parse_select_statement_accepts_expression_predicate_near_misses_for_lowering_validation() {
    for sql in [
        "SELECT * FROM users WHERE strength = dexterity + 1",
        "SELECT * FROM users WHERE strength + dexterity = 10",
        "SELECT * FROM users WHERE ROUND(strength, 1) = dexterity",
    ] {
        let statement =
            parse_sql(sql).expect("WHERE near-miss expressions should parse and fail later");

        assert!(
            matches!(
                statement,
                SqlStatement::Select(SqlSelectStatement {
                    predicate: Some(_),
                    ..
                })
            ),
            "expression predicate near-miss should remain parser-owned syntax for lowering validation: {sql}",
        );
    }
}

#[test]
fn parse_select_statement_with_expression_order_terms() {
    let statement =
        parse_sql("SELECT * FROM users ORDER BY LOWER(name) DESC, UPPER(email) ASC LIMIT 2")
            .expect("expression order select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("LOWER(name)"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("UPPER(email)"),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(2),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_direct_bounded_computed_order_terms() {
    let statement = parse_sql(
        "SELECT * FROM users ORDER BY age + 1 ASC, age + salary DESC, ROUND(age / 3, 2) ASC LIMIT 2",
    )
    .expect("direct bounded computed ORDER BY terms should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("age + 1"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("age + salary"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("ROUND(age / 3, 2)"),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(2),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_supported_scalar_text_order_terms() {
    let statement = parse_sql(
        "SELECT * FROM users ORDER BY TRIM(name), LTRIM(name), RTRIM(name), LENGTH(name) DESC, LEFT(name, 2), POSITION('a', name) DESC",
    )
    .expect("supported scalar text ORDER BY terms should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: vec![],
            distinct: false,
            predicate: None,
            group_by: vec![],
            having: vec![],
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("TRIM(name)"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("LTRIM(name)"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("RTRIM(name)"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("LENGTH(name)"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("LEFT(name, 2)"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("POSITION('a', name)"),
                    direction: SqlOrderDirection::Desc,
                },
            ],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_left_and_right_projection_items() {
    let statement = parse_sql("SELECT LEFT(name, 2), RIGHT(name, 3) FROM users")
        .expect("left/right projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Left,
                    field: "name".to_string(),
                    literal: Some(Value::Int(2)),
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Right,
                    field: "name".to_string(),
                    literal: Some(Value::Int(3)),
                    literal2: None,
                    literal3: None,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_starts_ends_and_position_projection_items() {
    let statement = parse_sql(
        "SELECT STARTS_WITH(name, 'A'), ENDS_WITH(name, 'z'), CONTAINS(name, 'd'), POSITION('da', name) FROM users",
    )
    .expect("text predicate projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::StartsWith,
                    field: "name".to_string(),
                    literal: Some(Value::Text("A".to_string())),
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::EndsWith,
                    field: "name".to_string(),
                    literal: Some(Value::Text("z".to_string())),
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Contains,
                    field: "name".to_string(),
                    literal: Some(Value::Text("d".to_string())),
                    literal2: None,
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Position,
                    field: "name".to_string(),
                    literal: Some(Value::Text("da".to_string())),
                    literal2: None,
                    literal3: None,
                }),
            ]),
            projection_aliases: vec![None, None, None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_replace_projection_item() {
    let statement = parse_sql("SELECT REPLACE(name, 'A', 'E') FROM users")
        .expect("replace projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::TextFunction(
                SqlTextFunctionCall {
                    function: SqlTextFunction::Replace,
                    field: "name".to_string(),
                    literal: Some(Value::Text("A".to_string())),
                    literal2: Some(Value::Text("E".to_string())),
                    literal3: None,
                },
            )]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_substring_projection_item() {
    let statement = parse_sql("SELECT SUBSTRING(name, 2, 3), SUBSTRING(name, 2) FROM users")
        .expect("substring projection select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Substring,
                    field: "name".to_string(),
                    literal: Some(Value::Int(2)),
                    literal2: Some(Value::Int(3)),
                    literal3: None,
                }),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Substring,
                    field: "name".to_string(),
                    literal: Some(Value::Int(2)),
                    literal2: None,
                    literal3: None,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_delete_statement_with_limit() {
    let statement = parse_sql("DELETE FROM users WHERE age < 18 ORDER BY age LIMIT 3")
        .expect("delete statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Delete(SqlDeleteStatement {
            entity: "users".to_string(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(18),
                CoercionId::NumericWiden,
            ))),
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: None,
            returning: None,
        }),
    );
}

#[test]
fn parse_delete_statement_with_limit_and_offset() {
    let statement = parse_sql("DELETE FROM users WHERE age < 18 ORDER BY age LIMIT 3 OFFSET 1")
        .expect("delete statement with offset should parse");

    assert_eq!(
        statement,
        SqlStatement::Delete(SqlDeleteStatement {
            entity: "users".to_string(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(18),
                CoercionId::NumericWiden,
            ))),
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: Some(1),
            returning: None,
        }),
    );
}

#[test]
fn parse_delete_statement_accepts_single_table_alias() {
    let statement = parse_sql(
        "DELETE FROM users u WHERE u.age < 18 ORDER BY LOWER(u.name) ASC LIMIT 3 OFFSET 1",
    )
    .expect("delete statement with one table alias should parse");

    assert_eq!(
        statement,
        SqlStatement::Delete(SqlDeleteStatement {
            entity: "users".to_string(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(18),
                CoercionId::NumericWiden,
            ))),
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("LOWER(name)"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: Some(1),
            returning: None,
        }),
    );
}

#[test]
fn parse_delete_statement_with_direct_starts_with_family() {
    let cases = [
        (
            "DELETE FROM users WHERE STARTS_WITH(name, 'Al') ORDER BY id ASC LIMIT 1",
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ),
        (
            "DELETE FROM users WHERE STARTS_WITH(LOWER(name), 'Al') ORDER BY id ASC LIMIT 1",
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        ),
        (
            "DELETE FROM users WHERE STARTS_WITH(UPPER(name), 'AL') ORDER BY id ASC LIMIT 1",
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ),
    ];

    for (sql, literal, coercion) in cases {
        let statement = parse_sql(sql).expect("direct STARTS_WITH delete statement should parse");

        assert_eq!(
            statement,
            SqlStatement::Delete(SqlDeleteStatement {
                entity: "users".to_string(),
                predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::StartsWith,
                    literal,
                    coercion,
                ))),
                order_by: vec![SqlOrderTerm {
                    field: sql_order_expr("id"),
                    direction: SqlOrderDirection::Asc,
                }],
                limit: Some(1),
                offset: None,
                returning: None,
            }),
        );
    }
}

#[test]
fn parse_explain_json_wrapped_select() {
    let statement = parse_sql("EXPLAIN JSON SELECT * FROM users LIMIT 1")
        .expect("explain statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Explain(SqlExplainStatement {
            mode: SqlExplainMode::Json,
            statement: SqlExplainTarget::Select(SqlSelectStatement {
                entity: "users".to_string(),
                projection: SqlProjection::All,
                projection_aliases: Vec::default(),
                predicate: None,
                distinct: false,
                group_by: vec![],
                having: vec![],
                order_by: vec![],
                limit: Some(1),
                offset: None,
            }),
        }),
    );
}

#[test]
fn parse_explain_json_wrapped_delete_with_direct_starts_with_family() {
    let cases = [
        (
            "EXPLAIN JSON DELETE FROM users WHERE STARTS_WITH(name, 'Al') ORDER BY id ASC LIMIT 1",
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ),
        (
            "EXPLAIN JSON DELETE FROM users WHERE STARTS_WITH(LOWER(name), 'Al') ORDER BY id ASC LIMIT 1",
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        ),
        (
            "EXPLAIN JSON DELETE FROM users WHERE STARTS_WITH(UPPER(name), 'AL') ORDER BY id ASC LIMIT 1",
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ),
    ];

    for (sql, literal, coercion) in cases {
        let statement =
            parse_sql(sql).expect("EXPLAIN JSON direct STARTS_WITH delete should parse");

        assert_eq!(
            statement,
            SqlStatement::Explain(SqlExplainStatement {
                mode: SqlExplainMode::Json,
                statement: SqlExplainTarget::Delete(SqlDeleteStatement {
                    entity: "users".to_string(),
                    predicate: option_sql_pred!(Predicate::Compare(
                        ComparePredicate::with_coercion(
                            "name",
                            CompareOp::StartsWith,
                            literal,
                            coercion,
                        )
                    )),
                    order_by: vec![SqlOrderTerm {
                        field: sql_order_expr("id"),
                        direction: SqlOrderDirection::Asc,
                    }],
                    limit: Some(1),
                    offset: None,
                    returning: None,
                }),
            }),
        );
    }
}

#[test]
fn parse_describe_statement_with_schema_qualified_entity() {
    let statement = parse_sql("DESCRIBE public.users").expect("describe statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Describe(SqlDescribeStatement {
            entity: "public.users".to_string(),
        }),
    );
}

#[test]
fn parse_show_indexes_statement_with_schema_qualified_entity() {
    let statement =
        parse_sql("SHOW INDEXES public.users").expect("show indexes statement should parse");

    assert_eq!(
        statement,
        SqlStatement::ShowIndexes(SqlShowIndexesStatement {
            entity: "public.users".to_string(),
        }),
    );
}

#[test]
fn parse_show_columns_statement_with_schema_qualified_entity() {
    let statement =
        parse_sql("SHOW COLUMNS public.users").expect("show columns statement should parse");

    assert_eq!(
        statement,
        SqlStatement::ShowColumns(SqlShowColumnsStatement {
            entity: "public.users".to_string(),
        }),
    );
}

#[test]
fn parse_show_entities_statement() {
    let statement = parse_sql("SHOW ENTITIES").expect("show entities statement should parse");

    assert_eq!(
        statement,
        SqlStatement::ShowEntities(SqlShowEntitiesStatement)
    );
}

#[test]
fn parse_show_tables_statement() {
    let statement = parse_sql("SHOW TABLES").expect("show tables statement should parse");

    assert_eq!(
        statement,
        SqlStatement::ShowEntities(SqlShowEntitiesStatement)
    );
}

#[test]
fn parse_select_statement_with_qualified_identifiers() {
    let statement = parse_sql(
        "SELECT users.name, users.age \
         FROM public.users \
         WHERE users.age >= 21 \
         ORDER BY users.age DESC LIMIT 10 OFFSET 1",
    )
    .expect("qualified-identifier select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "public.users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("users.name".to_string()),
                SqlSelectItem::Field("users.age".to_string()),
            ]),
            projection_aliases: vec![None, None],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "users.age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("users.age"),
                direction: SqlOrderDirection::Desc,
            }],
            limit: Some(10),
            offset: Some(1),
        }),
    );
}

#[test]
fn parse_select_statement_with_strict_like_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE name LIKE 'Al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("strict LIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_angle_bracket_not_equal_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE active <> true \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("angle-bracket not-equal select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Ne,
                Value::Bool(true),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_in_trailing_comma_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE age IN (10, 20, 30,) \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("IN with trailing comma select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::In,
                Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_is_true_and_is_false_predicates() {
    let is_true = parse_sql(
        "SELECT * FROM users \
         WHERE active IS TRUE \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("IS TRUE select statement should parse");
    let is_false = parse_sql(
        "SELECT * FROM users \
         WHERE active IS FALSE \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("IS FALSE select statement should parse");

    assert_eq!(
        is_true,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
    assert_eq!(
        is_false,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(false),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_is_not_true_and_is_not_false_predicates() {
    let is_not_true = parse_sql(
        "SELECT * FROM users \
         WHERE active IS NOT TRUE \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("IS NOT TRUE select statement should parse");
    let is_not_false = parse_sql(
        "SELECT * FROM users \
         WHERE active IS NOT FALSE \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("IS NOT FALSE select statement should parse");

    assert_eq!(
        is_not_true,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Not(Box::new(Predicate::Compare(
                ComparePredicate::with_coercion(
                    "active",
                    CompareOp::Eq,
                    Value::Bool(true),
                    CoercionId::Strict,
                ),
            )))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
    assert_eq!(
        is_not_false,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Not(Box::new(Predicate::Compare(
                ComparePredicate::with_coercion(
                    "active",
                    CompareOp::Eq,
                    Value::Bool(false),
                    CoercionId::Strict,
                ),
            )))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_field_bound_between_and_not_between_predicates() {
    let between = parse_sql(
        "SELECT * FROM users \
         WHERE age BETWEEN min_age AND max_age \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("field-bound BETWEEN select statement should parse");
    let not_between = parse_sql(
        "SELECT * FROM users \
         WHERE age NOT BETWEEN min_age AND max_age \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("field-bound NOT BETWEEN select statement should parse");

    assert_eq!(
        between,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::And(vec![
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    "min_age",
                    CoercionId::NumericWiden,
                )),
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "age",
                    CompareOp::Lte,
                    "max_age",
                    CoercionId::NumericWiden,
                )),
            ])),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
    assert_eq!(
        not_between,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Or(vec![
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "age",
                    CompareOp::Lt,
                    "min_age",
                    CoercionId::NumericWiden,
                )),
                Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                    "age",
                    CompareOp::Gt,
                    "max_age",
                    CoercionId::NumericWiden,
                )),
            ])),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_strict_not_like_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE name NOT LIKE 'Al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("strict NOT LIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Not(Box::new(Predicate::Compare(
                ComparePredicate::with_coercion(
                    "name",
                    CompareOp::StartsWith,
                    Value::Text("Al".to_string()),
                    CoercionId::Strict,
                ),
            )))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_ilike_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE name ILIKE 'al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("ILIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("al".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_not_ilike_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE name NOT ILIKE 'al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("NOT ILIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Not(Box::new(Predicate::Compare(
                ComparePredicate::with_coercion(
                    "name",
                    CompareOp::StartsWith,
                    Value::Text("al".to_string()),
                    CoercionId::TextCasefold,
                ),
            )))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_strict_text_range_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE name >= 'Al' AND name < 'Am' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("strict text-range select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::And(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::Gte,
                    Value::Text("Al".to_string()),
                    CoercionId::Strict,
                )),
                Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::Lt,
                    Value::Text("Am".to_string()),
                    CoercionId::Strict,
                )),
            ])),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_direct_starts_with_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE STARTS_WITH(name, 'Al') \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("direct STARTS_WITH select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_direct_lower_starts_with_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE STARTS_WITH(LOWER(name), 'Al') \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("direct LOWER(field) STARTS_WITH select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_direct_upper_starts_with_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE STARTS_WITH(UPPER(name), 'AL') \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("direct UPPER(field) STARTS_WITH select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_lower_like_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE LOWER(name) LIKE 'Al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("LOWER(field) LIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_lower_not_like_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE LOWER(name) NOT LIKE 'Al%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("LOWER(field) NOT LIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Not(Box::new(Predicate::Compare(
                ComparePredicate::with_coercion(
                    "name",
                    CompareOp::StartsWith,
                    Value::Text("Al".to_string()),
                    CoercionId::TextCasefold,
                ),
            )))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_with_upper_like_prefix_predicate() {
    let statement = parse_sql(
        "SELECT * FROM users \
         WHERE UPPER(name) LIKE 'AL%' \
         ORDER BY id ASC LIMIT 1",
    )
    .expect("UPPER(field) LIKE prefix select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("id"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_rejects_like_non_prefix_pattern() {
    let cases = [
        "SELECT * FROM users WHERE name LIKE '%Al'",
        "SELECT * FROM users WHERE LOWER(name) LIKE '%Al'",
        "SELECT * FROM users WHERE UPPER(name) LIKE '%Al'",
    ];

    for sql in cases {
        let err = parse_sql(sql).expect_err("non-prefix LIKE pattern should fail closed");
        assert_eq!(
            err,
            super::SqlParseError::UnsupportedFeature {
                feature: "LIKE patterns beyond trailing '%' prefix form"
            }
        );
    }
}

#[test]
fn parse_select_grouped_statement_with_qualified_identifiers() {
    let statement = parse_sql(
        "SELECT users.age, COUNT(*) \
         FROM public.users \
         WHERE users.age >= 21 \
         GROUP BY users.age \
         ORDER BY users.age DESC LIMIT 5 OFFSET 1",
    )
    .expect("qualified-identifier grouped select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "public.users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("users.age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "users.age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec!["users.age".to_string()],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("users.age"),
                direction: SqlOrderDirection::Desc,
            }],
            limit: Some(5),
            offset: Some(1),
        }),
    );
}

#[test]
fn parse_explain_execution_with_qualified_identifiers() {
    let statement = parse_sql(
        "EXPLAIN EXECUTION SELECT users.name FROM public.users \
         WHERE users.age >= 21 ORDER BY users.age DESC LIMIT 1",
    )
    .expect("qualified-identifier explain statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Explain(SqlExplainStatement {
            mode: SqlExplainMode::Execution,
            statement: SqlExplainTarget::Select(SqlSelectStatement {
                entity: "public.users".to_string(),
                projection: SqlProjection::Items(vec![SqlSelectItem::Field(
                    "users.name".to_string(),
                )]),
                projection_aliases: vec![None],
                predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                    "users.age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                ))),
                distinct: false,
                group_by: vec![],
                having: vec![],
                order_by: vec![SqlOrderTerm {
                    field: sql_order_expr("users.age"),
                    direction: SqlOrderDirection::Desc,
                }],
                limit: Some(1),
                offset: None,
            }),
        }),
    );
}

#[test]
fn parse_select_grouped_statement_with_having_clauses() {
    let statement = parse_sql(
        "SELECT age, COUNT(*) \
         FROM users \
         GROUP BY age \
         HAVING age >= 21 AND COUNT(*) > 1 \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped HAVING select statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gte,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(21))),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gt,
                    left: Box::new(SqlExpr::Aggregate(SqlAggregateCall {
                        kind: SqlAggregateKind::Count,
                        input: None,
                        filter_expr: None,
                        distinct: false,
                    })),
                    right: Box::new(SqlExpr::Literal(Value::Int(1))),
                }),
            }],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(10),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_grouped_statement_with_having_is_null_and_is_not_null_clauses() {
    let statement = parse_sql(
        "SELECT age, COUNT(*) \
         FROM users \
         GROUP BY age \
         HAVING age IS NOT NULL AND COUNT(*) IS NULL \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped HAVING IS [NOT] NULL clauses should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::NullTest {
                    expr: Box::new(SqlExpr::Field("age".to_string())),
                    negated: true,
                }),
                right: Box::new(SqlExpr::NullTest {
                    expr: Box::new(SqlExpr::Aggregate(SqlAggregateCall {
                        kind: SqlAggregateKind::Count,
                        input: None,
                        filter_expr: None,
                        distinct: false,
                    })),
                    negated: false,
                }),
            }],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(10),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_grouped_statement_with_post_aggregate_having_exprs() {
    let statement = parse_sql(
        "SELECT class_name, AVG(strength) \
         FROM character \
         GROUP BY class_name \
         HAVING ROUND(AVG(strength), 2) >= 10 AND COUNT(*) + 1 > 5 \
         ORDER BY class_name ASC LIMIT 100",
    )
    .expect("grouped post-aggregate HAVING expressions should parse");

    let SqlStatement::Select(statement) = statement else {
        panic!("expected grouped SELECT statement");
    };

    assert_eq!(statement.having.len(), 1);
}

#[test]
fn parse_select_grouped_statement_with_searched_case_having_exprs() {
    let statement = parse_sql(
        "SELECT age, COUNT(*) \
         FROM users \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1 \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped searched CASE HAVING expressions should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::Case {
                    arms: vec![SqlCaseArm {
                        condition: SqlExpr::Binary {
                            op: SqlExprBinaryOp::Gt,
                            left: Box::new(SqlExpr::Aggregate(SqlAggregateCall {
                                kind: SqlAggregateKind::Count,
                                input: None,
                                filter_expr: None,
                                distinct: false,
                            })),
                            right: Box::new(SqlExpr::Literal(Value::Int(1))),
                        },
                        result: SqlExpr::Literal(Value::Int(1)),
                    }],
                    else_expr: Some(Box::new(SqlExpr::Literal(Value::Int(0)))),
                }),
                right: Box::new(SqlExpr::Literal(Value::Int(1))),
            }],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(10),
            offset: None,
        }),
        "searched CASE HAVING values should stay on the shared post-aggregate SQL-expression boundary",
    );
}

#[test]
fn parse_select_grouped_statement_with_aggregate_order_terms() {
    let statement = parse_sql(
        "SELECT age, AVG(score) \
         FROM users \
         GROUP BY age \
         ORDER BY AVG(score) DESC, ROUND(AVG(score), 2) ASC, age ASC \
         LIMIT 10",
    )
    .expect("grouped aggregate ORDER BY terms should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("age".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Avg,
                    input: Some(Box::new(SqlAggregateInputExpr::Field("score".to_string()))),
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![],
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("AVG(score)"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("ROUND(AVG(score), 2)"),
                    direction: SqlOrderDirection::Asc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("age"),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(10),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_grouped_statement_accepts_having_is_true_for_post_aggregate_lowering() {
    let statement = parse_sql(
        "SELECT age, COUNT(*) \
         FROM users \
         GROUP BY age \
         HAVING COUNT(*) IS TRUE \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped HAVING IS TRUE should parse through the shared post-aggregate seam");

    assert!(
        matches!(
            statement,
            SqlStatement::Select(SqlSelectStatement {
                having,
                ..
            }) if matches!(
                having.as_slice(),
                [SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left,
                    right
                }] if matches!(left.as_ref(), SqlExpr::Aggregate(_))
                    && matches!(right.as_ref(), SqlExpr::Literal(Value::Bool(true)))
            )
        ),
        "grouped HAVING IS TRUE should stay parser-owned syntax and defer semantic typing to lowering",
    );
}

#[test]
fn parse_sql_rejects_select_limit_before_order_with_actionable_message() {
    let err = parse_sql("SELECT * FROM users LIMIT 1 ORDER BY id")
        .expect_err("out-of-order LIMIT/ORDER clause should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::InvalidSyntax {
            message: "ORDER BY must appear before LIMIT/OFFSET".to_string()
        }
    );
}

#[test]
fn parse_sql_rejects_select_offset_before_order_with_actionable_message() {
    let err = parse_sql("SELECT * FROM users OFFSET 1 ORDER BY id")
        .expect_err("out-of-order OFFSET/ORDER clause should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::InvalidSyntax {
            message: "ORDER BY must appear before LIMIT/OFFSET".to_string()
        }
    );
}

#[test]
fn parse_sql_rejects_delete_limit_before_order_with_actionable_message() {
    let err = parse_sql("DELETE FROM users LIMIT 1 ORDER BY id")
        .expect_err("out-of-order DELETE LIMIT/ORDER clause should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::InvalidSyntax {
            message: "ORDER BY must appear before LIMIT in DELETE".to_string()
        }
    );
}

#[test]
fn parse_insert_statement_with_explicit_columns_and_values() {
    let statement = parse_sql("INSERT INTO users (id, name, age) VALUES (7, 'Ada', 21)")
        .expect("insert statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            source: SqlInsertSource::Values(vec![vec![
                Value::Int(7),
                Value::Text("Ada".to_string()),
                Value::Int(21),
            ]]),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_with_multiple_values_tuples() {
    let statement =
        parse_sql("INSERT INTO users (id, name, age) VALUES (7, 'Ada', 21), (8, 'Bea', 22)")
            .expect("multi-row insert statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            source: SqlInsertSource::Values(vec![
                vec![
                    Value::Int(7),
                    Value::Text("Ada".to_string()),
                    Value::Int(21)
                ],
                vec![
                    Value::Int(8),
                    Value::Text("Bea".to_string()),
                    Value::Int(22)
                ],
            ]),
            returning: None,
        }),
    );
}

#[test]
fn parse_update_statement_with_assignments_and_predicate() {
    let statement = parse_sql("UPDATE users SET name = 'Ada', age = 21 WHERE id = 7")
        .expect("update statement should parse");

    assert_eq!(
        statement,
        SqlStatement::Update(SqlUpdateStatement {
            entity: "users".to_string(),
            assignments: vec![
                SqlAssignment {
                    field: "name".to_string(),
                    value: Value::Text("Ada".to_string()),
                },
                SqlAssignment {
                    field: "age".to_string(),
                    value: Value::Int(21),
                },
            ],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Eq,
                Value::Int(7),
                CoercionId::Strict,
            ))),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            returning: None,
        }),
    );
}

#[test]
fn parse_update_statement_accepts_single_table_alias() {
    let statement = parse_sql("UPDATE users u SET u.name = 'Ada', u.age = 21 WHERE u.id = 7")
        .expect("update statement with one table alias should parse");

    assert_eq!(
        statement,
        SqlStatement::Update(SqlUpdateStatement {
            entity: "users".to_string(),
            assignments: vec![
                SqlAssignment {
                    field: "name".to_string(),
                    value: Value::Text("Ada".to_string()),
                },
                SqlAssignment {
                    field: "age".to_string(),
                    value: Value::Int(21),
                },
            ],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Eq,
                Value::Int(7),
                CoercionId::Strict,
            ))),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            returning: None,
        }),
    );
}

#[test]
fn parse_update_statement_with_order_limit_and_offset() {
    let statement = parse_sql(
        "UPDATE users SET age = 22 WHERE active = true ORDER BY age DESC, id ASC LIMIT 2 OFFSET 1",
    )
    .expect("update statement with ordered window should parse");

    assert_eq!(
        statement,
        SqlStatement::Update(SqlUpdateStatement {
            entity: "users".to_string(),
            assignments: vec![SqlAssignment {
                field: "age".to_string(),
                value: Value::Int(22),
            }],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            ))),
            order_by: vec![
                SqlOrderTerm {
                    field: sql_order_expr("age"),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: sql_order_expr("id"),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(2),
            offset: Some(1),
            returning: None,
        }),
    );
}

#[test]
fn parse_update_statement_rejects_invalid_window_clause_order() {
    let cases = [
        (
            "UPDATE users SET age = 22 WHERE id = 7 LIMIT 1 ORDER BY id",
            "ORDER BY must appear before LIMIT/OFFSET in UPDATE",
        ),
        (
            "UPDATE users SET age = 22 WHERE id = 7 OFFSET 1 LIMIT 1",
            "LIMIT must appear before OFFSET in UPDATE",
        ),
    ];

    for (sql, message) in cases {
        let err = parse_sql(sql).expect_err("invalid UPDATE window clause order should fail");
        assert_eq!(
            err,
            SqlParseError::InvalidSyntax {
                message: message.to_string(),
            },
            "invalid UPDATE window clause order should preserve an actionable parser message",
        );
    }
}

#[test]
fn parse_insert_statement_with_returning_field_list_parses() {
    let statement = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Ada') RETURNING id, name")
        .expect("INSERT RETURNING field list should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            source: SqlInsertSource::Values(vec![vec![
                Value::Int(1),
                Value::Text("Ada".to_string())
            ]]),
            returning: Some(SqlReturningProjection::Fields(vec![
                "id".to_string(),
                "name".to_string(),
            ])),
        }),
    );
}

#[test]
fn parse_update_statement_with_returning_star_parses() {
    let statement =
        parse_sql("UPDATE users alias SET alias.name = 'Ada' WHERE alias.id = 1 RETURNING *")
            .expect("UPDATE RETURNING star should parse");

    assert_eq!(
        statement,
        SqlStatement::Update(SqlUpdateStatement {
            entity: "users".to_string(),
            assignments: vec![SqlAssignment {
                field: "name".to_string(),
                value: Value::Text("Ada".to_string()),
            }],
            predicate: option_sql_pred!(Predicate::eq("id".to_string(), Value::Int(1))),
            order_by: vec![],
            limit: None,
            offset: None,
            returning: Some(SqlReturningProjection::All),
        }),
    );
}

#[test]
fn parse_delete_statement_with_returning_field_list_parses() {
    let statement =
        parse_sql("DELETE FROM users alias WHERE alias.id = 1 RETURNING alias.id, alias.name")
            .expect("DELETE RETURNING field list should parse");

    assert_eq!(
        statement,
        SqlStatement::Delete(SqlDeleteStatement {
            entity: "users".to_string(),
            predicate: option_sql_pred!(Predicate::eq("id".to_string(), Value::Int(1),)),
            order_by: vec![],
            limit: None,
            offset: None,
            returning: Some(SqlReturningProjection::Fields(vec![
                "id".to_string(),
                "name".to_string(),
            ])),
        }),
    );
}

#[test]
fn parse_delete_statement_with_returning_star_parses() {
    let statement = parse_sql("DELETE FROM users WHERE id = 1 RETURNING *")
        .expect("DELETE RETURNING star should parse");

    assert_eq!(
        statement,
        SqlStatement::Delete(SqlDeleteStatement {
            entity: "users".to_string(),
            predicate: option_sql_pred!(Predicate::eq("id".to_string(), Value::Int(1),)),
            order_by: vec![],
            limit: None,
            offset: None,
            returning: Some(SqlReturningProjection::All),
        }),
    );
}

#[test]
fn parse_insert_statement_without_column_list_parses() {
    let statement =
        parse_sql("INSERT INTO users VALUES (1)").expect("insert without column list should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec![],
            source: SqlInsertSource::Values(vec![vec![Value::Int(1)]]),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_with_field_only_select_source_parses() {
    let statement = parse_sql(
        "INSERT INTO users (name, age) SELECT name, age FROM users WHERE age >= 21 ORDER BY id ASC LIMIT 1",
    )
    .expect("insert-select should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            source: SqlInsertSource::Select(Box::new(SqlSelectStatement {
                entity: "users".to_string(),
                projection: SqlProjection::Items(vec![
                    SqlSelectItem::Field("name".to_string()),
                    SqlSelectItem::Field("age".to_string()),
                ]),
                projection_aliases: vec![None, None],
                predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                ))),
                distinct: false,
                group_by: Vec::new(),
                having: Vec::new(),
                order_by: vec![SqlOrderTerm {
                    field: sql_order_expr("id"),
                    direction: SqlOrderDirection::Asc,
                }],
                limit: Some(1),
                offset: None,
            })),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_with_computed_select_source_parses() {
    let statement = parse_sql(
        "INSERT INTO users (name, age) \
         SELECT LOWER(name), age FROM users WHERE age >= 21 ORDER BY id ASC LIMIT 1",
    )
    .expect("insert-select with one admitted computed source projection should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            source: SqlInsertSource::Select(Box::new(SqlSelectStatement {
                entity: "users".to_string(),
                projection: SqlProjection::Items(vec![
                    SqlSelectItem::TextFunction(SqlTextFunctionCall {
                        function: SqlTextFunction::Lower,
                        field: "name".to_string(),
                        literal: None,
                        literal2: None,
                        literal3: None,
                    }),
                    SqlSelectItem::Field("age".to_string()),
                ]),
                projection_aliases: vec![None, None],
                predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                ))),
                distinct: false,
                group_by: Vec::new(),
                having: Vec::new(),
                order_by: vec![SqlOrderTerm {
                    field: sql_order_expr("id"),
                    direction: SqlOrderDirection::Asc,
                }],
                limit: Some(1),
                offset: None,
            })),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_accepts_single_table_alias() {
    let statement = parse_sql("INSERT INTO users u (id, name) VALUES (1, 'Ada')")
        .expect("insert table alias should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            source: SqlInsertSource::Values(vec![vec![
                Value::Int(1),
                Value::Text("Ada".to_string()),
            ]]),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_accepts_as_table_alias_without_column_list() {
    let statement = parse_sql("INSERT INTO users AS u VALUES (1)")
        .expect("insert AS table alias without column list should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec![],
            source: SqlInsertSource::Values(vec![vec![Value::Int(1)]]),
            returning: None,
        }),
    );
}

#[test]
fn parse_insert_statement_rejects_tuple_length_mismatch_in_any_values_tuple() {
    let err = parse_sql("INSERT INTO users (id, name, age) VALUES (7, 'Ada', 21), (8, 'Bea')")
        .expect_err("multi-row insert with tuple length mismatch should stay fail-closed");

    assert_eq!(
        err,
        super::SqlParseError::InvalidSyntax {
            message: "INSERT column list and VALUES tuple length must match".to_string(),
        }
    );
}

#[test]
fn parse_insert_statement_without_column_list_accepts_multiple_values_tuples() {
    let statement = parse_sql("INSERT INTO users VALUES (1, 'Ada', 21), (2, 'Bea', 22)")
        .expect("multi-row insert without column list should parse");

    assert_eq!(
        statement,
        SqlStatement::Insert(SqlInsertStatement {
            entity: "users".to_string(),
            columns: vec![],
            source: SqlInsertSource::Values(vec![
                vec![
                    Value::Int(1),
                    Value::Text("Ada".to_string()),
                    Value::Int(21)
                ],
                vec![
                    Value::Int(2),
                    Value::Text("Bea".to_string()),
                    Value::Int(22)
                ],
            ]),
            returning: None,
        }),
    );
}

#[test]
fn parse_sql_unsupported_feature_labels_are_stable() {
    let cases = [
        (
            "SELECT * FROM users JOIN other ON users.id = other.id",
            "JOIN",
        ),
        (
            "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
            "WITH",
        ),
        (
            "SELECT * FROM users UNION SELECT * FROM users",
            "UNION/INTERSECT/EXCEPT",
        ),
        (
            "SELECT * FROM users INTERSECT SELECT * FROM users",
            "UNION/INTERSECT/EXCEPT",
        ),
        (
            "SELECT * FROM users EXCEPT SELECT * FROM users",
            "UNION/INTERSECT/EXCEPT",
        ),
        ("EXPLAIN INSERT INTO users VALUES (1)", "INSERT"),
        (
            "SELECT * FROM users; SELECT * FROM users",
            "multi-statement SQL input",
        ),
        ("SELECT \"name\" FROM users", "quoted identifiers"),
        (
            "SELECT len(name) FROM users",
            "SQL function namespace beyond supported aggregate or scalar text projection forms",
        ),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY age DESC) FROM users",
            "window functions / OVER",
        ),
        (
            "INSERT INTO users (id, name) VALUES (1, 'Ada') RETURNING LOWER(name)",
            "SQL function namespace beyond supported aggregate or scalar text projection forms",
        ),
        ("DESCRIBE users WHERE age > 1", "DESCRIBE modifiers"),
        ("EXPLAIN DESCRIBE users", "DESCRIBE modifiers"),
        (
            "SHOW DATABASES",
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES/SHOW TABLES",
        ),
        (
            "SELECT * FROM users WHERE LOWER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM users WHERE UPPER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM users WHERE STARTS_WITH(TRIM(name), 'Al')",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        ),
        (
            "DELETE FROM users WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY id ASC LIMIT 1",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        ),
        (
            "EXPLAIN JSON DELETE FROM users WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY id ASC LIMIT 1",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        ),
        ("SHOW INDEXES users WHERE age > 1", "SHOW INDEXES modifiers"),
        ("SHOW COLUMNS users WHERE age > 1", "SHOW COLUMNS modifiers"),
        ("SHOW ENTITIES users", "SHOW ENTITIES modifiers"),
    ];

    for (sql, expected_feature) in cases {
        let err = parse_sql(sql).expect_err("unsupported SQL feature should fail closed");
        assert_eq!(
            err,
            super::SqlParseError::UnsupportedFeature {
                feature: expected_feature
            },
            "unsupported feature label should stay stable for SQL: {sql}",
        );
    }
}

#[test]
fn parse_select_statement_rejects_simple_case_expressions() {
    let err = parse_sql("SELECT CASE age WHEN 21 THEN 'adult' ELSE 'minor' END FROM users")
        .expect_err("simple CASE expressions should stay fail-closed");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "simple CASE expressions"
        }
    );
}

#[test]
fn parse_sql_accepts_projection_aliases() {
    let statement = parse_sql(
        "SELECT name AS display_name, COUNT(*) total FROM users GROUP BY name ORDER BY name ASC",
    )
    .expect("projection aliases should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("name".to_string()),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: None,
                    filter_expr: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![Some("display_name".to_string()), Some("total".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec!["name".to_string()],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("name"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_accepts_bare_projection_aliases() {
    let statement =
        parse_sql("SELECT TRIM(name) trimmed_name FROM users").expect("bare aliases should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::TextFunction(
                SqlTextFunctionCall {
                    function: SqlTextFunction::Trim,
                    field: "name".to_string(),
                    literal: None,
                    literal2: None,
                    literal3: None,
                },
            )]),
            projection_aliases: vec![Some("trimmed_name".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_rejects_multi_statement_input() {
    let err = parse_sql("SELECT * FROM users; SELECT * FROM users")
        .expect_err("multi-statement SQL input should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "multi-statement SQL input"
        }
    );
}

#[test]
fn parse_sql_rejects_unknown_function_namespace() {
    let err = parse_sql("SELECT len(name) FROM users")
        .expect_err("unknown SQL function namespace should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "SQL function namespace beyond supported aggregate or scalar text projection forms"
        }
    );
}

#[test]
fn parse_sql_accepts_distinct_aggregate_qualifier() {
    let statement = parse_sql("SELECT COUNT(DISTINCT age) FROM users")
        .expect("aggregate DISTINCT qualifier should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Aggregate(SqlAggregateCall {
                kind: SqlAggregateKind::Count,
                input: Some(Box::new(SqlAggregateInputExpr::Field("age".to_string()))),
                filter_expr: None,
                distinct: true,
            })]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_accepts_aggregate_filter_clauses() {
    let statement = parse_sql("SELECT COUNT(*) FILTER (WHERE age > 1) FROM users")
        .expect("aggregate FILTER clause should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Aggregate(SqlAggregateCall {
                kind: SqlAggregateKind::Count,
                input: None,
                filter_expr: Some(Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gt,
                    left: Box::new(SqlExpr::Field("age".to_string())),
                    right: Box::new(SqlExpr::Literal(Value::Int(1))),
                })),
                distinct: false,
            })]),
            projection_aliases: vec![None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_rejects_aggregate_filter_window_pairing() {
    let err =
        parse_sql("SELECT COUNT(*) FILTER (WHERE age > 1) OVER (ORDER BY age DESC) FROM users")
            .expect_err("aggregate FILTER + OVER should stay fail-closed");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "window functions / OVER"
        }
    );
}

#[test]
fn parse_sql_accepts_expression_aggregate_inputs() {
    let statement = parse_sql("SELECT AVG(age + 1), COUNT(1), ROUND(AVG(age + 1), 2) FROM users")
        .expect("expression aggregate inputs should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Avg,
                    input: Some(Box::new(SqlAggregateInputExpr::Arithmetic(
                        SqlArithmeticProjectionCall {
                            left: SqlProjectionOperand::Field("age".to_string()),
                            op: SqlArithmeticProjectionOp::Add,
                            right: SqlProjectionOperand::Literal(Value::Int(1)),
                        },
                    ))),
                    filter_expr: None,
                    distinct: false,
                }),
                SqlSelectItem::Aggregate(SqlAggregateCall {
                    kind: SqlAggregateKind::Count,
                    input: Some(Box::new(SqlAggregateInputExpr::Literal(Value::Int(1)))),
                    filter_expr: None,
                    distinct: false,
                }),
                SqlSelectItem::Round(SqlRoundProjectionCall {
                    input: SqlRoundProjectionInput::Operand(SqlProjectionOperand::Aggregate(
                        SqlAggregateCall {
                            kind: SqlAggregateKind::Avg,
                            input: Some(Box::new(SqlAggregateInputExpr::Arithmetic(
                                SqlArithmeticProjectionCall {
                                    left: SqlProjectionOperand::Field("age".to_string()),
                                    op: SqlArithmeticProjectionOp::Add,
                                    right: SqlProjectionOperand::Literal(Value::Int(1)),
                                },
                            ))),
                            filter_expr: None,
                            distinct: false,
                        },
                    )),
                    scale: Value::Int(2),
                }),
            ]),
            projection_aliases: vec![None, None, None],
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_accepts_table_alias_identifier_form() {
    let statement = parse_sql("SELECT * FROM users u").expect("single-table alias should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::All,
            projection_aliases: Vec::default(),
            predicate: None,
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_accepts_table_alias_as_form() {
    let statement = parse_sql(
        "SELECT u.name FROM users AS u WHERE u.age >= 21 ORDER BY LOWER(u.name) ASC LIMIT 1",
    )
    .expect("single-table AS alias should parse");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "users".to_string(),
            projection: SqlProjection::Items(vec![SqlSelectItem::Field("name".to_string())]),
            projection_aliases: vec![None],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("LOWER(name)"),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(1),
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_accepts_table_alias_for_schema_qualified_entity() {
    let statement = parse_sql(
        "SELECT u.name, u.age FROM public.users AS u WHERE u.age >= 21 ORDER BY u.age DESC",
    )
    .expect("single-table alias should parse for schema-qualified entity names");

    assert_eq!(
        statement,
        SqlStatement::Select(SqlSelectStatement {
            entity: "public.users".to_string(),
            projection: SqlProjection::Items(vec![
                SqlSelectItem::Field("name".to_string()),
                SqlSelectItem::Field("age".to_string()),
            ]),
            projection_aliases: vec![None, None],
            predicate: option_sql_pred!(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: sql_order_expr("age"),
                direction: SqlOrderDirection::Desc,
            }],
            limit: None,
            offset: None,
        }),
    );
}

#[test]
fn parse_sql_rejects_quoted_identifier_syntax() {
    let err = parse_sql("SELECT \"name\" FROM users")
        .expect_err("quoted identifiers should be rejected in reduced parser");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "quoted identifiers"
        }
    );
}

#[test]
fn parse_sql_normalization_is_case_and_whitespace_insensitive() {
    let canonical = parse_sql("SELECT name FROM users WHERE active = true ORDER BY name LIMIT 5")
        .expect("canonical statement should parse");
    let variant =
        parse_sql("  select   name  from users where active = TRUE  order by name  limit 5 ; ")
            .expect("variant statement should parse");

    assert_eq!(canonical, variant);
}
