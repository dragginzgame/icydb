//! Module: db::sql::parser::tests
//! Covers SQL parser behavior, error classification, and syntax edge cases.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlDescribeStatement, SqlExplainMode,
    SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol, SqlOrderDirection,
    SqlOrderTerm, SqlParseError, SqlProjection, SqlSelectItem, SqlSelectStatement,
    SqlShowColumnsStatement, SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement,
    SqlTextFunction, SqlTextFunctionCall, parse_sql,
};
use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    value::Value,
};

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
                    field: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: Some(Predicate::And(vec![
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
                    field: "age".to_string(),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: "name".to_string(),
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
                    field: "LOWER(name)".to_string(),
                    direction: SqlOrderDirection::Desc,
                },
                SqlOrderTerm {
                    field: "UPPER(email)".to_string(),
                    direction: SqlOrderDirection::Asc,
                },
            ],
            limit: Some(2),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_statement_rejects_unsupported_expression_order_terms() {
    let err = parse_sql("SELECT * FROM users ORDER BY TRIM(name)")
        .expect_err("unsupported ORDER BY function must fail closed");

    assert_eq!(
        err,
        SqlParseError::UnsupportedFeature {
            feature: "ORDER BY functions beyond supported LOWER(...) or UPPER(...) forms",
        },
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(18),
                CoercionId::NumericWiden,
            ))),
            order_by: vec![SqlOrderTerm {
                field: "age".to_string(),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: None,
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(18),
                CoercionId::NumericWiden,
            ))),
            order_by: vec![SqlOrderTerm {
                field: "age".to_string(),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(3),
            offset: Some(1),
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
                predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::StartsWith,
                    literal,
                    coercion,
                ))),
                order_by: vec![SqlOrderTerm {
                    field: "id".to_string(),
                    direction: SqlOrderDirection::Asc,
                }],
                limit: Some(1),
                offset: None,
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
                    predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                        "name",
                        CompareOp::StartsWith,
                        literal,
                        coercion,
                    ))),
                    order_by: vec![SqlOrderTerm {
                        field: "id".to_string(),
                        direction: SqlOrderDirection::Asc,
                    }],
                    limit: Some(1),
                    offset: None,
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "users.age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "users.age".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
            predicate: Some(Predicate::And(vec![
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
                field: "id".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            ))),
            distinct: false,
            group_by: vec![],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "id".to_string(),
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
                    field: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "users.age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            ))),
            distinct: false,
            group_by: vec!["users.age".to_string()],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "users.age".to_string(),
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
                predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                    "users.age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                ))),
                distinct: false,
                group_by: vec![],
                having: vec![],
                order_by: vec![SqlOrderTerm {
                    field: "users.age".to_string(),
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
                    field: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![
                SqlHavingClause {
                    symbol: SqlHavingSymbol::Field("age".to_string()),
                    op: CompareOp::Gte,
                    value: Value::Int(21),
                },
                SqlHavingClause {
                    symbol: SqlHavingSymbol::Aggregate(SqlAggregateCall {
                        kind: SqlAggregateKind::Count,
                        field: None,
                        distinct: false,
                    }),
                    op: CompareOp::Gt,
                    value: Value::Int(1),
                },
            ],
            order_by: vec![SqlOrderTerm {
                field: "age".to_string(),
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
                    field: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![None, None],
            predicate: None,
            distinct: false,
            group_by: vec!["age".to_string()],
            having: vec![
                SqlHavingClause {
                    symbol: SqlHavingSymbol::Field("age".to_string()),
                    op: CompareOp::Ne,
                    value: Value::Null,
                },
                SqlHavingClause {
                    symbol: SqlHavingSymbol::Aggregate(SqlAggregateCall {
                        kind: SqlAggregateKind::Count,
                        field: None,
                        distinct: false,
                    }),
                    op: CompareOp::Eq,
                    value: Value::Null,
                },
            ],
            order_by: vec![SqlOrderTerm {
                field: "age".to_string(),
                direction: SqlOrderDirection::Asc,
            }],
            limit: Some(10),
            offset: None,
        }),
    );
}

#[test]
fn parse_select_grouped_statement_rejects_having_is_true() {
    let err = parse_sql(
        "SELECT age, COUNT(*) \
         FROM users \
         GROUP BY age \
         HAVING COUNT(*) IS TRUE \
         ORDER BY age ASC LIMIT 10",
    )
    .expect_err("grouped HAVING IS TRUE should fail closed");

    assert_eq!(
        err,
        super::SqlParseError::InvalidSyntax {
            message: "expected NULL, found TRUE".to_string()
        }
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
fn parse_sql_rejects_insert_statement() {
    let err = parse_sql("INSERT INTO users VALUES (1)")
        .expect_err("insert should be rejected by reduced parser");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature { feature: "INSERT" }
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
        ("UPDATE users SET age = 1", "UPDATE"),
        (
            "SELECT age, COUNT(*) FROM users GROUP BY age HAVING age >= 21 OR COUNT(*) > 1",
            "HAVING boolean operators beyond AND",
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
        ("SELECT * FROM public.users AS u", "table aliases"),
        ("DESCRIBE users WHERE age > 1", "DESCRIBE modifiers"),
        ("EXPLAIN DESCRIBE users", "DESCRIBE modifiers"),
        (
            "SHOW DATABASES",
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES",
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
                    field: None,
                    distinct: false,
                }),
            ]),
            projection_aliases: vec![Some("display_name".to_string()), Some("total".to_string())],
            predicate: None,
            distinct: false,
            group_by: vec!["name".to_string()],
            having: vec![],
            order_by: vec![SqlOrderTerm {
                field: "name".to_string(),
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
                field: Some("age".to_string()),
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
fn parse_sql_rejects_table_alias_identifier_form() {
    let err = parse_sql("SELECT * FROM users u")
        .expect_err("table alias should be rejected in reduced parser");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "table aliases"
        }
    );
}

#[test]
fn parse_sql_rejects_table_alias_as_form() {
    let err = parse_sql("SELECT * FROM users AS u")
        .expect_err("table alias should be rejected in reduced parser");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "table aliases"
        }
    );
}

#[test]
fn parse_sql_rejects_table_alias_for_schema_qualified_entity() {
    let err = parse_sql("SELECT * FROM public.users AS u")
        .expect_err("table alias should be rejected for schema-qualified entity names");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "table aliases"
        }
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
