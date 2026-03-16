//! Module: db::sql::parser::tests
//! Responsibility: module-local ownership and contracts for db::sql::parser::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlDescribeStatement, SqlExplainMode,
    SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol, SqlOrderDirection,
    SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement,
    SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement, parse_sql,
    parse_sql_predicate,
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
                }),
            ]),
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
        }),
    );
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
                }),
            ]),
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
                }),
            ]),
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
                }),
            ]),
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
            message: "ORDER BY must appear before LIMIT/OFFSET; \
                      try: SELECT ... ORDER BY <field> [ASC|DESC] LIMIT <n> [OFFSET <n>]"
                .to_string()
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
            message: "ORDER BY must appear before LIMIT/OFFSET; \
                      try: SELECT ... ORDER BY <field> [ASC|DESC] LIMIT <n> [OFFSET <n>]"
                .to_string()
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
            message: "ORDER BY must appear before LIMIT in DELETE statements; \
                      try: DELETE ... ORDER BY <field> [ASC|DESC] LIMIT <n>"
                .to_string()
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
            "SELECT name AS alias FROM users",
            "column/expression aliases",
        ),
        ("SELECT name alias FROM users", "column/expression aliases"),
        ("DELETE FROM users OFFSET 1", "DELETE ... OFFSET"),
        (
            "SELECT * FROM users; SELECT * FROM users",
            "multi-statement SQL input",
        ),
        ("SELECT \"name\" FROM users", "quoted identifiers"),
        (
            "SELECT len(name) FROM users",
            "SQL function namespace beyond supported aggregate forms",
        ),
        (
            "SELECT COUNT(DISTINCT age) FROM users",
            "DISTINCT aggregate qualifiers",
        ),
        ("SELECT * FROM public.users AS u", "table aliases"),
        ("DESCRIBE users WHERE age > 1", "DESCRIBE modifiers"),
        ("EXPLAIN DESCRIBE users", "DESCRIBE modifiers"),
        (
            "SHOW TABLES",
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES",
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
            feature: "SQL function namespace beyond supported aggregate forms"
        }
    );
}

#[test]
fn parse_sql_rejects_distinct_aggregate_qualifier() {
    let err = parse_sql("SELECT COUNT(DISTINCT age) FROM users")
        .expect_err("aggregate DISTINCT qualifier should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "DISTINCT aggregate qualifiers"
        }
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
fn parse_sql_rejects_delete_offset() {
    let err = parse_sql("DELETE FROM users ORDER BY age LIMIT 1 OFFSET 1")
        .expect_err("delete with offset should be rejected");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "DELETE ... OFFSET"
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

#[test]
fn parse_sql_predicate_parses_expression_without_statement_wrapper() {
    let predicate = parse_sql_predicate("active = true AND age >= 21")
        .expect("predicate-only SQL should parse");

    assert_eq!(
        predicate,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )),
        ]),
    );
}

#[test]
fn parse_sql_predicate_rejects_trailing_unsupported_clause() {
    let err = parse_sql_predicate("active = true ORDER BY age")
        .expect_err("predicate parser should reject trailing unsupported clauses");

    assert!(matches!(err, super::SqlParseError::InvalidSyntax { .. }));
}
