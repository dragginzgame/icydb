use crate::{
    db::sql::parser::{
        SqlAggregateCall, SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlProjection,
        SqlSelectItem, SqlSelectStatement, SqlStatement, parse_sql,
    },
    value::Value,
};

fn sql_order_expr(term: &str) -> SqlExpr {
    let sql = format!("SELECT id FROM NormalizeOrderEntity ORDER BY {term}");
    let SqlStatement::Select(statement) =
        parse_sql(&sql).expect("normalize ORDER BY term helper SQL should parse")
    else {
        unreachable!("normalize ORDER BY term helper should always produce one SELECT");
    };

    statement
        .order_by
        .into_iter()
        .next()
        .expect("normalize ORDER BY term helper SQL should carry one ORDER BY term")
        .field
}

#[test]
fn local_scalar_select_is_already_local_canonical() {
    let statement = SqlSelectStatement {
        entity: "PerfAuditUser".to_string(),
        table_alias: None,
        projection: SqlProjection::Items(vec![
            SqlSelectItem::Field("id".to_string()),
            SqlSelectItem::Field("age".to_string()),
        ]),
        projection_aliases: vec![None, None],
        predicate: Some(SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Ne,
                left: Box::new(SqlExpr::Field("age".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int64(24))),
            }),
            right: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Ne,
                left: Box::new(SqlExpr::Field("age".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int64(31))),
            }),
        }),
        distinct: false,
        group_by: vec![],
        having: vec![],
        order_by: vec![SqlOrderTerm {
            field: sql_order_expr("id"),
            direction: SqlOrderDirection::Asc,
        }],
        limit: Some(3),
        offset: None,
    };

    assert!(statement.is_already_local_canonical());
}

#[test]
fn local_scalar_select_with_supported_order_expr_is_already_local_canonical() {
    let statement = SqlSelectStatement {
        entity: "PerfAuditUser".to_string(),
        table_alias: None,
        projection: SqlProjection::Items(vec![
            SqlSelectItem::Field("id".to_string()),
            SqlSelectItem::Field("name".to_string()),
        ]),
        projection_aliases: vec![None, None],
        predicate: None,
        distinct: false,
        group_by: vec![],
        having: vec![],
        order_by: vec![SqlOrderTerm {
            field: sql_order_expr("LOWER(name)"),
            direction: SqlOrderDirection::Asc,
        }],
        limit: Some(3),
        offset: None,
    };

    assert!(statement.is_already_local_canonical());
}

#[test]
fn local_grouped_select_with_local_aggregate_is_already_local_canonical() {
    let statement = SqlSelectStatement {
        entity: "PerfAuditUser".to_string(),
        table_alias: None,
        projection: SqlProjection::Items(vec![
            SqlSelectItem::Field("age".to_string()),
            SqlSelectItem::Aggregate(SqlAggregateCall {
                kind: crate::db::sql::parser::SqlAggregateKind::Count,
                input: None,
                filter_expr: None,
                distinct: false,
            }),
        ]),
        projection_aliases: vec![None, None],
        predicate: None,
        distinct: false,
        group_by: vec!["age".to_string()],
        having: vec![],
        order_by: vec![SqlOrderTerm {
            field: sql_order_expr("age"),
            direction: SqlOrderDirection::Asc,
        }],
        limit: Some(10),
        offset: None,
    };

    assert!(statement.is_already_local_canonical());
}

#[test]
fn qualified_field_select_is_not_already_local_canonical() {
    let statement = SqlSelectStatement {
        entity: "public.PerfAuditUser".to_string(),
        table_alias: None,
        projection: SqlProjection::Items(vec![SqlSelectItem::Field(
            "PerfAuditUser.id".to_string(),
        )]),
        projection_aliases: vec![None],
        predicate: Some(SqlExpr::Binary {
            op: SqlExprBinaryOp::Eq,
            left: Box::new(SqlExpr::Field("PerfAuditUser.age".to_string())),
            right: Box::new(SqlExpr::Literal(Value::Int64(24))),
        }),
        distinct: false,
        group_by: vec![],
        having: vec![],
        order_by: vec![SqlOrderTerm {
            field: sql_order_expr("PerfAuditUser.id"),
            direction: SqlOrderDirection::Asc,
        }],
        limit: Some(1),
        offset: None,
    };

    assert!(!statement.is_already_local_canonical());
}

#[test]
fn predicate_identifier_normalization_preserves_nested_field_paths() {
    let statement = SqlSelectStatement {
        entity: "users".to_string(),
        table_alias: Some("u".to_string()),
        projection: SqlProjection::All,
        projection_aliases: vec![],
        predicate: Some(SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::Field("profile.rank".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int64(5))),
            }),
            right: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::Field("u.age".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int64(21))),
            }),
        }),
        distinct: false,
        group_by: vec![],
        having: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
    };

    let normalized = super::normalize_select_statement_to_expected_entity(statement, "users")
        .expect("predicate identifiers should normalize");

    assert_eq!(
        normalized.predicate,
        Some(SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::FieldPath {
                    root: "profile".to_string(),
                    segments: vec!["rank".to_string()],
                }),
                right: Box::new(SqlExpr::Literal(Value::Int64(5))),
            }),
            right: Box::new(SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(SqlExpr::Field("age".to_string())),
                right: Box::new(SqlExpr::Literal(Value::Int64(21))),
            }),
        }),
    );
}
