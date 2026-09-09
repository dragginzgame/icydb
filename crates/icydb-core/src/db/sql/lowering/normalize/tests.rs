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
fn order_and_having_aliases_share_projection_families_and_case_matching() {
    for projection_sql in ["age", "age + 1", "COUNT(*)"] {
        let sql = format!("SELECT {projection_sql} AS total FROM E");
        let SqlStatement::Select(statement) = parse_sql(&sql).expect("projection") else {
            panic!("select projection");
        };
        let SqlProjection::Items(items) = &statement.projection else {
            panic!("projection items");
        };
        let expected = SqlExpr::from_select_item(&items[0]);
        let aliases = &statement.projection_aliases;
        let having = crate::db::query::preparation::with_preparation_work(|work| {
            super::normalize_having_clauses(
                vec![
                    SqlExpr::Field("TOTAL".into()),
                    SqlExpr::Field("missing".into()),
                ],
                &statement.projection,
                aliases,
                &[],
                work,
            )
        })
        .expect("normalization");
        let order = crate::db::query::preparation::with_preparation_work(|work| {
            super::normalize_select_order_terms(
                vec![SqlOrderTerm {
                    field: SqlExpr::Field("TOTAL".into()),
                    direction: SqlOrderDirection::Desc,
                }],
                &statement.projection,
                aliases,
                &[],
                work,
            )
        })
        .expect("normalization");
        assert_eq!(having, [expected.clone(), SqlExpr::Field("missing".into())]);
        assert_eq!(order[0].field, expected);
        assert_eq!(order[0].direction, SqlOrderDirection::Desc);
    }
}

#[test]
fn alias_replacements_are_single_pass_and_aggregate_inputs_remain_opaque() {
    let projection = SqlProjection::Items(vec![
        SqlSelectItem::Field("second".into()),
        SqlSelectItem::Field("third".into()),
    ]);
    let aliases = [Some("first".into()), Some("second".into())];
    let aggregate = SqlExpr::Aggregate(SqlAggregateCall {
        kind: crate::db::sql::parser::SqlAggregateKind::Count,
        input: Some(Box::new(SqlExpr::Field("first".into()))),
        filter_expr: None,
        distinct: false,
    });
    let clauses = crate::db::query::preparation::with_preparation_work(|work| {
        super::normalize_having_clauses(
            vec![SqlExpr::Field("first".into()), aggregate.clone()],
            &projection,
            &aliases,
            &[],
            work,
        )
    })
    .expect("normalization");
    assert_eq!(clauses, [SqlExpr::Field("second".into()), aggregate]);
    let order = crate::db::query::preparation::with_preparation_work(|work| {
        super::normalize_select_order_terms(
            vec![SqlOrderTerm {
                field: SqlExpr::Field("first".into()),
                direction: SqlOrderDirection::Asc,
            }],
            &projection,
            &aliases,
            &[],
            work,
        )
    })
    .expect("normalization");
    assert_eq!(order[0].field, SqlExpr::Field("second".into()));
}

#[test]
fn normalized_field_paths_move_retained_payloads_and_keep_longest_scope_match() {
    use super::normalize_field_path_to_scope;

    for (scope, root, tail, expected_root, expected_tail) in [
        (
            vec!["users"],
            "profile",
            vec!["address", "city"],
            "profile",
            vec!["address", "city"],
        ),
        (
            vec!["users"],
            "users",
            vec!["profile", "city"],
            "profile",
            vec!["city"],
        ),
        (
            vec!["app.users", "app"],
            "app",
            vec!["users", "name"],
            "name",
            vec![],
        ),
        (vec!["USERS"], "users", vec!["name"], "name", vec![]),
    ] {
        let root = root.to_string();
        let tail: Vec<String> = tail.into_iter().map(str::to_string).collect();
        let retained: Vec<_> = std::iter::once(&root)
            .chain(&tail)
            .filter(|part| part.as_str() == expected_root || expected_tail.contains(&part.as_str()))
            .map(|part| part.as_ptr())
            .collect();
        let scope: Vec<_> = scope.into_iter().map(str::to_string).collect();
        let result = crate::db::query::preparation::with_preparation_work(|work| {
            normalize_field_path_to_scope(root, tail, &scope, work)
        })
        .expect("path normalization");
        let (root, tail): (&String, &[String]) = match &result {
            SqlExpr::Field(root) => (root, &[]),
            SqlExpr::FieldPath { root, segments } => (root, segments),
            other => panic!("unexpected normalized field: {other:?}"),
        };
        assert_eq!(root, expected_root);
        assert_eq!(tail, expected_tail.as_slice());
        assert_eq!(
            std::iter::once(root)
                .chain(tail)
                .map(|part| part.as_ptr())
                .collect::<Vec<_>>(),
            retained
        );
    }
}

#[test]
fn plain_field_normalization_keeps_its_owned_string() {
    let field = "profile".to_string();
    let pointer = field.as_ptr();
    let result = crate::db::query::preparation::with_preparation_work(|work| {
        super::normalize_field_identifier_expr_to_scope(field, &["users".into()], work)
    })
    .expect("field normalization");
    let SqlExpr::Field(field) = &result else {
        panic!("expected plain field")
    };
    assert_eq!(field, "profile");
    assert_eq!(field.as_ptr(), pointer);
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

    let normalized = crate::db::query::preparation::with_preparation_work(|work| {
        super::normalize_select_statement_to_expected_entity(statement, "users", work)
    })
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
