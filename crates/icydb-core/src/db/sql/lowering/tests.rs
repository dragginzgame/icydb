//! Module: db::sql::lowering::tests
//! Responsibility: module-local ownership and contracts for db::sql::lowering::tests.
//! Does not own: production SQL lowering behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        executor::ExecutablePlan,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::intent::Query,
        query::plan::{
            QueryMode,
            expr::{Expr, ProjectionField},
        },
        sql::{
            lowering::{
                SqlCommand, SqlGlobalAggregateTerminal, SqlLoweringError, compile_sql_command,
                compile_sql_global_aggregate_command,
            },
            parser::{SqlExplainMode, SqlParseError},
        },
    },
    model::field::FieldKind,
    types::Ulid,
    value::Value,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct SqlLowerEntity {
    id: Ulid,
    name: String,
    age: u64,
}

crate::test_canister! {
    ident = SqlLowerCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = SqlLowerDataStore,
    canister = SqlLowerCanister,
}

crate::test_entity_schema! {
    ident = SqlLowerEntity,
    id = Ulid,
    entity_name = "SqlLowerEntity",
entity_tag = crate::testing::SQL_LOWER_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [],
    store = SqlLowerDataStore,
    canister = SqlLowerCanister,
}

#[test]
fn compile_sql_command_select_star_lowers_to_load_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 10 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("SELECT * should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_select_distinct_star_lowers_to_distinct_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT * FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SELECT DISTINCT * should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(
        query
            .explain()
            .expect("distinct explain should build")
            .distinct(),
        "SELECT DISTINCT * should preserve scalar distinct intent",
    );
}

#[test]
fn compile_sql_command_select_distinct_with_pk_field_list_lowers_to_distinct_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT id, age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SELECT DISTINCT with PK-projected field list should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(
        query
            .explain()
            .expect("distinct explain should build")
            .distinct(),
        "SELECT DISTINCT field-list including PK should preserve scalar distinct intent",
    );
}

#[test]
fn compile_sql_command_rejects_select_distinct_without_pk_projection() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
}

#[test]
fn compile_sql_command_delete_lowers_to_delete_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "DELETE FROM SqlLowerEntity WHERE age < 18 ORDER BY age LIMIT 3",
        MissingRowPolicy::Ignore,
    )
    .expect("DELETE should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(matches!(query.mode(), QueryMode::Delete(_)));
}

#[test]
fn compile_sql_command_delete_direct_starts_with_family_matches_like_delete_intent() {
    let cases = [
        (
            "DELETE FROM SqlLowerEntity WHERE STARTS_WITH(name, 'Al') ORDER BY id ASC LIMIT 1",
            "DELETE FROM SqlLowerEntity WHERE name LIKE 'Al%' ORDER BY id ASC LIMIT 1",
            "strict direct STARTS_WITH delete lowering",
        ),
        (
            "DELETE FROM SqlLowerEntity WHERE STARTS_WITH(LOWER(name), 'Al') ORDER BY id ASC LIMIT 1",
            "DELETE FROM SqlLowerEntity WHERE LOWER(name) LIKE 'Al%' ORDER BY id ASC LIMIT 1",
            "direct LOWER(field) STARTS_WITH delete lowering",
        ),
        (
            "DELETE FROM SqlLowerEntity WHERE STARTS_WITH(UPPER(name), 'AL') ORDER BY id ASC LIMIT 1",
            "DELETE FROM SqlLowerEntity WHERE UPPER(name) LIKE 'AL%' ORDER BY id ASC LIMIT 1",
            "direct UPPER(field) STARTS_WITH delete lowering",
        ),
    ];

    for (direct_sql, like_sql, context) in cases {
        let direct = compile_sql_command::<SqlLowerEntity>(direct_sql, MissingRowPolicy::Ignore)
            .expect("direct STARTS_WITH delete SQL should lower");
        let like = compile_sql_command::<SqlLowerEntity>(like_sql, MissingRowPolicy::Ignore)
            .expect("LIKE delete SQL should lower");

        let SqlCommand::Query(direct_query) = direct else {
            panic!("expected lowered query command for direct STARTS_WITH delete");
        };
        let SqlCommand::Query(like_query) = like else {
            panic!("expected lowered query command for LIKE delete");
        };

        assert!(
            matches!(direct_query.mode(), QueryMode::Delete(_)),
            "direct STARTS_WITH delete should stay on the delete query lane: {context}",
        );
        assert!(
            matches!(like_query.mode(), QueryMode::Delete(_)),
            "LIKE delete should stay on the delete query lane: {context}",
        );
        assert_eq!(
            direct_query
                .plan()
                .expect("direct STARTS_WITH delete plan should build")
                .into_inner(),
            like_query
                .plan()
                .expect("LIKE delete plan should build")
                .into_inner(),
            "bounded direct STARTS_WITH delete lowering should match the established LIKE delete intent: {context}",
        );
    }
}

#[test]
fn compile_sql_command_describe_lowers_to_describe_entity_lane() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "DESCRIBE public.SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("DESCRIBE should lower");

    assert!(
        matches!(command, SqlCommand::DescribeEntity),
        "DESCRIBE should lower to dedicated describe command lane",
    );
}

#[test]
fn compile_sql_command_describe_rejects_entity_mismatch() {
    let err =
        compile_sql_command::<SqlLowerEntity>("DESCRIBE DifferentEntity", MissingRowPolicy::Ignore)
            .expect_err("DESCRIBE entity mismatch should fail lowering");

    assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
}

#[test]
fn compile_sql_command_show_indexes_lowers_to_show_indexes_lane() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SHOW INDEXES public.SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SHOW INDEXES should lower");

    assert!(
        matches!(command, SqlCommand::ShowIndexesEntity),
        "SHOW INDEXES should lower to dedicated show-indexes command lane",
    );
}

#[test]
fn compile_sql_command_show_indexes_rejects_entity_mismatch() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SHOW INDEXES DifferentEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("SHOW INDEXES entity mismatch should fail lowering");

    assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
}

#[test]
fn compile_sql_command_show_columns_lowers_to_show_columns_lane() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SHOW COLUMNS public.SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SHOW COLUMNS should lower");

    assert!(
        matches!(command, SqlCommand::ShowColumnsEntity),
        "SHOW COLUMNS should lower to dedicated show-columns command lane",
    );
}

#[test]
fn compile_sql_command_show_columns_rejects_entity_mismatch() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SHOW COLUMNS DifferentEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("SHOW COLUMNS entity mismatch should fail lowering");

    assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
}

#[test]
fn compile_sql_command_show_entities_lowers_to_show_entities_lane() {
    let command = compile_sql_command::<SqlLowerEntity>("SHOW ENTITIES", MissingRowPolicy::Ignore)
        .expect("SHOW ENTITIES should lower");

    assert!(
        matches!(command, SqlCommand::ShowEntities),
        "SHOW ENTITIES should lower to dedicated show-entities command lane",
    );
}

#[test]
fn compile_sql_command_explain_execution_wraps_lowered_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN EXECUTION SELECT * FROM SqlLowerEntity LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN EXECUTION should lower");

    let SqlCommand::Explain { mode, query } = command else {
        panic!("expected lowered explain command");
    };

    assert_eq!(mode, SqlExplainMode::Execution);
    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_explain_select_distinct_star_lowers_to_distinct_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN SELECT DISTINCT * FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN SELECT DISTINCT * should lower");

    let SqlCommand::Explain { mode, query } = command else {
        panic!("expected lowered explain command");
    };
    assert_eq!(mode, SqlExplainMode::Plan);
    assert!(
        query
            .explain()
            .expect("distinct explain should build")
            .distinct(),
        "EXPLAIN SELECT DISTINCT * should preserve scalar distinct intent",
    );
}

#[test]
fn compile_sql_command_explain_select_distinct_without_pk_projection_rejects() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN SELECT DISTINCT age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("EXPLAIN SELECT DISTINCT without PK projection should fail closed");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectDistinct));
}

#[test]
fn compile_sql_command_explain_global_aggregate_lowers_to_dedicated_command() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN SELECT COUNT(*) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN global aggregate SQL should lower");

    let SqlCommand::ExplainGlobalAggregate { mode, command } = command else {
        panic!("expected lowered explain global aggregate command");
    };

    assert_eq!(mode, SqlExplainMode::Plan);
    assert!(
        matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
        "global aggregate EXPLAIN should preserve aggregate terminal lowering",
    );
}

#[test]
fn compile_sql_command_select_field_projection_lowers_to_scalar_field_selection() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT name, age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("field-list projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .expect("field-list plan should build")
        .projection_spec();
    let field_names = projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("scalar field-list projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn compile_sql_command_select_table_qualified_fields_parity_matches_unqualified_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT SqlLowerEntity.name, SqlLowerEntity.age \
         FROM SqlLowerEntity \
         WHERE SqlLowerEntity.age >= 21 \
         ORDER BY SqlLowerEntity.age DESC LIMIT 5 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified field-list SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name", "age"])
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .order_by_desc("age")
        .limit(5)
        .offset(1);

    assert_eq!(
        sql_query
            .plan()
            .expect("qualified SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("unqualified fluent plan should build")
            .into_inner(),
        "qualified SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
    );
}

#[test]
fn compile_sql_command_qualified_nested_predicate_matches_unqualified_fluent_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity \
         WHERE (SqlLowerEntity.age >= 21 OR SqlLowerEntity.name = 'Ada') \
         AND NOT (SqlLowerEntity.name = 'Bob')",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified nested-predicate SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query =
        Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::And(vec![
            Predicate::Or(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    Value::Int(21),
                    CoercionId::NumericWiden,
                )),
                Predicate::Compare(ComparePredicate::eq(
                    "name".to_string(),
                    Value::Text("Ada".to_string()),
                )),
            ]),
            Predicate::Not(Box::new(Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Bob".to_string()),
            )))),
        ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("qualified nested-predicate SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("unqualified fluent nested-predicate plan should build")
            .into_inner(),
        "qualified nested predicate identifiers should normalize to the same canonical planned intent as unqualified fluent predicates",
    );
}

#[test]
fn compile_sql_command_strict_like_prefix_parity_matches_strict_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE name LIKE 'Al%'",
        MissingRowPolicy::Ignore,
    )
    .expect("strict LIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("strict LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "plain LIKE 'prefix%' SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_direct_starts_with_parity_matches_strict_starts_with_intent() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(name, 'Al')",
        MissingRowPolicy::Ignore,
    )
    .expect("direct STARTS_WITH SQL query should lower");
    let SqlCommand::Query(sql_query) = command else {
        panic!("expected lowered query command");
    };
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("direct STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "direct STARTS_WITH SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_direct_lower_starts_with_parity_matches_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
        MissingRowPolicy::Ignore,
    )
    .expect("direct LOWER(field) STARTS_WITH SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("direct LOWER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct LOWER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_direct_upper_starts_with_parity_matches_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
        MissingRowPolicy::Ignore,
    )
    .expect("direct UPPER(field) STARTS_WITH SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("direct UPPER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct UPPER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_lower_like_prefix_parity_matches_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) LIKE 'Al%'",
        MissingRowPolicy::Ignore,
    )
    .expect("LOWER(field) LIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_upper_like_prefix_parity_matches_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE UPPER(name) LIKE 'AL%'",
        MissingRowPolicy::Ignore,
    )
    .expect("UPPER(field) LIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "UPPER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_like_non_prefix_pattern_rejects() {
    let cases = [
        "SELECT * FROM SqlLowerEntity WHERE name LIKE '%Al'",
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) LIKE '%Al'",
        "SELECT * FROM SqlLowerEntity WHERE UPPER(name) LIKE '%Al'",
    ];

    for sql in cases {
        let err = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .expect_err("non-prefix LIKE pattern should fail closed");

        assert!(matches!(
            err,
            SqlLoweringError::Parse(SqlParseError::UnsupportedFeature {
                feature: "LIKE patterns beyond trailing '%' prefix form"
            })
        ));
    }
}

#[test]
fn compile_sql_command_select_schema_qualified_entity_lowers_to_load_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM public.SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("schema-qualified entity SQL should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_rejects_global_aggregate_select_projection_in_current_slice() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("global aggregate projection should remain gated in this slice");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
}

#[test]
fn compile_sql_command_rejects_mixed_scalar_and_aggregate_projection_in_current_slice() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT name, COUNT(*) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("mixed scalar+aggregate projection should remain gated in this slice");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
}

#[test]
fn compile_sql_command_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };
    assert!(
        query.has_grouping(),
        "grouped aggregate SQL lowering should produce grouped query intent",
    );
}

#[test]
fn compile_sql_command_select_grouped_qualified_identifiers_match_unqualified_intent() {
    let qualified_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT SqlLowerEntity.age, COUNT(*) \
         FROM public.SqlLowerEntity \
         WHERE SqlLowerEntity.age >= 21 \
         GROUP BY SqlLowerEntity.age \
         ORDER BY SqlLowerEntity.age DESC LIMIT 2 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified grouped SQL query should lower");
    let unqualified_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         GROUP BY age \
         ORDER BY age DESC LIMIT 2 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("unqualified grouped SQL query should lower");

    let (SqlCommand::Query(qualified_query), SqlCommand::Query(unqualified_query)) =
        (qualified_command, unqualified_command)
    else {
        panic!("expected lowered grouped query commands");
    };

    assert_eq!(
        qualified_query
            .plan()
            .expect("qualified grouped SQL plan should build")
            .into_inner(),
        unqualified_query
            .plan()
            .expect("unqualified grouped SQL plan should build")
            .into_inner(),
        "qualified grouped SQL identifiers should normalize to the same canonical planned intent as unqualified grouped SQL",
    );
}

#[test]
fn compile_sql_command_select_grouped_having_parity_matches_fluent_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         GROUP BY age \
         HAVING age >= 21 AND COUNT(*) > 1 \
         ORDER BY age DESC LIMIT 3",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped HAVING SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered grouped HAVING SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_group("age", CompareOp::Gte, Value::Int(21))
        .expect("fluent grouped HAVING group-field clause should be accepted")
        .having_aggregate(0, CompareOp::Gt, Value::Int(1))
        .expect("fluent grouped HAVING aggregate clause should be accepted")
        .order_by_desc("age")
        .limit(3);

    assert_eq!(
        sql_query
            .plan()
            .expect("grouped HAVING SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent grouped HAVING plan should build")
            .into_inner(),
        "grouped HAVING SQL lowering and fluent grouped HAVING query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_select_grouped_having_is_null_parity_matches_fluent_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING age IS NOT NULL AND COUNT(*) IS NOT NULL \
         ORDER BY age DESC LIMIT 3",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped HAVING IS [NOT] NULL SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered grouped HAVING IS [NOT] NULL SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_group("age", CompareOp::Ne, Value::Null)
        .expect("fluent grouped HAVING group-field IS NOT NULL should be accepted")
        .having_aggregate(0, CompareOp::Ne, Value::Null)
        .expect("fluent grouped HAVING aggregate IS NOT NULL should be accepted")
        .order_by_desc("age")
        .limit(3);

    assert_eq!(
        sql_query
            .plan()
            .expect("grouped HAVING IS [NOT] NULL SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent grouped HAVING IS [NOT] NULL plan should build")
            .into_inner(),
        "grouped HAVING IS [NOT] NULL SQL lowering and fluent grouped HAVING query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_select_having_without_group_by_rejects() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity HAVING COUNT(*) > 1",
        MissingRowPolicy::Ignore,
    )
    .expect_err("HAVING without GROUP BY should fail closed");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectHaving));
}

#[test]
fn compile_sql_command_select_grouped_aggregate_parity_matches_query_and_executable_identity() {
    // Phase 1: lower equivalent grouped SQL and fluent grouped intents.
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         GROUP BY age \
         ORDER BY age DESC LIMIT 3 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered grouped SQL query command");
    };
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .order_by_desc("age")
        .limit(3)
        .offset(1);

    // Phase 2: assert canonical planned identity + fingerprint parity.
    let sql_compiled = sql_query.plan().expect("grouped SQL plan should build");
    let fluent_compiled = fluent_query
        .plan()
        .expect("fluent grouped plan should build");
    assert_eq!(
        sql_compiled.into_inner(),
        fluent_compiled.into_inner(),
        "grouped SQL lowering and fluent grouped query must produce identical normalized planned intent",
    );
    assert_eq!(
        sql_query
            .plan_hash_hex()
            .expect("grouped SQL plan hash should build"),
        fluent_query
            .plan_hash_hex()
            .expect("fluent grouped plan hash should build"),
        "equivalent grouped SQL and fluent grouped queries must produce identical fingerprints",
    );

    // Phase 3: assert executable-contract parity at route/runtime planning boundary.
    let sql_executable =
        ExecutablePlan::from(sql_query.plan().expect("grouped SQL executable plan"));
    let fluent_executable =
        ExecutablePlan::from(fluent_query.plan().expect("fluent grouped executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_strategy()
            .expect("grouped SQL execution strategy"),
        fluent_executable
            .execution_strategy()
            .expect("fluent grouped execution strategy"),
        "equivalent grouped SQL and fluent grouped queries must produce identical executable strategy",
    );
    assert_eq!(
        sql_executable
            .execution_ordering()
            .expect("grouped SQL execution ordering"),
        fluent_executable
            .execution_ordering()
            .expect("fluent grouped execution ordering"),
        "equivalent grouped SQL and fluent grouped queries must produce identical executable ordering",
    );
}

#[test]
fn compile_sql_command_select_field_projection_parity_matches_query_and_executable_identity() {
    // Phase 1: lower equivalent SQL and fluent field-list intents.
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT name, age FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 5 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("field-list SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name", "age"])
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .order_by_desc("age")
        .limit(5)
        .offset(1);

    // Phase 2: assert canonical planned identity + fingerprint parity.
    let sql_compiled = sql_query.plan().expect("SQL plan should build");
    let fluent_compiled = fluent_query.plan().expect("fluent plan should build");
    assert_eq!(
        sql_compiled.into_inner(),
        fluent_compiled.into_inner(),
        "SQL field-list lowering and fluent field-list query must produce identical normalized planned intent",
    );
    assert_eq!(
        sql_query
            .plan_hash_hex()
            .expect("SQL field-list plan hash should build"),
        fluent_query
            .plan_hash_hex()
            .expect("fluent field-list plan hash should build"),
        "equivalent SQL and fluent field-list projections must produce identical fingerprints",
    );

    // Phase 3: assert executable-contract parity at route/runtime planning boundary.
    let sql_executable = ExecutablePlan::from(sql_query.plan().expect("SQL executable plan"));
    let fluent_executable =
        ExecutablePlan::from(fluent_query.plan().expect("fluent executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_strategy()
            .expect("SQL execution strategy"),
        fluent_executable
            .execution_strategy()
            .expect("fluent execution strategy"),
        "equivalent SQL and fluent field-list projections must produce identical executable strategy",
    );
    assert_eq!(
        sql_executable
            .execution_ordering()
            .expect("SQL execution ordering"),
        fluent_executable
            .execution_ordering()
            .expect("fluent execution ordering"),
        "equivalent SQL and fluent field-list projections must produce identical executable ordering",
    );
}

#[test]
fn compile_sql_command_rejects_entity_mismatch() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM DifferentEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("entity mismatch should fail lowering");

    assert!(matches!(err, SqlLoweringError::EntityMismatch { .. }));
}

#[test]
fn compile_sql_global_aggregate_command_count_star_lowers() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity WHERE age >= 21",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate count SQL should lower");

    assert!(
        matches!(command.terminal(), SqlGlobalAggregateTerminal::CountRows),
        "COUNT(*) should lower to global count terminal",
    );
    assert!(
        !command.query().has_grouping(),
        "global aggregate SQL command should lower to scalar base query shape",
    );
}

#[test]
fn compile_sql_global_aggregate_command_count_sum_avg_min_max_lower() {
    let count_by_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("COUNT(field) SQL should lower");
    let sum_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SUM(field) SQL should lower");
    let avg_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT AVG(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("AVG(field) SQL should lower");
    let min_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT MIN(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("MIN(field) SQL should lower");
    let max_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT MAX(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("MAX(field) SQL should lower");

    assert!(
        matches!(
            count_by_command.terminal(),
            SqlGlobalAggregateTerminal::CountField(field) if field == "age"
        ),
        "COUNT(field) should preserve field target in lowered terminal",
    );
    assert!(
        matches!(
            sum_command.terminal(),
            SqlGlobalAggregateTerminal::SumField(field) if field == "age"
        ),
        "SUM(field) should preserve field target in lowered terminal",
    );
    assert!(
        matches!(
            avg_command.terminal(),
            SqlGlobalAggregateTerminal::AvgField(field) if field == "age"
        ),
        "AVG(field) should preserve field target in lowered terminal",
    );
    assert!(
        matches!(
            min_command.terminal(),
            SqlGlobalAggregateTerminal::MinField(field) if field == "age"
        ),
        "MIN(field) should preserve field target in lowered terminal",
    );
    assert!(
        matches!(
            max_command.terminal(),
            SqlGlobalAggregateTerminal::MaxField(field) if field == "age"
        ),
        "MAX(field) should preserve field target in lowered terminal",
    );
}

#[test]
fn compile_sql_global_aggregate_command_qualified_field_lowers_to_unqualified_terminal() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(SqlLowerEntity.age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified global aggregate field SQL should lower");

    assert!(
        matches!(
            command.terminal(),
            SqlGlobalAggregateTerminal::SumField(field) if field == "age"
        ),
        "qualified aggregate target fields should normalize to canonical unqualified field names",
    );
}

#[test]
fn compile_sql_global_aggregate_command_preserves_base_query_window_semantics() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(age) FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate SQL command should lower");
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .order_by_desc("age")
        .limit(2)
        .offset(1);

    assert_eq!(
        command
            .query()
            .plan()
            .expect("SQL global aggregate base query plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent base query plan should build")
            .into_inner(),
        "global aggregate SQL lowering should preserve scalar base query predicate/order/window semantics",
    );
    assert_eq!(
        command
            .query()
            .plan_hash_hex()
            .expect("SQL global aggregate base query plan hash should build"),
        fluent_query
            .plan_hash_hex()
            .expect("fluent base query plan hash should build"),
        "global aggregate SQL lowering should preserve deterministic base query fingerprint semantics",
    );
}

#[test]
fn compile_sql_global_aggregate_command_parity_matches_fluent_query_and_executable_identity() {
    // Phase 1: lower equivalent global aggregate SQL and fluent scalar base query intent.
    let sql_command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(age) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         ORDER BY age DESC LIMIT 3 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate SQL should lower");
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Int(21),
            CoercionId::NumericWiden,
        )))
        .order_by_desc("age")
        .limit(3)
        .offset(1);

    // Phase 2: assert aggregate-terminal contract and canonical planned identity + fingerprint parity.
    assert!(
        matches!(
            sql_command.terminal(),
            SqlGlobalAggregateTerminal::SumField(field) if field == "age"
        ),
        "global aggregate SQL SUM terminal should preserve canonical target field",
    );
    let sql_compiled = sql_command
        .query()
        .plan()
        .expect("global aggregate SQL base query plan should build");
    let fluent_compiled = fluent_query
        .plan()
        .expect("fluent scalar base query plan should build");
    assert_eq!(
        sql_compiled.into_inner(),
        fluent_compiled.into_inner(),
        "global aggregate SQL base query lowering and fluent scalar query must produce identical normalized planned intent",
    );
    assert_eq!(
        sql_command
            .query()
            .plan_hash_hex()
            .expect("global aggregate SQL base query plan hash should build"),
        fluent_query
            .plan_hash_hex()
            .expect("fluent scalar base query plan hash should build"),
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical fingerprints",
    );

    // Phase 3: assert executable-contract parity at route/runtime planning boundary.
    let sql_executable = ExecutablePlan::from(
        sql_command
            .query()
            .plan()
            .expect("global aggregate SQL base executable plan"),
    );
    let fluent_executable =
        ExecutablePlan::from(fluent_query.plan().expect("fluent scalar executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_strategy()
            .expect("global aggregate SQL base execution strategy"),
        fluent_executable
            .execution_strategy()
            .expect("fluent scalar execution strategy"),
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable strategy",
    );
    assert_eq!(
        sql_executable
            .execution_ordering()
            .expect("global aggregate SQL base execution ordering"),
        fluent_executable
            .execution_ordering()
            .expect("fluent scalar execution ordering"),
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable ordering",
    );
}

#[test]
fn compile_sql_global_aggregate_command_rejects_unsupported_shapes() {
    for sql in [
        "SELECT age FROM SqlLowerEntity",
        "SELECT COUNT(*), SUM(age) FROM SqlLowerEntity",
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
    ] {
        let err =
            compile_sql_global_aggregate_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
                .expect_err("unsupported global aggregate SQL shape should fail closed");

        assert!(
            matches!(
                err,
                SqlLoweringError::UnsupportedSelectProjection
                    | SqlLoweringError::UnsupportedSelectGroupBy
            ),
            "unsupported global aggregate SQL shape should remain lowering-gated: {sql}",
        );
    }
}
