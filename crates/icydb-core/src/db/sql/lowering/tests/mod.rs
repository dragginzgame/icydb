//! Module: db::sql::lowering::tests
//! Covers SQL lowering from parsed statements into structural query shapes.
//! Does not own: production SQL lowering behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        executor::PreparedExecutionPlan,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::intent::Query,
        query::plan::{
            AggregateKind, DeleteSpec, QueryMode,
            expr::{BinaryOp, CaseWhenArm, Expr, FieldId, ProjectionField},
        },
        sql::{
            lowering::{
                PreparedSqlScalarAggregateDescriptorShape, PreparedSqlScalarAggregateDomain,
                PreparedSqlScalarAggregateEmptySetBehavior,
                PreparedSqlScalarAggregateOrderingRequirement, PreparedSqlScalarAggregateRowSource,
                PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
                SqlCommand, SqlLoweringError, TypedSqlGlobalAggregateTerminal, compile_sql_command,
                compile_sql_global_aggregate_command,
            },
            parser::{SqlExplainMode, SqlParseError},
        },
    },
    model::field::FieldKind,
    model::index::{IndexExpression, IndexKeyItem, IndexModel},
    traits::Path,
    types::Ulid,
    value::Value,
};
use serde::Deserialize;
use std::ops::Bound;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct SqlLowerEntity {
    id: Ulid,
    name: String,
    age: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct SqlLowerExpressionEntity {
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

static SQL_LOWER_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
static SQL_LOWER_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
static SQL_LOWER_EXPRESSION_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated_with_key_items(
    "name_lower",
    SqlLowerDataStore::PATH,
    &SQL_LOWER_EXPRESSION_INDEX_FIELDS,
    &SQL_LOWER_EXPRESSION_INDEX_KEY_ITEMS,
    false,
)];

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

crate::test_entity_schema! {
    ident = SqlLowerExpressionEntity,
    id = Ulid,
    entity_name = "SqlLowerExpressionEntity",
    entity_tag = crate::types::EntityTag::new(0x1038),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [&SQL_LOWER_EXPRESSION_INDEX_MODELS[0]],
    store = SqlLowerDataStore,
    canister = SqlLowerCanister,
}

// Lower one SQL query command and extract the normalized first ORDER BY field
// so matrix tests can assert canonical ordering without repeating unwrap steps.
fn first_lowered_order_field(sql: &str, context: &str) -> String {
    let sql_command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
        .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"));
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("{context} should lower to a query command");
    };
    let plan = sql_query
        .plan()
        .unwrap_or_else(|err| panic!("{context} plan should build: {err:?}"))
        .into_inner();

    plan.scalar_plan()
        .order
        .as_ref()
        .unwrap_or_else(|| panic!("{context} ordering should be present"))
        .fields[0]
        .0
        .clone()
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
fn compile_sql_command_numeric_equality_on_uint_field_keeps_strict_plan_parity() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age = 21 ORDER BY age ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("strict numeric equality on uint field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Eq,
            Value::Uint(21),
            CoercionId::Strict,
        )))
        .order_by("age")
        .limit(1);

    assert_eq!(
        query.plan().expect("SQL plan should build").into_inner(),
        fluent_query
            .plan()
            .expect("fluent uint-equality plan should build")
            .into_inner(),
        "SQL uint equality should canonicalize its literal onto the strict runtime field variant",
    );
}

#[test]
fn compile_sql_explain_numeric_equality_on_uint_field_keeps_strict_plan_parity() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN EXECUTION SELECT * FROM SqlLowerEntity WHERE age = 21 ORDER BY age ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN EXECUTION with strict numeric equality on uint field should lower");

    let SqlCommand::Explain { mode, query } = command else {
        panic!("expected lowered explain command");
    };
    assert_eq!(mode, SqlExplainMode::Execution);

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Eq,
            Value::Uint(21),
            CoercionId::Strict,
        )))
        .order_by("age")
        .limit(1);

    assert_eq!(
        query
            .plan()
            .expect("SQL explain query plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent uint-equality plan should build")
            .into_inner(),
        "EXPLAIN EXECUTION should reuse the same canonical uint literal lowering as plain SQL execution",
    );
}

#[test]
fn compile_sql_command_field_to_field_predicate_matches_fluent_intent() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age > age",
        MissingRowPolicy::Ignore,
    )
    .expect("field-to-field predicate should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(crate::db::FieldRef::new("age").gt_field("age"));

    assert_eq!(
        query.plan().expect("SQL plan should build").into_inner(),
        fluent_query
            .plan()
            .expect("fluent field-to-field plan should build")
            .into_inner(),
        "field-to-field SQL lowering should match the canonical fluent predicate leaf",
    );
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
fn compile_sql_command_select_distinct_without_pk_projection_lowers_to_distinct_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("SELECT DISTINCT without PK in projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(
        query
            .explain()
            .expect("distinct explain should build")
            .distinct(),
        "SELECT DISTINCT field-list without PK should preserve scalar distinct intent",
    );
}

#[test]
fn compile_sql_command_order_by_field_alias_matches_canonical_order_target() {
    let alias_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT name AS display_name FROM SqlLowerEntity ORDER BY display_name ASC LIMIT 2",
        MissingRowPolicy::Ignore,
    )
    .expect("ORDER BY field alias should lower");
    let canonical_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT name FROM SqlLowerEntity ORDER BY name ASC LIMIT 2",
        MissingRowPolicy::Ignore,
    )
    .expect("canonical ORDER BY field should lower");

    let SqlCommand::Query(alias_query) = alias_command else {
        panic!("expected lowered field-alias query command");
    };
    let SqlCommand::Query(canonical_query) = canonical_command else {
        panic!("expected lowered canonical query command");
    };

    let alias_plan = alias_query
        .plan()
        .expect("field alias plan should build")
        .into_inner();
    let canonical_plan = canonical_query
        .plan()
        .expect("canonical field plan should build")
        .into_inner();

    assert_eq!(
        alias_plan.scalar_plan().order,
        canonical_plan.scalar_plan().order,
        "ORDER BY field aliases should normalize onto the same canonical logical order target",
    );
    assert_eq!(
        alias_plan.resolved_order(),
        canonical_plan.resolved_order(),
        "ORDER BY field aliases should preserve the same executor-facing resolved order contract",
    );
}

#[test]
fn compile_sql_command_normalizes_order_by_alias_for_supported_scalar_text_targets() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT TRIM(name) AS trimmed_name FROM SqlLowerEntity ORDER BY trimmed_name ASC LIMIT 2",
            "TRIM(name)",
            "ORDER BY TRIM alias",
        ),
        (
            "SELECT LTRIM(name) AS left_trimmed_name FROM SqlLowerEntity ORDER BY left_trimmed_name ASC LIMIT 2",
            "LTRIM(name)",
            "ORDER BY LTRIM alias",
        ),
        (
            "SELECT RTRIM(name) AS right_trimmed_name FROM SqlLowerEntity ORDER BY right_trimmed_name ASC LIMIT 2",
            "RTRIM(name)",
            "ORDER BY RTRIM alias",
        ),
        (
            "SELECT LENGTH(name) AS name_len FROM SqlLowerEntity ORDER BY name_len DESC LIMIT 2",
            "LENGTH(name)",
            "ORDER BY LENGTH alias",
        ),
        (
            "SELECT LEFT(name, 2) AS short_name FROM SqlLowerEntity ORDER BY short_name ASC LIMIT 2",
            "LEFT(name, 2)",
            "ORDER BY LEFT alias",
        ),
    ] {
        assert_eq!(
            first_lowered_order_field(sql, context),
            expected_order_field,
            "{context} should normalize onto the canonical scalar text order expression",
        );
    }
}

#[test]
fn compile_sql_command_normalizes_order_by_alias_for_bounded_numeric_projection_targets() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT age + 1 AS next_age FROM SqlLowerEntity ORDER BY next_age ASC LIMIT 2",
            "age + 1",
            "ORDER BY arithmetic aliases",
        ),
        (
            "SELECT age + age AS total_age FROM SqlLowerEntity ORDER BY total_age ASC LIMIT 2",
            "age + age",
            "ORDER BY field-to-field arithmetic aliases",
        ),
        (
            "SELECT ROUND(age / 3, 2) AS rounded_age FROM SqlLowerEntity ORDER BY rounded_age DESC LIMIT 2",
            "ROUND(age / 3, 2)",
            "ORDER BY ROUND aliases",
        ),
        (
            "SELECT ROUND(age + age, 2) AS rounded_total FROM SqlLowerEntity ORDER BY rounded_total DESC LIMIT 2",
            "ROUND(age + age, 2)",
            "ORDER BY ROUND(field + field) aliases",
        ),
    ] {
        assert_eq!(
            first_lowered_order_field(sql, context),
            expected_order_field,
            "{context} should normalize onto the canonical internal order expression",
        );
    }
}

#[test]
fn compile_sql_command_accepts_direct_bounded_numeric_order_terms() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT age FROM SqlLowerEntity ORDER BY age + 1 ASC LIMIT 2",
            "age + 1",
            "direct ORDER BY arithmetic terms",
        ),
        (
            "SELECT age FROM SqlLowerEntity ORDER BY age + age ASC LIMIT 2",
            "age + age",
            "direct ORDER BY field-to-field arithmetic terms",
        ),
        (
            "SELECT age FROM SqlLowerEntity ORDER BY ROUND(age / 3, 2) DESC LIMIT 2",
            "ROUND(age / 3, 2)",
            "direct ORDER BY ROUND terms",
        ),
    ] {
        assert_eq!(
            first_lowered_order_field(sql, context),
            expected_order_field,
            "{context} should normalize onto the canonical internal order expression",
        );
    }
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
fn compile_sql_command_delete_with_offset_lowers_to_delete_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "DELETE FROM SqlLowerEntity WHERE age < 18 ORDER BY age LIMIT 3 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("DELETE with OFFSET should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    assert!(matches!(
        query.mode(),
        QueryMode::Delete(DeleteSpec {
            limit: Some(3),
            offset: 1,
        })
    ));
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
fn compile_sql_command_select_expression_order_lowers_to_expression_index_range() {
    let command = compile_sql_command::<SqlLowerExpressionEntity>(
        "SELECT id FROM SqlLowerExpressionEntity ORDER BY LOWER(name) ASC LIMIT 2",
        MissingRowPolicy::Ignore,
    )
    .expect("expression-order SELECT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let plan = query
        .plan()
        .expect("expression-order query should plan")
        .into_inner();
    let Some((index, prefix_values, lower, upper)) = plan.access.as_index_range_path() else {
        panic!("expression-order query should use one index-range access path");
    };

    assert_eq!(index.name(), SQL_LOWER_EXPRESSION_INDEX_MODELS[0].name());
    assert!(
        prefix_values.is_empty(),
        "order-only expression fallback should not invent equality prefix values",
    );
    assert_eq!(lower, &Bound::Unbounded);
    assert_eq!(upper, &Bound::Unbounded);
}

#[test]
fn compile_sql_command_normalizes_qualified_expression_order_identifier() {
    let command = compile_sql_command::<SqlLowerExpressionEntity>(
        "SELECT id FROM public.SqlLowerExpressionEntity ORDER BY LOWER(public.SqlLowerExpressionEntity.name) ASC LIMIT 2",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified expression-order SELECT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };
    let explain = query
        .plan()
        .expect("qualified expression order should plan")
        .explain();
    let crate::db::query::explain::ExplainOrderBy::Fields(fields) = explain.order_by() else {
        panic!("qualified expression order should survive into explain order fields");
    };

    assert_eq!(
        fields
            .iter()
            .map(crate::db::query::explain::ExplainOrder::field)
            .collect::<Vec<_>>(),
        vec!["LOWER(name)", "id"],
        "qualified expression order identifiers should normalize to model-local canonical form",
    );
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
fn compile_sql_command_explain_select_distinct_without_pk_projection_lowers() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN SELECT DISTINCT age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN SELECT DISTINCT without PK projection should lower");

    let SqlCommand::Explain { mode, query } = command else {
        panic!("expected lowered explain command");
    };
    assert_eq!(mode, SqlExplainMode::Plan);
    assert!(
        query
            .explain()
            .expect("distinct explain should build")
            .distinct(),
        "EXPLAIN SELECT DISTINCT field-list without PK should preserve scalar distinct intent",
    );
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
        matches!(
            command.terminal(),
            TypedSqlGlobalAggregateTerminal::CountRows
        ),
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
fn compile_sql_command_select_scalar_add_projection_lowers_to_binary_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age + 1 FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("scalar arithmetic projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .expect("scalar arithmetic plan should build")
        .projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();

    assert_eq!(fields.len(), 1);
    match fields[0] {
        ProjectionField::Scalar {
            expr:
                Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left,
                    right,
                },
            alias: None,
        } => {
            assert!(matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age"));
            assert!(matches!(right.as_ref(), Expr::Literal(Value::Int(1))));
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("scalar arithmetic projection should lower to one add expression: {other:?}")
        }
    }
}

#[test]
fn compile_sql_command_select_scalar_field_to_field_projection_lowers_to_binary_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age + age AS total FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("field-to-field arithmetic projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .expect("field-to-field arithmetic plan should build")
        .projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();

    assert_eq!(fields.len(), 1);
    match fields[0] {
        ProjectionField::Scalar {
            expr:
                Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left,
                    right,
                },
            alias: Some(alias),
        } => {
            assert_eq!(alias.as_str(), "total");
            assert!(matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age"));
            assert!(matches!(right.as_ref(), Expr::Field(field) if field.as_str() == "age"));
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!(
                "field-to-field arithmetic projection should lower to one add expression: {other:?}"
            )
        }
    }
}

#[test]
fn compile_sql_command_select_scalar_sub_mul_div_projection_lowers_to_binary_expr() {
    for (sql, expected_op, expected_literal, context) in [
        (
            "SELECT age - 1 FROM SqlLowerEntity",
            crate::db::query::plan::expr::BinaryOp::Sub,
            Value::Int(1),
            "subtraction projection",
        ),
        (
            "SELECT age * 2 FROM SqlLowerEntity",
            crate::db::query::plan::expr::BinaryOp::Mul,
            Value::Int(2),
            "multiplication projection",
        ),
        (
            "SELECT age / 2 FROM SqlLowerEntity",
            crate::db::query::plan::expr::BinaryOp::Div,
            Value::Int(2),
            "division projection",
        ),
    ] {
        let command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"));

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        let projection = query
            .plan()
            .unwrap_or_else(|err| panic!("{context} plan should build: {err:?}"))
            .projection_spec();
        let fields = projection.fields().collect::<Vec<_>>();

        assert_eq!(
            fields.len(),
            1,
            "{context} should lower one projection field"
        );
        match fields[0] {
            ProjectionField::Scalar {
                expr: Expr::Binary { op, left, right },
                alias: None,
            } => {
                assert_eq!(
                    *op, expected_op,
                    "{context} should preserve the arithmetic operator"
                );
                assert!(matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age"));
                assert!(
                    matches!(right.as_ref(), Expr::Literal(value) if value == &expected_literal)
                );
            }
            other @ ProjectionField::Scalar { .. } => {
                panic!("{context} should lower to one bounded binary projection: {other:?}")
            }
        }
    }
}

#[test]
fn compile_sql_command_select_scalar_round_projection_lowers_to_function_expr() {
    for (sql, expected_inner, expected_scale, context) in [
        (
            "SELECT ROUND(age, 2) FROM SqlLowerEntity",
            Expr::Field(crate::db::query::plan::expr::FieldId::new("age")),
            Value::Uint(2),
            "round over plain field",
        ),
        (
            "SELECT ROUND(age / 3, 2) FROM SqlLowerEntity",
            Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Div,
                left: Box::new(Expr::Field(crate::db::query::plan::expr::FieldId::new(
                    "age",
                ))),
                right: Box::new(Expr::Literal(Value::Int(3))),
            },
            Value::Uint(2),
            "round over bounded arithmetic expression",
        ),
        (
            "SELECT ROUND(age + age, 2) FROM SqlLowerEntity",
            Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(Expr::Field(crate::db::query::plan::expr::FieldId::new(
                    "age",
                ))),
                right: Box::new(Expr::Field(crate::db::query::plan::expr::FieldId::new(
                    "age",
                ))),
            },
            Value::Uint(2),
            "round over bounded field-to-field arithmetic expression",
        ),
    ] {
        let command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"));

        let SqlCommand::Query(query) = command else {
            panic!("expected lowered query command");
        };

        let projection = query
            .plan()
            .unwrap_or_else(|err| panic!("{context} plan should build: {err:?}"))
            .projection_spec();
        let fields = projection.fields().collect::<Vec<_>>();

        assert_eq!(
            fields.len(),
            1,
            "{context} should lower one projection field"
        );
        match fields[0] {
            ProjectionField::Scalar {
                expr: Expr::FunctionCall { function, args },
                alias: None,
            } => {
                assert_eq!(
                    *function,
                    crate::db::query::plan::expr::Function::Round,
                    "{context} should lower to canonical ROUND function",
                );
                assert_eq!(args.len(), 2, "{context} should lower two ROUND args");
                assert_eq!(
                    args[0], expected_inner,
                    "{context} should preserve inner expr"
                );
                assert_eq!(
                    args[1],
                    Expr::Literal(expected_scale.clone()),
                    "{context} should preserve round scale literal",
                );
            }
            other @ ProjectionField::Scalar { .. } => {
                panic!("{context} should lower to one ROUND function call: {other:?}")
            }
        }
    }
}

#[test]
fn compile_sql_command_select_chained_scalar_projection_lowers_to_nested_binary_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age + 1 * 2 FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("chained scalar projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .unwrap_or_else(|err| panic!("chained scalar projection plan should build: {err:?}"))
        .into_inner()
        .projection_selection;

    assert!(
        matches!(
            projection,
            crate::db::query::plan::expr::ProjectionSelection::Exprs(fields)
            if matches!(
                &fields[0],
            ProjectionField::Scalar {
                expr: Expr::Binary { op: BinaryOp::Add, left, right },
                alias: None,
            }
            if matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age")
                && matches!(
                    right.as_ref(),
                    Expr::Binary { op: BinaryOp::Mul, left, right }
                    if matches!(left.as_ref(), Expr::Literal(Value::Int(1)))
                        && matches!(right.as_ref(), Expr::Literal(Value::Int(2)))
                )
            )
        ),
        "chained scalar projection should lower to nested binary expressions with multiplication precedence preserved",
    );
}

#[test]
fn compile_sql_command_select_searched_case_projection_lowers_to_case_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT CASE WHEN age >= 21 THEN 'adult' ELSE 'minor' END FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .unwrap_or_else(|err| panic!("searched CASE projection plan should build: {err:?}"))
        .into_inner()
        .projection_selection;

    assert!(
        matches!(
            projection,
            crate::db::query::plan::expr::ProjectionSelection::Exprs(fields)
                if matches!(
                    &fields[0],
                    ProjectionField::Scalar {
                        expr: Expr::Case {
                            when_then_arms,
                            else_expr,
                        },
                        alias: None,
                    }
                    if when_then_arms.as_slice() == [CaseWhenArm::new(
                        Expr::Binary {
                            op: BinaryOp::Gte,
                            left: Box::new(Expr::Field(FieldId::new("age"))),
                            right: Box::new(Expr::Literal(Value::Int(21))),
                        },
                        Expr::Literal(Value::Text("adult".to_string())),
                    )]
                        && else_expr.as_ref() == &Expr::Literal(Value::Text("minor".to_string()))
                )
        ),
        "searched CASE projection should lower onto one planner-owned CASE expression",
    );
}

#[test]
fn compile_sql_command_select_searched_case_without_else_canonicalizes_to_null() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT CASE WHEN age >= 21 THEN 'adult' END FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE without ELSE should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .unwrap_or_else(|err| panic!("searched CASE without ELSE plan should build: {err:?}"))
        .into_inner()
        .projection_selection;

    assert!(
        matches!(
            projection,
            crate::db::query::plan::expr::ProjectionSelection::Exprs(fields)
                if matches!(
                    &fields[0],
                    ProjectionField::Scalar {
                        expr: Expr::Case { else_expr, .. },
                        alias: None,
                    } if else_expr.as_ref() == &Expr::Literal(Value::Null)
                )
        ),
        "searched CASE without ELSE should canonicalize onto one explicit planner NULL fallback",
    );
}

#[test]
fn compile_sql_command_select_where_searched_case_matches_canonical_predicate_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE WHERE SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered searched CASE WHERE query command");
    };

    let fluent_query =
        Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::Or(vec![
            Predicate::And(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gte,
                    Value::Int(30),
                    CoercionId::NumericWiden,
                )),
                Predicate::True,
            ]),
            Predicate::And(vec![
                Predicate::Not(Box::new(Predicate::Compare(
                    ComparePredicate::with_coercion(
                        "age",
                        CompareOp::Gte,
                        Value::Int(30),
                        CoercionId::NumericWiden,
                    ),
                ))),
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Eq,
                    Value::Uint(20),
                    CoercionId::Strict,
                )),
            ]),
        ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("searched CASE WHERE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("canonical searched CASE WHERE fluent plan should build")
            .into_inner(),
        "searched CASE WHERE should lower through the shared pre-aggregate expression seam before predicate adaptation",
    );
}

#[test]
fn compile_sql_command_distinguishes_is_null_from_eq_null_predicates() {
    let is_null = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age IS NULL",
        MissingRowPolicy::Ignore,
    )
    .expect("IS NULL SQL query should lower");
    let eq_null = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age = NULL",
        MissingRowPolicy::Ignore,
    )
    .expect("= NULL SQL query should lower");

    let SqlCommand::Query(is_null_query) = is_null else {
        panic!("expected lowered IS NULL query command");
    };
    let SqlCommand::Query(eq_null_query) = eq_null else {
        panic!("expected lowered = NULL query command");
    };

    assert_ne!(
        is_null_query
            .plan()
            .expect("IS NULL SQL plan should build")
            .into_inner(),
        eq_null_query
            .plan()
            .expect("= NULL SQL plan should build")
            .into_inner(),
        "IS NULL and = NULL should remain semantically distinct through SQL lowering",
    );
}

#[test]
fn compile_sql_command_rejects_round_with_negative_scale() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT ROUND(age, -1) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("ROUND should reject negative scale in the bounded slice");

    assert!(matches!(err, SqlLoweringError::Query(_)));
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
fn compile_sql_command_select_table_alias_fields_parity_matches_unqualified_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT alias.name, alias.age \
         FROM SqlLowerEntity alias \
         WHERE alias.age >= 21 \
         ORDER BY alias.age DESC LIMIT 5 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("table-alias field-list SQL query should lower");
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
            .expect("table-alias SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("unqualified fluent plan should build")
            .into_inner(),
        "single-table alias SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
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
fn compile_sql_command_angle_bracket_not_equal_matches_canonical_ne_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE name <> 'Al'",
        MissingRowPolicy::Ignore,
    )
    .expect("angle-bracket not-equal SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Ne,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("angle-bracket not-equal SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("canonical fluent not-equal plan should build")
            .into_inner(),
        "SQL <> lowering must match the canonical != intent",
    );
}

#[test]
fn compile_sql_command_in_trailing_comma_matches_canonical_in_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age IN (10, 20, 30,)",
        MissingRowPolicy::Ignore,
    )
    .expect("IN with trailing comma SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let canonical_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE age IN (10, 20, 30)",
        MissingRowPolicy::Ignore,
    )
    .expect("canonical IN SQL query should lower");
    let SqlCommand::Query(canonical_query) = canonical_command else {
        panic!("expected lowered canonical query command");
    };

    assert_eq!(
        sql_query
            .plan()
            .expect("IN with trailing comma SQL plan should build")
            .into_inner(),
        canonical_query
            .plan()
            .expect("canonical IN SQL plan should build")
            .into_inner(),
        "SQL IN with trailing comma must match the canonical IN intent",
    );
}

#[test]
fn compile_sql_command_strict_not_like_prefix_parity_matches_negated_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE name NOT LIKE 'Al%'",
        MissingRowPolicy::Ignore,
    )
    .expect("strict NOT LIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::not(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ))),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("strict NOT LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent negated strict starts-with plan should build")
            .into_inner(),
        "plain NOT LIKE 'prefix%' SQL lowering and fluent negated strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_ilike_prefix_matches_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE name ILIKE 'al%'",
        MissingRowPolicy::Ignore,
    )
    .expect("ILIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("al".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("ILIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent casefold starts-with plan should build")
            .into_inner(),
        "plain ILIKE 'prefix%' SQL lowering must match the canonical casefold starts-with intent",
    );
}

#[test]
fn compile_sql_command_not_ilike_prefix_matches_negated_casefold_starts_with_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE name NOT ILIKE 'al%'",
        MissingRowPolicy::Ignore,
    )
    .expect("NOT ILIKE prefix SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        Predicate::not(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("al".to_string()),
            CoercionId::TextCasefold,
        ))),
    );

    assert_eq!(
        sql_query
            .plan()
            .expect("NOT ILIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent negated casefold starts-with plan should build")
            .into_inner(),
        "plain NOT ILIKE 'prefix%' SQL lowering must match the canonical negated casefold starts-with intent",
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
fn compile_sql_command_casefold_not_like_prefix_matrix_matches_negated_casefold_starts_with_intent()
{
    let cases = [
        (
            "SELECT * FROM SqlLowerEntity WHERE LOWER(name) NOT LIKE 'Al%'",
            "LOWER(field) NOT LIKE 'prefix%' SQL lowering",
            "Al",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE UPPER(name) NOT LIKE 'AL%'",
            "UPPER(field) NOT LIKE 'prefix%' SQL lowering",
            "AL",
        ),
    ];

    for (sql, context, prefix) in cases {
        let sql_command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"));
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command");
        };

        let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
            Predicate::not(Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text(prefix.to_string()),
                CoercionId::TextCasefold,
            ))),
        );

        assert_eq!(
            sql_query
                .plan()
                .unwrap_or_else(|err| panic!("{context} SQL plan should build: {err}"))
                .into_inner(),
            fluent_query
                .plan()
                .unwrap_or_else(|err| panic!("{context} fluent plan should build: {err}"))
                .into_inner(),
            "{context} and fluent negated casefold starts-with query must produce identical normalized planned intent",
        );
    }
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
fn compile_sql_command_lower_ordered_text_range_parity_matches_casefold_range_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) >= 'Al' AND LOWER(name) < 'Am'",
        MissingRowPolicy::Ignore,
    )
    .expect("LOWER(field) ordered text range SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query =
        Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("Am".to_string()),
                CoercionId::TextCasefold,
            )),
        ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) ordered text range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold range plan should build")
            .into_inner(),
        "LOWER(field) ordered text range SQL lowering and fluent text-casefold range query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_upper_ordered_text_range_parity_matches_casefold_range_intent() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity WHERE UPPER(name) >= 'AL' AND UPPER(name) < 'AM'",
        MissingRowPolicy::Ignore,
    )
    .expect("UPPER(field) ordered text range SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };

    let fluent_query =
        Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("AM".to_string()),
                CoercionId::TextCasefold,
            )),
        ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) ordered text range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold range plan should build")
            .into_inner(),
        "UPPER(field) ordered text range SQL lowering and fluent text-casefold range query must produce identical normalized planned intent",
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
fn compile_sql_command_select_grouped_top_level_distinct_normalizes_to_grouped_query() {
    let distinct_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("top-level grouped SELECT DISTINCT should lower");
    let plain_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("plain grouped aggregate projection should lower");

    let (SqlCommand::Query(distinct_query), SqlCommand::Query(plain_query)) =
        (distinct_command, plain_command)
    else {
        panic!("expected lowered grouped query commands");
    };

    assert_eq!(
        distinct_query
            .plan()
            .expect("distinct grouped SQL plan should build")
            .into_inner(),
        plain_query
            .plan()
            .expect("plain grouped SQL plan should build")
            .into_inner(),
        "top-level grouped SELECT DISTINCT should normalize to the same grouped intent as the non-DISTINCT form",
    );
}

#[test]
fn compile_sql_command_allows_grouped_text_projection_over_grouped_field() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT name, TRIM(name), COUNT(*) FROM SqlLowerEntity GROUP BY name",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped text projection over grouped field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped text projection over grouped field should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_grouped_projection_unknown_field_stays_specific() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT agge, AVG(age) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped projection typo should stay a field-resolution error");

    assert!(matches!(
        err,
        SqlLoweringError::UnknownField { ref field } if field == "agge"
    ));
}

#[test]
fn compile_sql_command_allows_grouped_arithmetic_projection_over_grouped_field() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, age + 1, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped arithmetic projection over grouped field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped arithmetic projection over grouped field should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_allows_grouped_round_projection_over_grouped_field() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, ROUND(age / 3, 2), COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped ROUND projection over grouped field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped ROUND projection over grouped field should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_allows_grouped_round_projection_over_aggregate_output() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, ROUND(AVG(age), 2) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped ROUND projection over aggregate output should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped ROUND projection over aggregate output should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_allows_grouped_arithmetic_projection_over_aggregate_output() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) + MAX(age) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped arithmetic projection over aggregate output should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped arithmetic projection over aggregate output should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_deduplicates_repeated_grouped_aggregate_leaves_in_projection_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) + COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped arithmetic projection with repeated aggregate leaves should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped arithmetic projection with repeated aggregate leaves should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_deduplicates_repeated_grouped_aggregate_input_leaves_in_projection_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age + 1) + AVG(age + 1) \
         FROM SqlLowerEntity \
         GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped arithmetic projection with repeated aggregate-input leaves should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };
    let planned = query
        .plan()
        .expect("grouped arithmetic projection with repeated aggregate-input leaves should plan")
        .into_inner();
    let grouped = planned
        .grouped_plan()
        .expect("grouped arithmetic projection should keep grouped plan shape");

    assert_eq!(
        grouped.group.aggregates.len(),
        1,
        "repeated grouped aggregate-input leaves should keep one semantic grouped aggregate declaration",
    );
}

#[test]
fn compile_sql_command_allows_grouped_additive_order_over_grouped_field() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age + 1 ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped additive ORDER BY over grouped field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped additive ORDER BY over grouped field should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_allows_grouped_subtractive_order_over_grouped_field() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age - 2 ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped subtractive ORDER BY over grouped field should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped subtractive ORDER BY over grouped field should stay on the admitted grouped plan lane",
    );
}

#[test]
fn compile_sql_command_rejects_grouped_non_preserving_computed_order() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age + age ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped non-preserving computed ORDER BY should still lower structurally");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    let err = query.plan().expect_err(
        "grouped ORDER BY expressions that do not preserve grouped-key order should remain fail-closed",
    );

    assert!(matches!(
        err,
        crate::db::query::intent::QueryError::Plan(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanError::Policy(policy)
                    if matches!(
                        policy.as_ref(),
                        crate::db::query::plan::validate::PlanPolicyError::Group(group)
                            if matches!(
                                group.as_ref(),
                                crate::db::query::plan::validate::GroupPlanError::OrderExpressionNotAdmissible { term } if term == "age + age"
                            )
                    )
            )
    ));
}

#[test]
fn compile_sql_command_allows_grouped_aggregate_order_with_limit() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age) \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY AVG(age) DESC, age ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate ORDER BY with LIMIT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped aggregate ORDER BY with LIMIT should reserve the bounded Top-K grouped lane",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_aggregate_order_by_alias_with_limit() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age) AS avg_age \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY avg_age DESC, age ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate ORDER BY alias with LIMIT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };
    let plan = query
        .plan()
        .expect("grouped aggregate ORDER BY alias with LIMIT should plan")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("grouped aggregate ORDER BY alias should preserve order terms");

    assert_eq!(
        order.fields[0].0, "AVG(age)",
        "grouped aggregate ORDER BY aliases should normalize onto the canonical aggregate term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_aggregate_input_order_by_alias_with_limit() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age + 1) AS avg_plus_one \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY avg_plus_one DESC, age ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate input ORDER BY alias with LIMIT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };
    let plan = query
        .plan()
        .expect("grouped aggregate input ORDER BY alias with LIMIT should plan")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("grouped aggregate input ORDER BY alias should preserve order terms");

    assert_eq!(
        order.fields[0].0, "AVG(age + 1)",
        "grouped aggregate input ORDER BY aliases should normalize onto the canonical aggregate term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_wrapped_aggregate_input_order_by_alias_with_limit() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, ROUND(AVG((age + age) / 2), 2) AS avg_balanced \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY avg_balanced DESC, age ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped wrapped aggregate input ORDER BY alias with LIMIT should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };
    let plan = query
        .plan()
        .expect("grouped wrapped aggregate input ORDER BY alias with LIMIT should plan")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("grouped wrapped aggregate input ORDER BY alias should preserve order terms");

    assert_eq!(
        order.fields[0].0, "ROUND(AVG((age + age) / 2), 2)",
        "grouped wrapped aggregate input ORDER BY aliases should preserve the canonical parenthesized aggregate term",
    );
}

#[test]
fn compile_sql_command_accepts_grouped_aggregate_order_by_alias_with_field_compare_predicate() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, ROUND(AVG(age), 2) AS avg_age \
         FROM SqlLowerEntity \
         WHERE name > name \
         GROUP BY age \
         ORDER BY avg_age DESC, age ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate ORDER BY alias with grouped residual predicate should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped aggregate ORDER BY alias should still reserve the bounded Top-K grouped lane when a residual predicate is present",
    );
}

#[test]
fn compile_sql_command_rejects_grouped_aggregate_order_with_offset() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age) \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY AVG(age) DESC \
         LIMIT 1 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate ORDER BY with OFFSET should still lower structurally");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    let err = query
        .plan()
        .expect_err("grouped aggregate ORDER BY with OFFSET must stay fail-closed");

    assert!(matches!(
        err,
        crate::db::query::intent::QueryError::Plan(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanError::Policy(policy)
                    if matches!(
                        policy.as_ref(),
                        crate::db::query::plan::validate::PlanPolicyError::Group(group)
                            if matches!(
                                group.as_ref(),
                                crate::db::query::plan::validate::GroupPlanError::OrderOffsetNotSupported
                            )
                    )
            )
    ));
}

#[test]
fn compile_sql_command_rejects_grouped_non_group_field_projection() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, name, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped non-group field projection should stay fail-closed");

    assert!(matches!(
        err,
        SqlLoweringError::GroupedProjectionReferencesNonGroupField { index: 1 }
    ));
}

#[test]
fn compile_sql_command_rejects_grouped_projection_without_aggregate_specifically() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT age FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped projection without aggregates should stay fail-closed");

    assert!(matches!(
        err,
        SqlLoweringError::GroupedProjectionRequiresAggregate
    ));
}

#[test]
fn compile_sql_command_rejects_grouped_star_projection_specifically() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped star projection should stay fail-closed");

    assert!(matches!(
        err,
        SqlLoweringError::GroupedProjectionRequiresExplicitList
    ));
}

#[test]
fn compile_sql_command_rejects_grouped_scalar_projection_after_aggregate_specifically() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT COUNT(*), age FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped scalar terms after aggregate terms should stay fail-closed");

    assert!(matches!(
        err,
        SqlLoweringError::GroupedProjectionScalarAfterAggregate { index: 1 }
    ));
}

#[test]
fn compile_sql_command_accepts_grouped_field_to_field_predicate() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity WHERE name > name GROUP BY age ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped field-to-field predicate SQL should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query.plan().expect(
        "grouped field-to-field predicate should now stay on the grouped residual predicate path",
    );
}

#[test]
fn compile_sql_command_accepts_projected_direct_bounded_numeric_order_terms() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age + 1 FROM SqlLowerEntity ORDER BY age + 1 ASC",
        MissingRowPolicy::Ignore,
    )
    .expect("projected direct ORDER BY arithmetic term should lower");

    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered projected arithmetic order query command");
    };

    let plan = sql_query
        .plan()
        .expect("projected direct arithmetic order plan should build")
        .into_inner();
    assert_eq!(
        plan.scalar_plan()
            .order
            .as_ref()
            .expect("projected direct arithmetic order should be present")
            .fields[0]
            .0,
        "age + 1",
        "projected direct ORDER BY arithmetic terms should normalize onto the canonical internal numeric expression",
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
fn compile_sql_command_select_grouped_post_aggregate_having_exprs_lowers() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING ROUND(AVG(age), 2) >= 10 AND COUNT(*) + 1 > 1 \
         ORDER BY age DESC LIMIT 3",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped post-aggregate HAVING SQL query should lower");

    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered grouped HAVING SQL query command");
    };

    sql_query
        .plan()
        .expect("grouped post-aggregate HAVING SQL plan should build");
}

#[test]
fn compile_sql_command_select_grouped_searched_case_having_exprs_lowers() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1 \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped searched CASE HAVING SQL query should lower");
    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped searched CASE HAVING SQL query command");
    };

    let planned = query
        .plan()
        .expect("grouped searched CASE HAVING SQL plan should build")
        .into_inner();
    let grouped = planned
        .grouped_plan()
        .expect("grouped searched CASE HAVING SQL should keep grouped plan shape");

    assert!(
        matches!(
            grouped.having_expr.as_ref(),
            Some(crate::db::query::plan::GroupHavingExpr::Compare { left, op: CompareOp::Eq, right })
                if matches!(
                    left,
                    crate::db::query::plan::GroupHavingValueExpr::Case { else_expr, .. }
                        if else_expr.as_ref()
                            == &crate::db::query::plan::GroupHavingValueExpr::Literal(Value::Int(0))
                ) && matches!(
                    right,
                    crate::db::query::plan::GroupHavingValueExpr::Literal(Value::Int(1))
                )
        ),
        "grouped searched CASE HAVING should lower through the shared post-aggregate value seam",
    );
}

#[test]
fn compile_sql_command_select_having_without_group_by_rejects() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity HAVING COUNT(*) > 1",
        MissingRowPolicy::Ignore,
    )
    .expect_err("HAVING without GROUP BY should fail closed");

    assert!(matches!(err, SqlLoweringError::HavingRequiresGroupBy));
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
        PreparedExecutionPlan::from(sql_query.plan().expect("grouped SQL executable plan"));
    let fluent_executable =
        PreparedExecutionPlan::from(fluent_query.plan().expect("fluent grouped executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_family()
            .expect("grouped SQL execution family"),
        fluent_executable
            .execution_family()
            .expect("fluent grouped execution family"),
        "equivalent grouped SQL and fluent grouped queries must produce identical executable family",
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
    let sql_executable =
        PreparedExecutionPlan::from(sql_query.plan().expect("SQL executable plan"));
    let fluent_executable =
        PreparedExecutionPlan::from(fluent_query.plan().expect("fluent executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_family()
            .expect("SQL execution family"),
        fluent_executable
            .execution_family()
            .expect("fluent execution family"),
        "equivalent SQL and fluent field-list projections must produce identical executable family",
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
        matches!(
            command.terminal(),
            TypedSqlGlobalAggregateTerminal::CountRows
        ),
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
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "COUNT(field) should resolve the target field slot in the typed lowered terminal",
    );
    assert!(
        matches!(
            sum_command.terminal(),
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "SUM(field) should resolve the target field slot in the typed lowered terminal",
    );
    assert!(
        matches!(
            avg_command.terminal(),
            TypedSqlGlobalAggregateTerminal::AvgField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "AVG(field) should resolve the target field slot in the typed lowered terminal",
    );
    assert!(
        matches!(
            min_command.terminal(),
            TypedSqlGlobalAggregateTerminal::MinField(field) if field.field() == "age"
        ),
        "MIN(field) should resolve the target field slot in the typed lowered terminal",
    );
    assert!(
        matches!(
            max_command.terminal(),
            TypedSqlGlobalAggregateTerminal::MaxField(field) if field.field() == "age"
        ),
        "MAX(field) should resolve the target field slot in the typed lowered terminal",
    );
}

#[test]
fn compile_sql_global_aggregate_command_multiple_terminals_lower() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT MIN(age), MAX(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("multiple global aggregate terminals should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "multi-terminal global aggregate SQL should preserve both aggregate terminals",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::MinField(field) if field.field() == "age"
        ),
        "the first lowered terminal should preserve MIN(age)",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::MaxField(field) if field.field() == "age"
        ),
        "the second lowered terminal should preserve MAX(age)",
    );
}

#[test]
fn compile_sql_global_aggregate_command_duplicate_terminals_dedup_to_unique_terminal_remap() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(age), COUNT(age), SUM(age), COUNT(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("duplicate global aggregate terminals should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "duplicate global aggregate SQL should keep only unique executable terminals",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "the first unique lowered terminal should preserve COUNT(age)",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "the second unique lowered terminal should preserve SUM(age)",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 0, 1, 0],
        "duplicate aggregate outputs should remap back to the original projection order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_mixed_duplicate_terminals_preserve_unique_order_remap() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(age), SUM(age), COUNT(age), SUM(age), MAX(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("mixed duplicate global aggregate terminals should lower");

    assert_eq!(
        command.terminals().len(),
        3,
        "mixed duplicate global aggregate SQL should keep one unique terminal per semantic aggregate",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "the first unique lowered terminal should preserve COUNT(age)",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "the second unique lowered terminal should preserve SUM(age)",
    );
    assert!(
        matches!(
            &command.terminals()[2],
            TypedSqlGlobalAggregateTerminal::MaxField(target_slot)
                if target_slot.field() == "age"
        ),
        "the third unique lowered terminal should preserve MAX(age)",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0, 1, 2],
        "mixed duplicate aggregate outputs should remap to the first-seen unique terminal order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_distinct_terminals_do_not_collapse_into_plain_count() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(age), COUNT(DISTINCT age), COUNT(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("distinct and non-distinct global aggregate terminals should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "COUNT(age) and COUNT(DISTINCT age) should remain separate executable terminals",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "the first unique lowered terminal should preserve plain COUNT(age)",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: true,
            } if target_slot.field() == "age"
        ),
        "the second unique lowered terminal should preserve COUNT(DISTINCT age)",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0],
        "distinct and non-distinct aggregate outputs should only collapse exact duplicates",
    );
}

#[test]
fn compile_sql_global_aggregate_command_qualified_and_unqualified_duplicates_collapse() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(age), COUNT(SqlLowerEntity.age), COUNT(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("qualified and unqualified duplicate global aggregate terminals should lower");

    assert_eq!(
        command.terminals().len(),
        1,
        "qualified and unqualified aggregate terminals should normalize to one unique executable terminal",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::CountField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "qualified aggregate target fields should normalize onto the canonical COUNT(age) terminal",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 0, 0],
        "qualified and unqualified duplicate outputs should remap to the same unique terminal",
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
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "qualified aggregate target fields should normalize to canonical unqualified target slots",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_expression_input_terminals() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(1), SUM(age + 1), AVG(age + 1) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("aggregate input expressions should lower once the scalar aggregate runtime widens");

    assert!(
        matches!(
            command.terminals(),
            [
                TypedSqlGlobalAggregateTerminal::CountExpr {
                    distinct: false,
                    ..
                },
                TypedSqlGlobalAggregateTerminal::SumExpr {
                    distinct: false,
                    ..
                },
                TypedSqlGlobalAggregateTerminal::AvgExpr {
                    distinct: false,
                    ..
                },
            ]
        ),
        "expression aggregate inputs should preserve expression-backed typed terminals",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_chained_expression_input_terminals() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT AVG(age + 1 * 2), ROUND(AVG((age + age) / 2), 2) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("chained aggregate input expressions should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "chained aggregate input expressions should lower onto one terminal per aggregate leaf",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::AvgExpr { input_expr, distinct: false }
                if matches!(
                    input_expr,
                    Expr::Binary { op: BinaryOp::Add, left, right }
                    if matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age")
                        && matches!(right.as_ref(), Expr::Literal(Value::Decimal(value)) if *value == crate::types::Decimal::from(2_u64))
                )
        ),
        "AVG(age + 1 * 2) should preserve the folded semantic input shape in the typed aggregate terminal",
    );
    assert_eq!(
        command.projection().len(),
        2,
        "chained aggregate input expressions should still preserve the outward projection shape",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_post_aggregate_projection_expressions() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT ROUND(AVG(age), 4), COUNT(*) + 1, MAX(age) - MIN(age) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect(
        "post-aggregate scalar wrappers should lower through the dedicated global aggregate lane",
    );

    assert_eq!(
        command.terminals().len(),
        4,
        "wrapped global aggregate output expressions should dedupe onto one unique executable terminal per aggregate leaf",
    );
    assert_eq!(
        command.projection().len(),
        3,
        "wrapped global aggregate output expressions should preserve the outward projection shape",
    );
    assert!(
        command.output_remap().is_empty(),
        "wrapped global aggregate output expressions should stop depending on the legacy top-level terminal remap",
    );
}

#[test]
fn compile_sql_global_aggregate_command_ignores_singleton_output_order_by_alias() {
    let ordered = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT AVG(age) AS avg_age FROM SqlLowerEntity ORDER BY avg_age DESC",
        MissingRowPolicy::Ignore,
    )
    .expect("singleton global aggregate output ordering should lower as an inert no-op");
    let canonical = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT AVG(age) AS avg_age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("canonical singleton global aggregate should lower");

    assert_eq!(
        ordered
            .query()
            .plan()
            .expect("ordered singleton global aggregate base query plan should build")
            .into_inner(),
        canonical
            .query()
            .plan()
            .expect("canonical singleton global aggregate base query plan should build")
            .into_inner(),
        "singleton global aggregate ORDER BY aliases should not leak into the base-row aggregate window query",
    );
}

#[test]
fn compile_sql_global_aggregate_command_ignores_singleton_wrapped_output_order_by_alias() {
    let ordered = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT ROUND(AVG(age), 2) AS avg_age FROM SqlLowerEntity ORDER BY avg_age DESC",
        MissingRowPolicy::Ignore,
    )
    .expect("singleton wrapped global aggregate output ordering should lower as an inert no-op");
    let canonical = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT ROUND(AVG(age), 2) AS avg_age FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("canonical singleton wrapped global aggregate should lower");

    assert_eq!(
        ordered
            .query()
            .plan()
            .expect("ordered singleton wrapped global aggregate base query plan should build")
            .into_inner(),
        canonical
            .query()
            .plan()
            .expect("canonical singleton wrapped global aggregate base query plan should build")
            .into_inner(),
        "singleton wrapped global aggregate ORDER BY aliases should not leak into the base-row aggregate window query",
    );
}

#[test]
fn compile_sql_global_aggregate_command_deduplicates_expression_input_terminals() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(1), SUM(age + 1), COUNT(1), SUM(age + 1) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("duplicate expression aggregate inputs should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "duplicate expression aggregate inputs should keep one unique executable terminal per semantic aggregate",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::CountExpr {
                distinct: false,
                ..
            }
        ),
        "the first unique lowered terminal should preserve COUNT(1)",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::SumExpr {
                distinct: false,
                ..
            }
        ),
        "the second unique lowered terminal should preserve SUM(age + 1)",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0, 1],
        "duplicate expression aggregate outputs should remap to the first-seen unique terminal order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_constant_folds_expression_input_terminals_before_dedup() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(2 * 3), SUM(6), AVG(ROUND(2 * 3, 1)), AVG(6.0) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("constant aggregate input expressions should lower");

    assert_eq!(
        command.terminals().len(),
        2,
        "constant-folded aggregate input expressions should dedupe onto one semantic terminal per aggregate kind",
    );
    assert!(
        matches!(
            &command.terminals()[0],
            TypedSqlGlobalAggregateTerminal::SumExpr { input_expr, distinct: false }
                if *input_expr == Expr::Literal(Value::Decimal(crate::types::Decimal::from(6_u64)))
        ),
        "SUM(2 * 3) should fold onto the canonical SUM(6) terminal",
    );
    assert!(
        matches!(
            &command.terminals()[1],
            TypedSqlGlobalAggregateTerminal::AvgExpr { input_expr, distinct: false }
                if *input_expr == Expr::Literal(Value::Decimal(crate::types::Decimal::from(6_u64)))
        ),
        "AVG(ROUND(2 * 3, 1)) should fold onto the canonical AVG(6) terminal",
    );
    assert_eq!(
        command.output_remap(),
        &[0, 0, 1, 1],
        "constant-folded aggregate outputs should remap to the first-seen folded terminal order",
    );
}

#[test]
fn compile_sql_command_accepts_grouped_aggregate_input_expressions() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age + 1) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate input expressions should lower once grouped runtime widens");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };
    let planned = query
        .plan()
        .expect("grouped aggregate input SQL should plan")
        .into_inner();
    let grouped = planned
        .grouped_plan()
        .expect("grouped aggregate input SQL should keep grouped plan shape");
    let aggregate = grouped
        .group
        .aggregates
        .first()
        .expect("grouped aggregate input SQL should declare one aggregate");

    assert_eq!(aggregate.target_field(), None);
    assert_eq!(
        aggregate.input_expr(),
        Some(&Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Decimal(crate::types::Decimal::from(
                1_u64
            ),))),
        }),
        "grouped aggregate input SQL should preserve the canonical normalized aggregate input expression in grouped plan semantics",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_case_input_expressions() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(CASE WHEN age >= 21 THEN 1 ELSE 0 END) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE aggregate inputs should lower");
    let terminal = command.terminal();

    assert!(
        matches!(
            terminal,
            TypedSqlGlobalAggregateTerminal::SumExpr { input_expr, distinct: false }
                if matches!(
                    input_expr,
                    Expr::Case {
                        when_then_arms,
                        else_expr,
                    }
                        if when_then_arms.as_slice() == [CaseWhenArm::new(
                            Expr::Binary {
                                op: BinaryOp::Gte,
                                left: Box::new(Expr::Field(FieldId::new("age"))),
                                right: Box::new(Expr::Literal(Value::Decimal(
                                    crate::types::Decimal::from(21_u64),
                                ))),
                            },
                            Expr::Literal(Value::Decimal(crate::types::Decimal::from(1_u64))),
                        )]
                            && else_expr.as_ref()
                                == &Expr::Literal(Value::Decimal(crate::types::Decimal::from(
                                    0_u64,
                                )))
                )
        ),
        "searched CASE aggregate inputs should lower through the shared pre-aggregate expression seam: {terminal:?}",
    );
}

fn compile_prepared_sql_scalar_strategy(sql: &str) -> PreparedSqlScalarAggregateStrategy {
    let command =
        compile_sql_global_aggregate_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .expect("typed scalar aggregate SQL should lower");

    match command.terminal() {
        TypedSqlGlobalAggregateTerminal::CountRows => {
            PreparedSqlScalarAggregateStrategy::from_resolved_shape(
                None,
                None,
                false,
                PreparedSqlScalarAggregateDescriptorShape::CountRows,
            )
        }
        TypedSqlGlobalAggregateTerminal::CountField {
            target_slot,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            Some(target_slot.clone()),
            None,
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::CountField,
        ),
        TypedSqlGlobalAggregateTerminal::CountExpr {
            input_expr,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            None,
            Some(input_expr.clone()),
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::CountField,
        ),
        TypedSqlGlobalAggregateTerminal::SumField {
            target_slot,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            Some(target_slot.clone()),
            None,
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::SumField,
        ),
        TypedSqlGlobalAggregateTerminal::SumExpr {
            input_expr,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            None,
            Some(input_expr.clone()),
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::SumField,
        ),
        TypedSqlGlobalAggregateTerminal::AvgField {
            target_slot,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            Some(target_slot.clone()),
            None,
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::AvgField,
        ),
        TypedSqlGlobalAggregateTerminal::AvgExpr {
            input_expr,
            distinct,
        } => PreparedSqlScalarAggregateStrategy::from_resolved_shape(
            None,
            Some(input_expr.clone()),
            *distinct,
            PreparedSqlScalarAggregateDescriptorShape::AvgField,
        ),
        TypedSqlGlobalAggregateTerminal::MinField(target_slot) => {
            PreparedSqlScalarAggregateStrategy::from_resolved_shape(
                Some(target_slot.clone()),
                None,
                false,
                PreparedSqlScalarAggregateDescriptorShape::MinField,
            )
        }
        TypedSqlGlobalAggregateTerminal::MinExpr { input_expr } => {
            PreparedSqlScalarAggregateStrategy::from_resolved_shape(
                None,
                Some(input_expr.clone()),
                false,
                PreparedSqlScalarAggregateDescriptorShape::MinField,
            )
        }
        TypedSqlGlobalAggregateTerminal::MaxField(target_slot) => {
            PreparedSqlScalarAggregateStrategy::from_resolved_shape(
                Some(target_slot.clone()),
                None,
                false,
                PreparedSqlScalarAggregateDescriptorShape::MaxField,
            )
        }
        TypedSqlGlobalAggregateTerminal::MaxExpr { input_expr } => {
            PreparedSqlScalarAggregateStrategy::from_resolved_shape(
                None,
                Some(input_expr.clone()),
                false,
                PreparedSqlScalarAggregateDescriptorShape::MaxField,
            )
        }
    }
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_count_rows() {
    let count_rows_strategy =
        compile_prepared_sql_scalar_strategy("SELECT COUNT(*) FROM SqlLowerEntity");

    assert_eq!(
        count_rows_strategy.domain(),
        PreparedSqlScalarAggregateDomain::ExistingRows,
        "COUNT(*) should prepare as an existing-rows aggregate domain",
    );
    assert_eq!(
        count_rows_strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::CountRows,
        "COUNT(*) should prepare the count-rows descriptor shape",
    );
    assert_eq!(
        count_rows_strategy.row_source(),
        PreparedSqlScalarAggregateRowSource::ExistingRows,
        "COUNT(*) should keep existing-row source semantics",
    );
    assert_eq!(
        count_rows_strategy.ordering_requirement(),
        PreparedSqlScalarAggregateOrderingRequirement::None,
        "COUNT(*) should not require field-order semantics",
    );
    assert_eq!(
        count_rows_strategy.empty_set_behavior(),
        PreparedSqlScalarAggregateEmptySetBehavior::Zero,
        "COUNT(*) should preserve zero-on-empty semantics",
    );
    assert_eq!(
        count_rows_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::CountRows,
        "COUNT(*) should project the count-rows runtime descriptor",
    );
    assert!(
        count_rows_strategy.target_slot().is_none(),
        "COUNT(*) should not require a target slot",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_count_field() {
    let count_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT COUNT(age) FROM SqlLowerEntity");

    assert_eq!(
        count_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::ProjectionField,
        "COUNT(field) should prepare through the projection-field domain",
    );
    assert_eq!(
        count_field_strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::CountField,
        "COUNT(field) should prepare the count-field descriptor shape",
    );
    assert_eq!(
        count_field_strategy.row_source(),
        PreparedSqlScalarAggregateRowSource::ProjectedField,
        "COUNT(field) should preserve projection-field row sourcing",
    );
    assert_eq!(
        count_field_strategy.empty_set_behavior(),
        PreparedSqlScalarAggregateEmptySetBehavior::Zero,
        "COUNT(field) should preserve zero-on-empty semantics",
    );
    assert_eq!(
        count_field_strategy
            .target_slot()
            .expect("COUNT(field) should keep target slot")
            .field(),
        "age",
        "COUNT(field) should keep the canonical resolved target slot",
    );
    assert_eq!(
        count_field_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::CountField,
        "COUNT(field) should project the count-field runtime descriptor",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_count_distinct_field() {
    let count_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT COUNT(DISTINCT age) FROM SqlLowerEntity");

    assert_eq!(
        count_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::ProjectionField,
        "COUNT(DISTINCT field) should still prepare through the projection-field domain",
    );
    assert!(
        count_field_strategy.is_distinct(),
        "COUNT(DISTINCT field) should preserve distinct-input semantics on the prepared strategy",
    );
    assert_eq!(
        count_field_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::CountField,
        "COUNT(DISTINCT field) should keep the count-field runtime descriptor",
    );
    assert_eq!(
        count_field_strategy
            .target_slot()
            .expect("COUNT(DISTINCT field) should keep target slot")
            .field(),
        "age",
        "COUNT(DISTINCT field) should preserve the canonical target slot",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_sum_field() {
    let sum_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT SUM(age) FROM SqlLowerEntity");

    assert_eq!(
        sum_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::NumericField,
        "SUM(field) should prepare through the numeric domain",
    );
    assert_eq!(
        sum_field_strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::SumField,
        "SUM(field) should prepare the sum-field descriptor shape",
    );
    assert_eq!(
        sum_field_strategy.row_source(),
        PreparedSqlScalarAggregateRowSource::NumericField,
        "SUM(field) should preserve numeric-field row sourcing",
    );
    assert_eq!(
        sum_field_strategy.empty_set_behavior(),
        PreparedSqlScalarAggregateEmptySetBehavior::Null,
        "SUM(field) should preserve null-on-empty semantics",
    );
    assert_eq!(
        sum_field_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
            kind: AggregateKind::Sum,
        },
        "SUM(field) should project the numeric SUM runtime descriptor",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_sum_distinct_field() {
    let sum_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT SUM(DISTINCT age) FROM SqlLowerEntity");

    assert_eq!(
        sum_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::NumericField,
        "SUM(DISTINCT field) should prepare through the numeric domain",
    );
    assert!(
        sum_field_strategy.is_distinct(),
        "SUM(DISTINCT field) should preserve distinct-input semantics on the prepared strategy",
    );
    assert_eq!(
        sum_field_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
            kind: AggregateKind::Sum,
        },
        "SUM(DISTINCT field) should keep the numeric SUM runtime descriptor",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_min_field() {
    let min_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT MIN(age) FROM SqlLowerEntity");

    assert_eq!(
        min_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
        "MIN(field) should prepare through the scalar-extrema-value domain",
    );
    assert_eq!(
        min_field_strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::MinField,
        "MIN(field) should prepare the min-field descriptor shape",
    );
    assert_eq!(
        min_field_strategy.row_source(),
        PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
        "MIN(field) should preserve extremal-winner row sourcing",
    );
    assert_eq!(
        min_field_strategy.ordering_requirement(),
        PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
        "MIN(field) should keep field-order sensitivity explicit",
    );
    assert_eq!(
        min_field_strategy.empty_set_behavior(),
        PreparedSqlScalarAggregateEmptySetBehavior::Null,
        "MIN(field) should preserve null-on-empty semantics",
    );
    assert_eq!(
        min_field_strategy.runtime_descriptor(),
        PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
            kind: AggregateKind::Min,
        },
        "MIN(field) should project the extrema runtime descriptor",
    );
}

#[test]
fn compile_sql_global_aggregate_command_prepares_typed_scalar_strategy_for_min_distinct_field() {
    let min_field_strategy =
        compile_prepared_sql_scalar_strategy("SELECT MIN(DISTINCT age) FROM SqlLowerEntity");

    assert_eq!(
        min_field_strategy.domain(),
        PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
        "MIN(DISTINCT field) should keep the scalar-extrema-value domain",
    );
    assert_eq!(
        min_field_strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::MinField,
        "MIN(DISTINCT field) should lower to the existing MIN(field) descriptor shape",
    );
    assert_eq!(
        min_field_strategy.row_source(),
        PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
        "MIN(DISTINCT field) should preserve extremal-winner row sourcing",
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
            TypedSqlGlobalAggregateTerminal::SumField {
                target_slot,
                distinct: false,
            } if target_slot.field() == "age"
        ),
        "global aggregate SQL SUM terminal should preserve the canonical target slot",
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
    let sql_executable = PreparedExecutionPlan::from(
        sql_command
            .query()
            .plan()
            .expect("global aggregate SQL base executable plan"),
    );
    let fluent_executable =
        PreparedExecutionPlan::from(fluent_query.plan().expect("fluent scalar executable plan"));
    assert_eq!(sql_executable.mode(), fluent_executable.mode());
    assert_eq!(sql_executable.is_grouped(), fluent_executable.is_grouped());
    assert_eq!(sql_executable.access(), fluent_executable.access());
    assert_eq!(
        sql_executable.consistency(),
        fluent_executable.consistency()
    );
    assert_eq!(
        sql_executable
            .execution_family()
            .expect("global aggregate SQL base execution family"),
        fluent_executable
            .execution_family()
            .expect("fluent scalar execution family"),
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable family",
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
        "SELECT COUNT(*), age FROM SqlLowerEntity",
    ] {
        let err =
            compile_sql_global_aggregate_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
                .expect_err("unsupported global aggregate SQL shape should fail closed");

        assert!(
            matches!(err, SqlLoweringError::UnsupportedGlobalAggregateProjection),
            "unsupported global aggregate SQL shape should remain lowering-gated: {sql}",
        );
    }

    let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped SQL shape should stay out of the dedicated global aggregate lane");

    assert!(
        matches!(err, SqlLoweringError::GlobalAggregateDoesNotSupportGroupBy),
        "grouped SQL shape should fail through the grouped/global aggregate boundary specifically",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_global_aggregate_having() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity HAVING COUNT(*) > 1",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate lane should admit aggregate-only HAVING");

    assert!(
        command.having().is_some(),
        "global aggregate HAVING should lower onto the shared post-aggregate boolean contract",
    );
    assert_eq!(
        command.terminals(),
        &[TypedSqlGlobalAggregateTerminal::CountRows],
        "global aggregate HAVING should reuse the same unique terminal list instead of introducing a second aggregate lane",
    );
}

#[test]
fn compile_sql_global_aggregate_command_rejects_direct_field_global_having() {
    let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity HAVING age > 1",
        MissingRowPolicy::Ignore,
    )
    .expect_err("global aggregate HAVING should stay aggregate-only");

    assert!(
        matches!(err, SqlLoweringError::UnsupportedSelectHaving),
        "global aggregate HAVING should reject direct field references through the existing HAVING boundary",
    );
}

#[test]
fn compile_sql_global_aggregate_command_rejection_message_names_global_aggregate_list_support() {
    let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT MIN(age), name FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("mixed global aggregate and scalar projection should remain fail-closed");

    assert!(
        err.to_string()
            .contains("scalar wrappers over aggregate results"),
        "mixed aggregate rejection should name the admitted global aggregate list shape: {err}",
    );
}
