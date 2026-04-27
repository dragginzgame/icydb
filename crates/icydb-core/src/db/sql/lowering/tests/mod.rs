//! Module: db::sql::lowering::tests
//! Covers SQL lowering from parsed statements into structural query shapes.
//! Does not own: production SQL lowering behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        executor::PreparedExecutionPlan,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteSpec, QueryMode,
            expr::{
                BinaryOp, CaseWhenArm, Expr, FieldId, Function, ProjectionField,
                canonicalize_grouped_having_bool_expr, canonicalize_scalar_where_bool_expr,
            },
        },
        query::{builder::FieldRef, expr::FilterExpr, intent::Query},
        sql::{
            lowering::{
                PreparedSqlScalarAggregateDescriptorShape, PreparedSqlScalarAggregateDomain,
                PreparedSqlScalarAggregateEmptySetBehavior,
                PreparedSqlScalarAggregateOrderingRequirement, PreparedSqlScalarAggregateRowSource,
                PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
                SqlCommand, SqlLoweringError, compile_sql_command,
                compile_sql_global_aggregate_command, lower_sql_command_from_prepared_statement,
                parse_grouped_post_aggregate_order_expr, parse_supported_order_expr,
                prepare_sql_statement,
            },
            parser::{
                SqlAggregateCall, SqlAggregateKind, SqlExplainMode, SqlExpr, SqlExprBinaryOp,
                SqlParseError, parse_sql,
            },
        },
    },
    model::field::FieldKind,
    model::index::{IndexExpression, IndexKeyItem, IndexModel},
    traits::{EntitySchema, Path},
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct SqlLowerBoolEntity {
    id: Ulid,
    label: String,
    active: bool,
    archived: bool,
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
        ("name", FieldKind::Text { max_len: None }),
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
        ("name", FieldKind::Text { max_len: None }),
        ("age", FieldKind::Uint),
    ],
    indexes = [&SQL_LOWER_EXPRESSION_INDEX_MODELS[0]],
    store = SqlLowerDataStore,
    canister = SqlLowerCanister,
}

crate::test_entity_schema! {
    ident = SqlLowerBoolEntity,
    id = Ulid,
    entity_name = "SqlLowerBoolEntity",
    entity_tag = crate::types::EntityTag::new(0x1039),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("label", FieldKind::Text { max_len: None }),
        ("active", FieldKind::Bool),
        ("archived", FieldKind::Bool),
    ],
    indexes = [],
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
        .rendered_label()
}

// Build one expected planner field expression for SQL order-lowering parser
// tests without reaching back into query/plan parser helpers.
fn lowered_field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

// Build one expected planner integer literal for SQL order-lowering parser
// tests.
fn lowered_int(value: i64) -> Expr {
    Expr::Literal(Value::Int(value))
}

// Build one expected planner unsigned integer literal for SQL order-lowering
// parser tests.
fn lowered_uint(value: u64) -> Expr {
    Expr::Literal(Value::Uint(value))
}

// Build one expected planner scalar function expression for SQL order-lowering
// parser tests.
fn lowered_function(function: Function, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall { function, args }
}

// Build one expected planner binary expression for SQL order-lowering parser
// tests.
fn lowered_binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// Build one expected planner aggregate expression for SQL order-lowering
// parser tests.
fn lowered_aggregate(kind: AggregateKind, input: Expr) -> Expr {
    Expr::Aggregate(
        crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(kind, input),
    )
}

// Lower one SQL command through the shared reduced SQL lane and extract the
// typed query shell so parity tests do not repeat command unwrap boilerplate.
fn compile_sql_lower_query_command(sql: &str, context: &str) -> Query<SqlLowerEntity> {
    let sql_command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
        .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"));
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("{context} should lower to a query command");
    };

    sql_query
}

#[test]
fn sql_order_expr_parser_lowers_scalar_order_terms_to_semantic_expr() {
    let expr = parse_supported_order_expr("ROUND((age + rank) / (age + 1), 2)")
        .expect("scalar SQL ORDER BY expression should lower");

    assert_eq!(
        expr,
        lowered_function(
            Function::Round,
            vec![
                lowered_binary(
                    BinaryOp::Div,
                    lowered_binary(BinaryOp::Add, lowered_field("age"), lowered_field("rank")),
                    lowered_binary(BinaryOp::Add, lowered_field("age"), lowered_int(1)),
                ),
                lowered_uint(2),
            ],
        ),
        "SQL ORDER BY token parsing should stay in parser while lowering preserves the semantic planner expression",
    );
}

#[test]
fn sql_order_expr_parser_lowers_grouped_aggregate_order_terms_to_semantic_expr() {
    let expr = parse_grouped_post_aggregate_order_expr("ROUND(AVG(rank + score), 2)")
        .expect("grouped SQL ORDER BY expression should lower");

    assert_eq!(
        expr,
        lowered_function(
            Function::Round,
            vec![
                lowered_aggregate(
                    AggregateKind::Avg,
                    lowered_binary(BinaryOp::Add, lowered_field("rank"), lowered_field("score"),),
                ),
                lowered_uint(2),
            ],
        ),
        "grouped SQL ORDER BY parsing should preserve aggregate input expression structure",
    );
}

#[test]
fn sql_order_expr_parser_lowers_grouped_filtered_aggregate_order_terms_to_semantic_expr() {
    let expr =
        parse_grouped_post_aggregate_order_expr("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))")
            .expect("filtered grouped SQL ORDER BY expression should lower");

    assert_eq!(
        expr,
        Expr::Aggregate(
            crate::db::query::builder::aggregate::count().with_filter_expr(lowered_function(
                Function::IsNotNull,
                vec![lowered_field("guild_rank")],
            )),
        ),
        "grouped SQL ORDER BY parsing should preserve aggregate FILTER semantics",
    );
}

// Lower one SQL SELECT statement just through the normalized frontend lane so
// shape tests can inspect grouped keys and ORDER BY terms before planner
// validation runs.
fn lower_sql_select_shape_for_test(
    sql: &str,
    context: &str,
) -> crate::db::sql::lowering::LoweredSelectShape {
    let statement = crate::db::sql::parser::parse_sql(sql)
        .unwrap_or_else(|err| panic!("{context} should parse: {err:?}"));
    let prepared = prepare_sql_statement(statement, SqlLowerEntity::MODEL.name())
        .unwrap_or_else(|err| panic!("{context} should prepare: {err:?}"));
    let lowered = lower_sql_command_from_prepared_statement(prepared, SqlLowerEntity::MODEL)
        .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"));
    let Some(crate::db::sql::lowering::LoweredSqlQuery::Select(select)) = lowered.into_query()
    else {
        panic!("{context} should lower to one SELECT query shape");
    };

    select
}

// Lower one global aggregate SQL command through the shared reduced SQL lane
// so aggregate tests do not each repeat the same typed lowering shell.
fn compile_sql_lower_global_aggregate_command(
    sql: &str,
    context: &str,
) -> crate::db::sql::lowering::SqlGlobalAggregateCommand<SqlLowerEntity> {
    compile_sql_global_aggregate_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
        .unwrap_or_else(|err| panic!("{context} should lower: {err:?}"))
}

// Strip semantic scalar filter ownership when parity tests only care about the
// canonical predicate/access/runtime contract shared across front doors.
fn strip_semantic_filter_expr_for_parity(mut plan: AccessPlannedQuery) -> AccessPlannedQuery {
    plan.scalar_plan_mut().filter_expr = None;
    plan.scalar_plan_mut().predicate_covers_filter_expr = false;

    plan
}

// Compare two typed query shells through the normalized planned intent so SQL
// parity tests can share one plan-equivalence assertion path.
fn assert_sql_lower_queries_share_plan_identity(
    left: &Query<SqlLowerEntity>,
    left_context: &str,
    right: &Query<SqlLowerEntity>,
    right_context: &str,
    message: &str,
) {
    assert_eq!(
        strip_semantic_filter_expr_for_parity(
            left.plan()
                .unwrap_or_else(|err| panic!("{left_context} plan should build: {err:?}"))
                .into_inner(),
        ),
        strip_semantic_filter_expr_for_parity(
            right
                .plan()
                .unwrap_or_else(|err| panic!("{right_context} plan should build: {err:?}"))
                .into_inner(),
        ),
        "{message}",
    );
}

// Compare two typed query shells through their deterministic query hash so SQL
// parity tests can share one fingerprint-equivalence assertion path.
fn assert_sql_lower_queries_share_plan_hash(
    left: &Query<SqlLowerEntity>,
    left_context: &str,
    right: &Query<SqlLowerEntity>,
    right_context: &str,
    message: &str,
) {
    assert_eq!(
        left.plan_hash_hex()
            .unwrap_or_else(|err| panic!("{left_context} plan hash should build: {err:?}")),
        right
            .plan_hash_hex()
            .unwrap_or_else(|err| panic!("{right_context} plan hash should build: {err:?}")),
        "{message}",
    );
}

// Compare two typed query shells at the structural query-cache boundary so
// SQL canonicalization tests exercise the exact input identity used before
// access planning or executor preparation.
fn assert_sql_lower_queries_share_structural_cache_key(
    left: &Query<SqlLowerEntity>,
    right: &Query<SqlLowerEntity>,
    message: &str,
) {
    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "{message}",
    );
}

// Lower two SQL query shells and assert their pre-planning structural query
// cache keys are identical. This keeps SQL syntax-convergence coverage on the
// same semantic identity boundary used by shared query-plan caching.
fn assert_sql_lower_queries_share_structural_cache_key_for_sql(
    left_sql: &str,
    left_context: &str,
    right_sql: &str,
    right_context: &str,
    message: &str,
) {
    let left_query = compile_sql_lower_query_command(left_sql, left_context);
    let right_query = compile_sql_lower_query_command(right_sql, right_context);

    assert_sql_lower_queries_share_structural_cache_key(&left_query, &right_query, message);
}

// Compare two typed query shells through their prepared execution contracts so
// parity tests can share one route/runtime identity assertion path.
fn assert_sql_lower_queries_share_executable_identity(
    left: &Query<SqlLowerEntity>,
    left_context: &str,
    right: &Query<SqlLowerEntity>,
    right_context: &str,
    family_message: &str,
    ordering_message: &str,
) {
    let left_executable = PreparedExecutionPlan::from(
        left.plan()
            .unwrap_or_else(|err| panic!("{left_context} executable plan should build: {err:?}")),
    );
    let right_executable = PreparedExecutionPlan::from(
        right
            .plan()
            .unwrap_or_else(|err| panic!("{right_context} executable plan should build: {err:?}")),
    );

    assert_eq!(left_executable.mode(), right_executable.mode());
    assert_eq!(left_executable.is_grouped(), right_executable.is_grouped());
    assert_eq!(left_executable.access(), right_executable.access());
    assert_eq!(
        left_executable.consistency(),
        right_executable.consistency()
    );
    assert_eq!(
        left_executable
            .execution_family()
            .unwrap_or_else(|err| panic!("{left_context} execution family should build: {err:?}")),
        right_executable.execution_family().unwrap_or_else(|err| {
            panic!("{right_context} execution family should build: {err:?}")
        }),
        "{family_message}",
    );
    assert_eq!(
        left_executable.execution_ordering().unwrap_or_else(|err| {
            panic!("{left_context} execution ordering should build: {err:?}")
        }),
        right_executable.execution_ordering().unwrap_or_else(|err| {
            panic!("{right_context} execution ordering should build: {err:?}")
        }),
        "{ordering_message}",
    );
}

// Lower one SQL query shell and compare it to the equivalent fluent query
// through the normalized planned intent so parity tests can share one path.
fn assert_sql_lower_query_matches_fluent_plan(
    sql: &str,
    sql_context: &str,
    fluent_query: &Query<SqlLowerEntity>,
    fluent_context: &str,
    message: &str,
) {
    let sql_query = compile_sql_lower_query_command(sql, sql_context);

    assert_sql_lower_queries_share_plan_identity(
        &sql_query,
        sql_context,
        fluent_query,
        fluent_context,
        message,
    );
}

// Lower two SQL query shells and compare them through the normalized planned
// intent so SQL-only parity tests can reuse the same assertion path.
fn assert_sql_lower_query_matches_sql_plan(
    left_sql: &str,
    left_context: &str,
    right_sql: &str,
    right_context: &str,
    message: &str,
) {
    let left_query = compile_sql_lower_query_command(left_sql, left_context);
    let right_query = compile_sql_lower_query_command(right_sql, right_context);

    assert_sql_lower_queries_share_plan_identity(
        &left_query,
        left_context,
        &right_query,
        right_context,
        message,
    );
}

// Lower one SQL query shell and assert its normalized plan builds so structural
// admission tests do not repeat the same query extraction boilerplate.
fn assert_sql_lower_query_plan_builds(sql: &str, context: &str) {
    compile_sql_lower_query_command(sql, context)
        .plan()
        .unwrap_or_else(|err| panic!("{context} plan should build: {err:?}"));
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
fn compile_sql_command_select_preserves_scalar_where_filter_expr_ownership() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity WHERE age >= 21",
        "scalar WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("scalar WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Gte,
                left,
                right,
            }) if left.as_ref() == &Expr::Field(FieldId::new("age"))
                && right.as_ref() == &Expr::Literal(Value::Uint(21))
        ),
        "reduced SQL lowering should preserve one planner-owned scalar WHERE expression and keep direct compare literals canonicalized to the resolved field kind",
    );
    assert!(
        plan.scalar_plan().predicate.is_some(),
        "the current 0.100 slice should still derive the existing predicate contract for access planning and runtime fast paths",
    );
}

#[test]
fn compile_sql_command_equivalent_compare_orderings_share_structural_cache_key() {
    assert_sql_lower_queries_share_structural_cache_key_for_sql(
        "SELECT * FROM SqlLowerEntity WHERE age >= 21 ORDER BY name ASC LIMIT 3",
        "field-leading compare SQL",
        "SELECT * FROM SqlLowerEntity WHERE 21 <= age ORDER BY name ASC LIMIT 3",
        "literal-leading compare SQL",
        "field-leading and literal-leading compares should lower to the same structural query cache key",
    );
}

#[test]
fn compile_sql_command_equivalent_function_predicates_share_structural_cache_key() {
    assert_sql_lower_queries_share_structural_cache_key_for_sql(
        "SELECT * FROM SqlLowerEntity WHERE name LIKE 'Al%' ORDER BY age DESC LIMIT 2",
        "LIKE prefix SQL",
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(name, 'Al') ORDER BY age DESC LIMIT 2",
        "direct STARTS_WITH SQL",
        "LIKE prefix syntax and direct STARTS_WITH should lower to the same structural query cache key",
    );
}

#[test]
fn compile_sql_command_alias_qualified_identifiers_share_structural_cache_key() {
    assert_sql_lower_queries_share_structural_cache_key_for_sql(
        "SELECT e.name FROM SqlLowerEntity e WHERE e.age >= 21 ORDER BY e.name ASC LIMIT 5",
        "alias-qualified SQL",
        "SELECT name FROM SqlLowerEntity WHERE age >= 21 ORDER BY name ASC LIMIT 5",
        "canonical unqualified SQL",
        "alias-qualified field references should normalize away before structural query cache identity is built",
    );
}

#[test]
fn compile_sql_command_equivalent_boolean_predicates_share_structural_cache_key() {
    assert_sql_lower_queries_share_structural_cache_key_for_sql(
        "SELECT * FROM SqlLowerEntity WHERE age >= 21 AND name = 'Ada' ORDER BY age ASC LIMIT 4",
        "age then name predicate SQL",
        "SELECT * FROM SqlLowerEntity WHERE name = 'Ada' AND age >= 21 ORDER BY age ASC LIMIT 4",
        "name then age predicate SQL",
        "commuted AND children should lower to the same structural query cache key",
    );
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
        .filter(FieldRef::new("age").eq(21_u64))
        .order_term(crate::db::asc("age"))
        .limit(1);

    assert_eq!(
        strip_semantic_filter_expr_for_parity(
            query.plan().expect("SQL plan should build").into_inner(),
        ),
        strip_semantic_filter_expr_for_parity(
            fluent_query
                .plan()
                .expect("fluent uint-equality plan should build")
                .into_inner(),
        ),
        "SQL uint equality should canonicalize its literal onto the strict runtime field variant",
    );
}

#[test]
fn compile_sql_command_typed_fluent_filter_matches_sql_canonical_predicate() {
    let sql_command = compile_sql_command::<SqlLowerEntity>(
        "SELECT * FROM SqlLowerEntity \
         WHERE (age >= 21 AND name = 'Ada') OR age = age",
        MissingRowPolicy::Ignore,
    )
    .expect("SQL filter convergence query should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered SQL query command");
    };
    let sql_plan = sql_query
        .plan()
        .expect("SQL filter convergence plan should build")
        .into_inner();
    let fluent_plan = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FilterExpr::or(vec![
            FilterExpr::and(vec![
                FilterExpr::gte("age", 21_i64),
                FilterExpr::eq("name", "Ada"),
            ]),
            FilterExpr::eq_field("age", "age"),
        ]))
        .plan()
        .expect("typed fluent filter convergence plan should build")
        .into_inner();

    assert_eq!(
        sql_plan.scalar_plan().predicate,
        fluent_plan.scalar_plan().predicate,
        "typed fluent filters and SQL WHERE lowering should produce the same canonical predicate",
    );
}

#[test]
fn compile_sql_command_typed_fluent_filter_matrix_matches_sql_canonical_predicate() {
    let cases = [
        (
            "SELECT * FROM SqlLowerEntity WHERE age >= 21",
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
                .filter(FieldRef::new("age").gte(21_i64)),
            "numeric widen compare",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE age > age",
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
                .filter(FieldRef::new("age").gt_field("age")),
            "field-to-field compare",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE age IN (10, 20, 30)",
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
                .filter(FieldRef::new("age").in_list([10_u64, 20_u64, 30_u64])),
            "membership compare",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE age IS NULL",
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
                .filter(FieldRef::new("age").is_null()),
            "null test",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE name ILIKE 'al%'",
            Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
                .filter(FieldRef::new("name").text_starts_with_ci("al")),
            "text casefold prefix",
        ),
    ];

    for (sql, fluent_query, context) in cases {
        let sql_command = compile_sql_command::<SqlLowerEntity>(sql, MissingRowPolicy::Ignore)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"));
        let SqlCommand::Query(sql_query) = sql_command else {
            panic!("expected lowered SQL query command for {context}");
        };
        let sql_plan = sql_query
            .plan()
            .unwrap_or_else(|err| panic!("{context} SQL plan should build: {err:?}"))
            .into_inner();
        let fluent_plan = fluent_query
            .plan()
            .unwrap_or_else(|err| panic!("{context} fluent plan should build: {err:?}"))
            .into_inner();

        assert_eq!(
            sql_plan.scalar_plan().predicate,
            fluent_plan.scalar_plan().predicate,
            "{context} should produce identical canonical predicates through SQL and typed fluent lowering",
        );
    }
}

#[test]
fn compile_sql_explain_numeric_equality_on_uint_field_keeps_strict_plan_parity() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN EXECUTION SELECT * FROM SqlLowerEntity WHERE age = 21 ORDER BY age ASC LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN EXECUTION with strict numeric equality on uint field should lower");

    let SqlCommand::Explain {
        mode,
        verbose: _,
        query,
    } = command
    else {
        panic!("expected lowered explain command");
    };
    assert_eq!(mode, SqlExplainMode::Execution);

    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("age").eq(21_u64))
        .order_term(crate::db::asc("age"))
        .limit(1);

    assert_eq!(
        strip_semantic_filter_expr_for_parity(
            query
                .plan()
                .expect("SQL explain query plan should build")
                .into_inner(),
        ),
        strip_semantic_filter_expr_for_parity(
            fluent_query
                .plan()
                .expect("fluent uint-equality plan should build")
                .into_inner(),
        ),
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
            "{context} should normalize onto the canonical scalar-function order expression",
        );
    }
}

#[test]
fn compile_sql_command_normalizes_order_by_alias_for_supported_scalar_numeric_targets() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT ABS(age) AS age_abs FROM SqlLowerEntity ORDER BY age_abs ASC LIMIT 2",
            "ABS(age)",
            "ORDER BY ABS alias",
        ),
        (
            "SELECT CEIL(age) AS age_ceil FROM SqlLowerEntity ORDER BY age_ceil ASC LIMIT 2",
            "CEILING(age)",
            "ORDER BY CEIL alias",
        ),
        (
            "SELECT CEILING(age) AS age_ceiling FROM SqlLowerEntity ORDER BY age_ceiling ASC LIMIT 2",
            "CEILING(age)",
            "ORDER BY CEILING alias",
        ),
        (
            "SELECT FLOOR(age) AS age_floor FROM SqlLowerEntity ORDER BY age_floor ASC LIMIT 2",
            "FLOOR(age)",
            "ORDER BY FLOOR alias",
        ),
    ] {
        assert_eq!(
            first_lowered_order_field(sql, context),
            expected_order_field,
            "{context} should normalize onto the canonical scalar-function order expression",
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
fn compile_sql_command_accepts_direct_scalar_function_expression_order_terms() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT age FROM SqlLowerEntity ORDER BY ABS(age - 30) ASC LIMIT 2",
            "ABS(age - 30)",
            "direct ORDER BY ABS expression terms",
        ),
        (
            "SELECT age FROM SqlLowerEntity ORDER BY COALESCE(NULLIF(age, 20), 99) DESC LIMIT 2",
            "COALESCE(NULLIF(age, 20), 99)",
            "direct ORDER BY COALESCE/NULLIF expression terms",
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
fn compile_sql_command_accepts_direct_unary_text_function_expression_order_terms() {
    for (sql, expected_order_field, context) in [
        (
            "SELECT name FROM SqlLowerEntity \
             ORDER BY LOWER(COALESCE(NULLIF(name, 'alpha'), 'zzz')) ASC LIMIT 2",
            "LOWER(COALESCE(NULLIF(name, 'alpha'), 'zzz'))",
            "direct ORDER BY LOWER/COALESCE/NULLIF expression terms",
        ),
        (
            "SELECT name FROM SqlLowerEntity ORDER BY LENGTH(TRIM(name)) DESC LIMIT 2",
            "LENGTH(TRIM(name))",
            "direct ORDER BY LENGTH/TRIM expression terms",
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
fn compile_sql_command_delete_wrapped_starts_with_family_matches_like_delete_intent() {
    let cases = [
        (
            "DELETE FROM SqlLowerEntity \
             WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), 'Al') \
             ORDER BY id ASC LIMIT 1",
            "DELETE FROM SqlLowerEntity \
             WHERE REPLACE(name, 'a', 'A') LIKE 'Al%' \
             ORDER BY id ASC LIMIT 1",
            "strict wrapped STARTS_WITH delete lowering",
        ),
        (
            "DELETE FROM SqlLowerEntity \
             WHERE STARTS_WITH(LOWER(REPLACE(name, 'a', 'A')), 'al') \
             ORDER BY id ASC LIMIT 1",
            "DELETE FROM SqlLowerEntity \
             WHERE REPLACE(name, 'a', 'A') ILIKE 'al%' \
             ORDER BY id ASC LIMIT 1",
            "casefold wrapped STARTS_WITH delete lowering",
        ),
    ];

    for (direct_sql, like_sql, context) in cases {
        let direct = compile_sql_command::<SqlLowerEntity>(direct_sql, MissingRowPolicy::Ignore)
            .expect("wrapped direct STARTS_WITH delete SQL should lower");
        let like = compile_sql_command::<SqlLowerEntity>(like_sql, MissingRowPolicy::Ignore)
            .expect("wrapped LIKE delete SQL should lower");

        let SqlCommand::Query(direct_query) = direct else {
            panic!("expected lowered query command for wrapped direct STARTS_WITH delete");
        };
        let SqlCommand::Query(like_query) = like else {
            panic!("expected lowered query command for wrapped LIKE delete");
        };

        assert!(
            matches!(direct_query.mode(), QueryMode::Delete(_)),
            "wrapped direct STARTS_WITH delete should stay on the delete query lane: {context}",
        );
        assert!(
            matches!(like_query.mode(), QueryMode::Delete(_)),
            "wrapped LIKE delete should stay on the delete query lane: {context}",
        );
        assert_eq!(
            direct_query
                .plan()
                .expect("wrapped direct STARTS_WITH delete plan should build")
                .into_inner(),
            like_query
                .plan()
                .expect("wrapped LIKE delete plan should build")
                .into_inner(),
            "wrapped direct STARTS_WITH delete lowering should match the widened LIKE/ILIKE delete intent: {context}",
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
    let Some(spec) = plan.access.as_index_range_path() else {
        panic!("expression-order query should use one index-range access path");
    };

    assert_eq!(
        spec.index().name(),
        SQL_LOWER_EXPRESSION_INDEX_MODELS[0].name()
    );
    assert!(
        spec.prefix_values().is_empty(),
        "order-only expression fallback should not invent equality prefix values",
    );
    assert_eq!(spec.lower(), &Bound::Unbounded);
    assert_eq!(spec.upper(), &Bound::Unbounded);
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
fn prepare_sql_statement_rejects_parameters_before_lowering() {
    let cases = [
        (
            "SELECT * FROM SqlLowerEntity WHERE age > ?",
            "SELECT WHERE parameter",
        ),
        (
            "SELECT ? FROM SqlLowerEntity",
            "SELECT projection parameter",
        ),
        (
            "SELECT COUNT(*) FILTER (WHERE age > ?) FROM SqlLowerEntity",
            "aggregate FILTER parameter",
        ),
        (
            "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age HAVING COUNT(*) > ?",
            "HAVING parameter",
        ),
        (
            "DELETE FROM SqlLowerEntity WHERE age > ?",
            "DELETE WHERE parameter",
        ),
        (
            "EXPLAIN SELECT * FROM SqlLowerEntity WHERE age > ?",
            "EXPLAIN target parameter",
        ),
        (
            "INSERT INTO SqlLowerEntity (id, name, age) SELECT id, name, age FROM SqlLowerEntity WHERE age > ?",
            "INSERT SELECT source parameter",
        ),
        (
            "UPDATE SqlLowerEntity SET age = 1 WHERE age > ?",
            "UPDATE WHERE parameter",
        ),
    ];

    for (sql, context) in cases {
        let statement =
            parse_sql(sql).unwrap_or_else(|err| panic!("{context} should parse: {err}"));
        let Err(err) = prepare_sql_statement(statement, SqlLowerEntity::MODEL.name()) else {
            panic!("{context} should fail during prepare");
        };

        assert!(
            matches!(err, SqlLoweringError::UnsupportedParameterPlacement { .. }),
            "{context} should be rejected by the prepared parameter contract: {err:?}",
        );
    }
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

    let SqlCommand::Explain {
        mode,
        verbose,
        query,
    } = command
    else {
        panic!("expected lowered explain command");
    };

    assert_eq!(mode, SqlExplainMode::Execution);
    assert!(!verbose);
    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_explain_execution_verbose_wraps_lowered_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN EXECUTION VERBOSE SELECT * FROM SqlLowerEntity LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN EXECUTION VERBOSE should lower");

    let SqlCommand::Explain {
        mode,
        verbose,
        query,
    } = command
    else {
        panic!("expected lowered explain command");
    };

    assert_eq!(mode, SqlExplainMode::Execution);
    assert!(verbose);
    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_explain_select_distinct_star_lowers_to_distinct_query() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "EXPLAIN SELECT DISTINCT * FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("EXPLAIN SELECT DISTINCT * should lower");

    let SqlCommand::Explain {
        mode,
        verbose: _,
        query,
    } = command
    else {
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

    let SqlCommand::Explain {
        mode,
        verbose: _,
        query,
    } = command
    else {
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

    let SqlCommand::ExplainGlobalAggregate {
        mode,
        verbose,
        command,
    } = command
    else {
        panic!("expected lowered explain global aggregate command");
    };

    assert_eq!(mode, SqlExplainMode::Plan);
    assert!(!verbose);
    assert_count_rows_strategy(command.terminal());
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
fn compile_sql_command_select_case_text_predicate_preserves_raw_target_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT CASE WHEN UPPER(name) LIKE 'AL%' THEN 1 ELSE 0 END FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE text predicate projection should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .unwrap_or_else(|err| {
            panic!("searched CASE text predicate projection plan should build: {err:?}")
        })
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
                        Expr::FunctionCall {
                            function: Function::StartsWith,
                            args: vec![
                                Expr::FunctionCall {
                                    function: Function::Upper,
                                    args: vec![Expr::Field(FieldId::new("name"))],
                                },
                                Expr::Literal(Value::Text("AL".to_string())),
                            ],
                        },
                        Expr::Literal(Value::Int(1)),
                    )]
                        && else_expr.as_ref() == &Expr::Literal(Value::Int(0))
                )
        ),
        "non-WHERE expression lowering must preserve the raw text predicate target",
    );
}

#[test]
fn compile_sql_command_select_searched_case_is_null_projection_lowers_to_case_expr() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT CASE WHEN name IS NULL THEN 'missing' ELSE name END FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE projection with IS NULL should lower");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered query command");
    };

    let projection = query
        .plan()
        .unwrap_or_else(|err| panic!("searched CASE IS NULL projection plan should build: {err:?}"))
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
                        Expr::FunctionCall {
                            function: Function::IsNull,
                            args: vec![Expr::Field(FieldId::new("name"))],
                        },
                        Expr::Literal(Value::Text("missing".to_string())),
                    )]
                        && else_expr.as_ref() == &Expr::Field(FieldId::new("name"))
                )
        ),
        "searched CASE IS NULL projection should lower onto one planner-owned CASE expression",
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
fn compile_sql_command_select_where_searched_case_matches_null_safe_canonical_filter_expr() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END",
        "searched CASE WHERE SQL query",
    );
    let sql_plan = sql_query
        .plan()
        .expect("searched CASE WHERE SQL plan should build")
        .into_inner();

    assert!(
        sql_plan.scalar_plan().predicate.is_none(),
        "null-safe searched CASE WHERE should stay expression-owned instead of claiming one derived predicate subset",
    );
    assert!(
        matches!(
            sql_plan.scalar_plan().filter_expr,
            Some(Expr::Binary {
                op: BinaryOp::Or,
                ref left,
                ref right,
            })
                if matches!(
                    left.as_ref(),
                    Expr::FunctionCall {
                        function: Function::Coalesce,
                        args,
                    }
                        if args.as_slice()
                            == [
                                Expr::Binary {
                                    op: BinaryOp::Gte,
                                    left: Box::new(Expr::Field(FieldId::new("age"))),
                                    right: Box::new(Expr::Literal(Value::Int(30))),
                                },
                                Expr::Literal(Value::Bool(false)),
                            ]
                )
                    && matches!(
                        right.as_ref(),
                        Expr::Binary {
                            op: BinaryOp::And,
                            left,
                            right,
                        }
                            if matches!(
                                left.as_ref(),
                                Expr::Unary {
                                    op: crate::db::query::plan::expr::UnaryOp::Not,
                                    expr,
                                }
                                    if matches!(
                                        expr.as_ref(),
                                        Expr::FunctionCall {
                                            function: Function::Coalesce,
                                            args,
                                        }
                                            if args.as_slice()
                                                == [
                                                    Expr::Binary {
                                                        op: BinaryOp::Gte,
                                                        left: Box::new(Expr::Field(FieldId::new("age"))),
                                                        right: Box::new(Expr::Literal(Value::Int(30))),
                                                    },
                                                    Expr::Literal(Value::Bool(false)),
                                                ]
                                    )
                            )
                                && right.as_ref()
                                    == &Expr::Binary {
                                        op: BinaryOp::Eq,
                                        left: Box::new(Expr::Field(FieldId::new("age"))),
                                        right: Box::new(Expr::Literal(Value::Int(20))),
                                    }
                    )
        ),
        "searched CASE WHERE should lower onto the null-safe canonical first-match boolean filter expression",
    );
}

#[test]
fn compile_sql_command_select_where_affine_numeric_compare_matches_canonical_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gte,
            Value::Decimal(crate::types::Decimal::from(20_u64)),
            CoercionId::NumericWiden,
        )),
    );

    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity WHERE age + 1 >= 21",
        "affine numeric WHERE SQL query",
    );
    let mut sql_plan = sql_query
        .plan()
        .expect("affine numeric WHERE SQL plan should build")
        .into_inner();
    let mut fluent_plan = fluent_query
        .plan()
        .expect("canonical fluent WHERE plan should build")
        .into_inner();
    sql_plan.scalar_plan_mut().filter_expr = None;
    sql_plan.scalar_plan_mut().predicate_covers_filter_expr = false;
    fluent_plan.scalar_plan_mut().filter_expr = None;
    fluent_plan.scalar_plan_mut().predicate_covers_filter_expr = false;

    assert_eq!(
        sql_plan, fluent_plan,
        "simple field-plus-literal WHERE compares should still normalize onto the same canonical predicate intent once semantic filter ownership is ignored",
    );
}

#[test]
fn compile_sql_command_select_where_coalesce_and_nullif_preserves_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity WHERE COALESCE(NULLIF(age, 20), 99) = 99",
        "COALESCE/NULLIF WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("COALESCE/NULLIF WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if matches!(
                left.as_ref(),
                Expr::FunctionCall {
                    function: Function::Coalesce,
                    args,
                } if matches!(
                    args.as_slice(),
                    [
                        Expr::FunctionCall {
                            function: Function::NullIf,
                            args: nullif_args,
                        },
                        Expr::Literal(Value::Int(99)),
                    ] if matches!(
                        nullif_args.as_slice(),
                        [
                            Expr::Field(field),
                            Expr::Literal(Value::Int(20)),
                        ] if *field == FieldId::new("age")
                    )
                )
            ) && right.as_ref() == &Expr::Literal(Value::Int(99))
        ),
        "COALESCE/NULLIF WHERE should preserve the semantic planner-owned filter expression through SQL lowering",
    );
    assert!(
        plan.scalar_plan().predicate.is_none(),
        "COALESCE/NULLIF WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
    );
}

#[test]
fn compile_sql_command_select_where_compare_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity WHERE name = TRIM('alpha')",
        "compare constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("compare constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if left.as_ref() == &Expr::Field(FieldId::new("name"))
                && right.as_ref() == &Expr::Literal(Value::Text("alpha".to_string()))
        ),
        "compare constant arguments WHERE should preserve one folded planner-owned equality expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
            CoercionId::Strict,
        ))),
        "compare constant arguments WHERE should now derive the strict field-vs-literal predicate contract after literal-only folding",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_extractable_and_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity WHERE age = 20 AND name = 'alpha'",
        "left equivalent extractable AND WHERE SQL query",
        "SELECT * FROM SqlLowerEntity WHERE name = 'alpha' AND (age = 20)",
        "right equivalent extractable AND WHERE SQL query",
        "equivalent extractable AND WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_residual_and_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) AND age = 20",
        "left equivalent residual AND WHERE SQL query",
        "SELECT * FROM SqlLowerEntity \
         WHERE age = 20 AND (STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')))",
        "right equivalent residual AND WHERE SQL query",
        "equivalent residual AND WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_extractable_or_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity WHERE age = 20 OR name = 'alpha'",
        "left equivalent extractable OR WHERE SQL query",
        "SELECT * FROM SqlLowerEntity WHERE name = 'alpha' OR (age = 20)",
        "right equivalent extractable OR WHERE SQL query",
        "equivalent extractable OR WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_residual_or_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) OR age = 20",
        "left equivalent residual OR WHERE SQL query",
        "SELECT * FROM SqlLowerEntity \
         WHERE age = 20 OR (STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')))",
        "right equivalent residual OR WHERE SQL query",
        "equivalent residual OR WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_mixed_extractable_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE (age = 20 AND name = 'alpha') OR age = 30",
        "left equivalent mixed extractable WHERE SQL query",
        "SELECT * FROM SqlLowerEntity \
         WHERE age = 30 OR (name = 'alpha' AND age = 20)",
        "right equivalent mixed extractable WHERE SQL query",
        "equivalent mixed extractable WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_mixed_residual_shapes_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE (age = 20 AND STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))) OR name = 'alpha'",
        "left equivalent mixed residual WHERE SQL query",
        "SELECT * FROM SqlLowerEntity \
         WHERE name = 'alpha' OR (STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) AND age = 20)",
        "right equivalent mixed residual WHERE SQL query",
        "equivalent mixed residual WHERE SQL queries should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_duplicate_extractable_boolean_children_collapse() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity WHERE age = 20 AND age = 20",
        "duplicate extractable AND WHERE SQL query",
        "SELECT * FROM SqlLowerEntity WHERE age = 20",
        "canonical extractable WHERE SQL query",
        "duplicate extractable boolean children should collapse onto one canonical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_duplicate_residual_boolean_children_collapse() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) \
           OR STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
        "duplicate residual OR WHERE SQL query",
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
        "canonical residual WHERE SQL query",
        "duplicate residual boolean children should collapse onto one canonical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_extractable_compare_orientations_share_plan_identity()
 {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity WHERE 20 = age",
        "literal-left extractable compare WHERE SQL query",
        "SELECT * FROM SqlLowerEntity WHERE age = 20",
        "canonical extractable compare WHERE SQL query",
        "equivalent extractable compare orientations should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_equivalent_residual_compare_orientations_share_plan_identity() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT * FROM SqlLowerEntity WHERE 'AlphA' = REPLACE(name, 'a', 'A')",
        "literal-left residual compare WHERE SQL query",
        "SELECT * FROM SqlLowerEntity WHERE REPLACE(name, 'a', 'A') = 'AlphA'",
        "canonical residual compare WHERE SQL query",
        "equivalent residual compare orientations should normalize onto one identical planned filter shape",
    );
}

#[test]
fn compile_sql_command_select_where_casefold_compare_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) = TRIM('ALPHA')",
        "casefold compare constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("casefold compare constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if matches!(
                left.as_ref(),
                Expr::FunctionCall {
                    function: Function::Lower,
                    args,
                } if matches!(args.as_slice(), [Expr::Field(field)] if *field == FieldId::new("name"))
            ) && right.as_ref() == &Expr::Literal(Value::Text("ALPHA".to_string()))
        ),
        "casefold compare constant arguments WHERE should preserve the semantic LOWER(field) equality expression after literal-only folding",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ALPHA".to_string()),
            CoercionId::TextCasefold,
        ))),
        "casefold compare constant arguments WHERE should now derive the existing LOWER(field)-vs-literal predicate contract after literal-only folding",
    );
}

#[test]
fn compile_sql_command_select_where_compare_and_true_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE name = TRIM('alpha') AND NULLIF('alpha', 'alpha') IS NULL",
        "compare and true constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("compare and true constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if left.as_ref() == &Expr::Field(FieldId::new("name"))
                && right.as_ref() == &Expr::Literal(Value::Text("alpha".to_string()))
        ),
        "compare and true constant arguments WHERE should simplify back to one folded planner-owned equality expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
            CoercionId::Strict,
        ))),
        "compare and true constant arguments WHERE should recover the strict field-vs-literal predicate lane after boolean simplification",
    );
}

#[test]
fn compile_sql_command_select_where_compare_and_false_constant_arguments_derive_false_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE name = TRIM('alpha') AND NULLIF('alpha', 'alpha') IS NOT NULL",
        "compare and false constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("compare and false constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Literal(Value::Bool(false)))
        ),
        "compare and false constant arguments WHERE should simplify all the way down to one folded FALSE filter expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::False),
        "compare and false constant arguments WHERE should recover the existing FALSE derived predicate lane after boolean simplification",
    );
}

#[test]
fn compile_sql_command_select_where_compare_or_false_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE name = TRIM('alpha') OR NULLIF('alpha', 'alpha') IS NOT NULL",
        "compare or false constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("compare or false constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if left.as_ref() == &Expr::Field(FieldId::new("name"))
                && right.as_ref() == &Expr::Literal(Value::Text("alpha".to_string()))
        ),
        "compare or false constant arguments WHERE should simplify back to one folded planner-owned equality expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
            CoercionId::Strict,
        ))),
        "compare or false constant arguments WHERE should recover the strict field-vs-literal predicate lane after boolean simplification",
    );
}

#[test]
fn compile_sql_command_select_where_compare_or_true_constant_arguments_derive_true_filter_expr() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE name = TRIM('alpha') OR NULLIF('alpha', 'alpha') IS NULL",
        "compare or true constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("compare or true constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Literal(Value::Bool(true)))
        ),
        "compare or true constant arguments WHERE should simplify all the way down to one folded TRUE filter expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        None,
        "compare or true constant arguments WHERE should preserve the current TRUE predicate storage behavior",
    );
}

#[test]
fn compile_sql_command_select_where_null_test_constant_arguments_derive_boolean_predicates() {
    let cases = [
        (
            "SELECT * FROM SqlLowerEntity WHERE NULLIF('alpha', 'alpha') IS NULL",
            true,
            None,
            "constant null-test WHERE that folds to TRUE",
        ),
        (
            "SELECT * FROM SqlLowerEntity WHERE NULLIF('alpha', 'alpha') IS NOT NULL",
            false,
            Some(Predicate::False),
            "constant null-test WHERE that folds to FALSE",
        ),
    ];

    for (sql, expected_value, expected_predicate, context) in cases {
        let sql_query = compile_sql_lower_query_command(sql, context);
        let plan = sql_query
            .plan()
            .unwrap_or_else(|err| panic!("{context} SQL plan should build: {err:?}"))
            .into_inner();

        assert!(
            matches!(
                plan.scalar_plan().filter_expr.as_ref(),
                Some(Expr::Literal(Value::Bool(found))) if *found == expected_value
            ),
            "{context} should preserve one folded planner-owned boolean literal expression",
        );
        assert_eq!(
            plan.scalar_plan().predicate,
            expected_predicate.clone(),
            "{context} should preserve the current folded boolean predicate storage behavior",
        );
    }
}

#[test]
fn compile_sql_command_select_where_unary_text_wrapped_value_selection_preserves_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE LOWER(COALESCE(NULLIF(name, 'alpha'), 'zzz')) = 'zzz'",
        "unary text wrapped value-selection WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("unary text wrapped value-selection WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }) if matches!(
                left.as_ref(),
                Expr::FunctionCall {
                    function: Function::Lower,
                    args,
                } if matches!(
                    args.as_slice(),
                    [Expr::FunctionCall {
                        function: Function::Coalesce,
                        args: coalesce_args,
                    }] if matches!(
                        coalesce_args.as_slice(),
                        [
                            Expr::FunctionCall {
                                function: Function::NullIf,
                                args: nullif_args,
                            },
                            Expr::Literal(Value::Text(fallback)),
                        ] if fallback == "zzz"
                            && matches!(
                                nullif_args.as_slice(),
                                [
                                    Expr::Field(field),
                                    Expr::Literal(Value::Text(excluded)),
                                ] if *field == FieldId::new("name") && excluded == "alpha"
                            )
                    )
                )
            ) && right.as_ref() == &Expr::Literal(Value::Text("zzz".to_string()))
        ),
        "unary text wrappers should preserve the semantic planner-owned filter expression through SQL lowering",
    );
    assert!(
        plan.scalar_plan().predicate.is_none(),
        "unary text wrapped value-selection WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
    );
}

#[test]
fn compile_sql_command_select_where_text_transform_operands_preserve_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE REPLACE(name, 'a', 'A') = 'AlphA'",
        "text transform WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("text transform WHERE SQL plan should build")
        .into_inner();
    let filter_expr = plan
        .scalar_plan()
        .filter_expr
        .as_ref()
        .expect("text transform WHERE should preserve semantic filter ownership");
    let Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = filter_expr
    else {
        panic!("text transform WHERE should lower to an equality comparison");
    };
    let Expr::FunctionCall {
        function: Function::Replace,
        args: replace_args,
    } = left.as_ref()
    else {
        panic!("text transform WHERE left operand should stay on the REPLACE(...) expression seam");
    };
    let [
        Expr::Field(field),
        Expr::Literal(Value::Text(from)),
        Expr::Literal(Value::Text(to)),
    ] = replace_args.as_slice()
    else {
        panic!("text transform WHERE should preserve the REPLACE(field, from, to) argument order");
    };

    assert_eq!(field, &FieldId::new("name"));
    assert_eq!(from, "a");
    assert_eq!(to, "A");
    assert_eq!(
        right.as_ref(),
        &Expr::Literal(Value::Text("AlphA".to_string()))
    );

    assert!(
        plan.scalar_plan().predicate.is_none(),
        "text transform WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
    );
}

#[test]
fn compile_sql_command_select_where_text_predicate_wrapped_transform_preserves_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), 'Al')",
        "text predicate wrapped transform WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("text predicate wrapped transform WHERE SQL plan should build")
        .into_inner();
    let filter_expr =
        plan.scalar_plan().filter_expr.as_ref().expect(
            "text predicate wrapped transform WHERE should preserve semantic filter ownership",
        );

    let Expr::FunctionCall {
        function: Function::StartsWith,
        args,
    } = filter_expr
    else {
        panic!("text predicate wrapped transform WHERE should lower to STARTS_WITH(...)");
    };
    let [left, Expr::Literal(Value::Text(prefix))] = args.as_slice() else {
        panic!("text predicate wrapped transform WHERE should preserve one text literal prefix");
    };
    let Expr::FunctionCall {
        function: Function::Replace,
        args: replace_args,
    } = left
    else {
        panic!(
            "text predicate wrapped transform WHERE should preserve the nested REPLACE(...) operand"
        );
    };
    let [
        Expr::Field(field),
        Expr::Literal(Value::Text(from)),
        Expr::Literal(Value::Text(to)),
    ] = replace_args.as_slice()
    else {
        panic!(
            "text predicate wrapped transform WHERE should preserve the REPLACE(field, from, to) argument order"
        );
    };

    assert_eq!(field, &FieldId::new("name"));
    assert_eq!(from, "a");
    assert_eq!(to, "A");
    assert_eq!(prefix, "Al");
    assert!(
        plan.scalar_plan().predicate.is_none(),
        "text predicate wrapped transform WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
    );
}

#[test]
fn compile_sql_command_select_where_text_predicate_expression_arguments_preserve_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))",
        "text predicate expression arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("text predicate expression arguments WHERE SQL plan should build")
        .into_inner();
    let filter_expr = plan.scalar_plan().filter_expr.as_ref().expect(
        "text predicate expression arguments WHERE should preserve semantic filter ownership",
    );

    let Expr::FunctionCall {
        function: Function::StartsWith,
        args,
    } = filter_expr
    else {
        panic!("text predicate expression arguments WHERE should lower to STARTS_WITH(...)");
    };
    let [left, right] = args.as_slice() else {
        panic!("text predicate expression arguments WHERE should preserve two operands");
    };
    let Expr::FunctionCall {
        function: Function::Replace,
        args: replace_args,
    } = left
    else {
        panic!(
            "text predicate expression arguments WHERE should preserve the nested REPLACE(...) left operand"
        );
    };
    let [
        Expr::Field(field),
        Expr::Literal(Value::Text(from)),
        Expr::Literal(Value::Text(to)),
    ] = replace_args.as_slice()
    else {
        panic!(
            "text predicate expression arguments WHERE should preserve the REPLACE(field, from, to) argument order"
        );
    };
    let Expr::Literal(Value::Text(source)) = right else {
        panic!(
            "text predicate expression arguments WHERE should fold the literal-only TRIM(...) right operand before predicate admission"
        );
    };

    assert_eq!(field, &FieldId::new("name"));
    assert_eq!(from, "a");
    assert_eq!(to, "A");
    assert_eq!(source, "Al");
    assert!(
        plan.scalar_plan().predicate.is_none(),
        "text predicate expression arguments WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
    );
}

#[test]
fn compile_sql_command_select_where_text_predicate_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(name, TRIM('Al'))",
        "text predicate constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("text predicate constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::FunctionCall {
                function: Function::StartsWith,
                ..
            })
        ),
        "text predicate constant arguments WHERE should still preserve semantic filter ownership",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ))),
        "text predicate constant arguments WHERE should now derive the existing STARTS_WITH predicate contract after literal-only folding",
    );
}

#[test]
fn compile_sql_command_select_where_casefold_text_predicate_constant_arguments_derive_predicate() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(LOWER(name), TRIM('AL'))",
        "casefold text predicate constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("casefold text predicate constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::FunctionCall {
                function: Function::StartsWith,
                args,
            }) if matches!(
                args.as_slice(),
                [
                    Expr::FunctionCall {
                        function: Function::Lower,
                        args: lower_args,
                    },
                    Expr::Literal(Value::Text(prefix)),
                ] if matches!(lower_args.as_slice(), [Expr::Field(field)] if *field == FieldId::new("name"))
                    && prefix == "AL"
            )
        ),
        "casefold text predicate constant arguments WHERE should preserve semantic filter ownership around LOWER(field)",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ))),
        "casefold text predicate constant arguments WHERE should now derive the existing casefold STARTS_WITH predicate contract after literal-only folding",
    );
}

#[test]
fn compile_sql_command_select_where_casefold_text_predicate_and_true_constant_arguments_derive_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(LOWER(name), TRIM('AL')) \
           AND NULLIF('alpha', 'alpha') IS NULL",
        "casefold text predicate and true constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("casefold text predicate and true constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::FunctionCall {
                function: Function::StartsWith,
                args,
            }) if matches!(
                args.as_slice(),
                [
                    Expr::FunctionCall {
                        function: Function::Lower,
                        args: lower_args,
                    },
                    Expr::Literal(Value::Text(prefix)),
                ] if matches!(lower_args.as_slice(), [Expr::Field(field)] if *field == FieldId::new("name"))
                    && prefix == "AL"
            )
        ),
        "casefold text predicate and true constant arguments WHERE should simplify back to one folded STARTS_WITH semantic filter expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ))),
        "casefold text predicate and true constant arguments WHERE should recover the casefold STARTS_WITH predicate lane after boolean simplification",
    );
}

#[test]
fn compile_sql_command_select_where_casefold_text_predicate_or_false_constant_arguments_derive_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE STARTS_WITH(LOWER(name), TRIM('AL')) \
           OR NULLIF('alpha', 'alpha') IS NOT NULL",
        "casefold text predicate or false constant arguments WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("casefold text predicate or false constant arguments WHERE SQL plan should build")
        .into_inner();

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::FunctionCall {
                function: Function::StartsWith,
                args,
            }) if matches!(
                args.as_slice(),
                [
                    Expr::FunctionCall {
                        function: Function::Lower,
                        args: lower_args,
                    },
                    Expr::Literal(Value::Text(prefix)),
                ] if matches!(lower_args.as_slice(), [Expr::Field(field)] if *field == FieldId::new("name"))
                    && prefix == "AL"
            )
        ),
        "casefold text predicate or false constant arguments WHERE should simplify back to one folded STARTS_WITH semantic filter expression",
    );
    assert_eq!(
        plan.scalar_plan().predicate,
        Some(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ))),
        "casefold text predicate or false constant arguments WHERE should recover the casefold STARTS_WITH predicate lane after boolean simplification",
    );
}

#[test]
fn compile_sql_command_select_where_ilike_wrapped_transform_preserves_filter_expr_with_fallback_predicate()
 {
    let sql_query = compile_sql_lower_query_command(
        "SELECT * FROM SqlLowerEntity \
         WHERE REPLACE(name, 'a', 'A') ILIKE 'al%'",
        "ILIKE wrapped transform WHERE SQL query",
    );
    let plan = sql_query
        .plan()
        .expect("ILIKE wrapped transform WHERE SQL plan should build")
        .into_inner();
    let filter_expr = plan
        .scalar_plan()
        .filter_expr
        .as_ref()
        .expect("ILIKE wrapped transform WHERE should preserve semantic filter ownership");

    let Expr::FunctionCall {
        function: Function::StartsWith,
        args,
    } = filter_expr
    else {
        panic!("ILIKE wrapped transform WHERE should lower to STARTS_WITH(...)");
    };
    let [left, Expr::Literal(Value::Text(prefix))] = args.as_slice() else {
        panic!("ILIKE wrapped transform WHERE should preserve one text literal prefix");
    };
    let Expr::FunctionCall {
        function: Function::Lower,
        args: lower_args,
    } = left
    else {
        panic!("ILIKE wrapped transform WHERE should preserve LOWER(...) around the target");
    };
    let [
        Expr::FunctionCall {
            function: Function::Replace,
            args: replace_args,
        },
    ] = lower_args.as_slice()
    else {
        panic!("ILIKE wrapped transform WHERE should preserve the nested REPLACE(...) operand");
    };
    let [
        Expr::Field(field),
        Expr::Literal(Value::Text(from)),
        Expr::Literal(Value::Text(to)),
    ] = replace_args.as_slice()
    else {
        panic!(
            "ILIKE wrapped transform WHERE should preserve the REPLACE(field, from, to) argument order"
        );
    };

    assert_eq!(field, &FieldId::new("name"));
    assert_eq!(from, "a");
    assert_eq!(to, "A");
    assert_eq!(prefix, "al");
    assert!(
        plan.scalar_plan().predicate.is_none(),
        "ILIKE wrapped transform WHERE should currently fall back to residual filter execution instead of claiming one derived predicate shape",
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
fn compile_sql_command_select_where_searched_case_with_bool_field_stays_expression_owned_under_null_safe_canonicalization()
 {
    let sql_query = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT * FROM SqlLowerBoolEntity \
         WHERE CASE WHEN active THEN FALSE ELSE TRUE END",
        MissingRowPolicy::Ignore,
    )
    .expect("searched CASE bool-field WHERE SQL query should lower");
    let SqlCommand::Query(sql_query) = sql_query else {
        panic!("expected lowered searched CASE bool-field WHERE query command");
    };

    let sql_plan = sql_query
        .plan()
        .expect("searched CASE bool-field WHERE SQL plan should build")
        .into_inner();

    assert!(
        sql_plan.scalar_plan().predicate.is_none(),
        "null-safe searched CASE bool-field WHERE should stay expression-owned instead of collapsing onto the older predicate-only seam",
    );
    assert!(
        matches!(
            sql_plan.scalar_plan().filter_expr,
            Some(Expr::Binary {
                op: BinaryOp::Or,
                ref left,
                ref right,
            }) if left.as_ref() == &Expr::Literal(Value::Bool(false))
                && matches!(
                    right.as_ref(),
                    Expr::Unary {
                        op: crate::db::query::plan::expr::UnaryOp::Not,
                        expr,
                    }
                        if matches!(
                            expr.as_ref(),
                            Expr::FunctionCall {
                                function: Function::Coalesce,
                                args,
                            }
                                if args.as_slice()
                                    == [
                                        Expr::Field(FieldId::new("active")),
                                        Expr::Literal(Value::Bool(false)),
                                    ]
                        )
                )
        ),
        "null-safe searched CASE bool-field WHERE should lower onto the canonical COALESCE-backed boolean residual form",
    );
}

#[test]
fn compile_sql_command_select_where_is_true_matches_bare_bool_field_plan_identity() {
    let wrapped_query = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT * FROM SqlLowerBoolEntity WHERE active IS TRUE",
        MissingRowPolicy::Ignore,
    )
    .expect("IS TRUE bool-field SQL query should lower");
    let canonical_query = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT * FROM SqlLowerBoolEntity WHERE active",
        MissingRowPolicy::Ignore,
    )
    .expect("bare bool-field SQL query should lower");

    let SqlCommand::Query(wrapped_query) = wrapped_query else {
        panic!("expected lowered IS TRUE bool-field query command");
    };
    let SqlCommand::Query(canonical_query) = canonical_query else {
        panic!("expected lowered bare bool-field query command");
    };

    assert_eq!(
        wrapped_query
            .plan()
            .expect("IS TRUE bool-field SQL plan should build")
            .into_inner(),
        canonical_query
            .plan()
            .expect("bare bool-field SQL plan should build")
            .into_inner(),
        "IS TRUE should lower onto the same canonical scalar bool-field plan as the bare truth condition",
    );
    assert_eq!(
        wrapped_query
            .plan_hash_hex()
            .expect("IS TRUE bool-field plan hash should build"),
        canonical_query
            .plan_hash_hex()
            .expect("bare bool-field plan hash should build"),
        "IS TRUE should keep the same plan hash as the bare bool-field truth condition once planner wrapper canonicalization owns that family",
    );
}

#[test]
fn compile_sql_command_select_where_is_false_matches_not_bool_field_plan_identity() {
    let wrapped_query = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT * FROM SqlLowerBoolEntity WHERE active IS FALSE",
        MissingRowPolicy::Ignore,
    )
    .expect("IS FALSE bool-field SQL query should lower");
    let canonical_query = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT * FROM SqlLowerBoolEntity WHERE NOT active",
        MissingRowPolicy::Ignore,
    )
    .expect("NOT bool-field SQL query should lower");

    let SqlCommand::Query(wrapped_query) = wrapped_query else {
        panic!("expected lowered IS FALSE bool-field query command");
    };
    let SqlCommand::Query(canonical_query) = canonical_query else {
        panic!("expected lowered NOT bool-field query command");
    };

    assert_eq!(
        wrapped_query
            .plan()
            .expect("IS FALSE bool-field SQL plan should build")
            .into_inner(),
        canonical_query
            .plan()
            .expect("NOT bool-field SQL plan should build")
            .into_inner(),
        "IS FALSE should lower onto the same canonical scalar bool-field plan as NOT <bool field>",
    );
    assert_eq!(
        wrapped_query
            .plan_hash_hex()
            .expect("IS FALSE bool-field plan hash should build"),
        canonical_query
            .plan_hash_hex()
            .expect("NOT bool-field plan hash should build"),
        "IS FALSE should keep the same plan hash as NOT <bool field> once planner wrapper canonicalization owns that family",
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
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name", "age"])
        .filter(FieldRef::new("age").gte(21_i64))
        .order_term(crate::db::desc("age"))
        .limit(5)
        .offset(1);

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT SqlLowerEntity.name, SqlLowerEntity.age \
         FROM SqlLowerEntity \
         WHERE SqlLowerEntity.age >= 21 \
         ORDER BY SqlLowerEntity.age DESC LIMIT 5 OFFSET 1",
        "qualified field-list SQL query",
        &fluent_query,
        "unqualified fluent query",
        "qualified SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
    );
}

#[test]
fn compile_sql_command_select_table_alias_fields_parity_matches_unqualified_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name", "age"])
        .filter(FieldRef::new("age").gte(21_i64))
        .order_term(crate::db::desc("age"))
        .limit(5)
        .offset(1);

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT alias.name, alias.age \
         FROM SqlLowerEntity alias \
         WHERE alias.age >= 21 \
         ORDER BY alias.age DESC LIMIT 5 OFFSET 1",
        "table-alias field-list SQL query",
        &fluent_query,
        "unqualified fluent query",
        "single-table alias SQL field references should normalize to the same canonical planned intent as unqualified fluent references",
    );
}

#[test]
fn compile_sql_command_qualified_nested_predicate_matches_unqualified_fluent_intent() {
    let fluent_query =
        Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(FilterExpr::and(vec![
            FilterExpr::or(vec![
                FieldRef::new("age").gte(21_i64),
                FieldRef::new("name").eq("Ada"),
            ]),
            FilterExpr::not(FieldRef::new("name").eq("Bob")),
        ]));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity \
         WHERE (SqlLowerEntity.age >= 21 OR SqlLowerEntity.name = 'Ada') \
         AND NOT (SqlLowerEntity.name = 'Bob')",
        "qualified nested-predicate SQL query",
        &fluent_query,
        "unqualified fluent nested-predicate query",
        "qualified nested predicate identifiers should normalize to the same canonical planned intent as unqualified fluent predicates",
    );
}

#[test]
fn compile_sql_command_strict_like_prefix_parity_matches_strict_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with("Al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE name LIKE 'Al%'",
        "strict LIKE prefix SQL query",
        &fluent_query,
        "fluent strict starts-with query",
        "plain LIKE 'prefix%' SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_angle_bracket_not_equal_matches_canonical_ne_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").ne("Al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE name <> 'Al'",
        "angle-bracket not-equal SQL query",
        &fluent_query,
        "canonical fluent not-equal query",
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
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        FilterExpr::not(FieldRef::new("name").text_starts_with("Al")),
    );

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE name NOT LIKE 'Al%'",
        "strict NOT LIKE prefix SQL query",
        &fluent_query,
        "fluent negated strict starts-with query",
        "plain NOT LIKE 'prefix%' SQL lowering and fluent negated strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_ilike_prefix_matches_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with_ci("al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE name ILIKE 'al%'",
        "ILIKE prefix SQL query",
        &fluent_query,
        "fluent casefold starts-with query",
        "plain ILIKE 'prefix%' SQL lowering must match the canonical casefold starts-with intent",
    );
}

#[test]
fn compile_sql_command_not_ilike_prefix_matches_negated_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter(
        FilterExpr::not(FieldRef::new("name").text_starts_with_ci("al")),
    );

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE name NOT ILIKE 'al%'",
        "NOT ILIKE prefix SQL query",
        &fluent_query,
        "fluent negated casefold starts-with query",
        "plain NOT ILIKE 'prefix%' SQL lowering must match the canonical negated casefold starts-with intent",
    );
}

#[test]
fn compile_sql_command_direct_starts_with_parity_matches_strict_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with("Al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(name, 'Al')",
        "direct STARTS_WITH SQL query",
        &fluent_query,
        "fluent strict starts-with query",
        "direct STARTS_WITH SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_direct_lower_starts_with_parity_matches_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with_ci("Al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
        "direct LOWER(field) STARTS_WITH SQL query",
        &fluent_query,
        "fluent text-casefold starts-with query",
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
            FilterExpr::not(FieldRef::new("name").text_starts_with_ci(prefix)),
        );

        assert_sql_lower_queries_share_plan_identity(
            &sql_query,
            context,
            &fluent_query,
            context,
            &format!(
                "{context} and fluent negated casefold starts-with query must produce identical normalized planned intent"
            ),
        );
    }
}

#[test]
fn compile_sql_command_direct_upper_starts_with_parity_matches_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with_ci("AL"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
        "direct UPPER(field) STARTS_WITH SQL query",
        &fluent_query,
        "fluent text-casefold starts-with query",
        "direct UPPER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_lower_like_prefix_parity_matches_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with_ci("Al"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) LIKE 'Al%'",
        "LOWER(field) LIKE prefix SQL query",
        &fluent_query,
        "fluent text-casefold starts-with query",
        "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_upper_like_prefix_parity_matches_casefold_starts_with_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").text_starts_with_ci("AL"));

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE UPPER(name) LIKE 'AL%'",
        "UPPER(field) LIKE prefix SQL query",
        &fluent_query,
        "fluent text-casefold starts-with query",
        "UPPER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_lower_ordered_text_range_parity_matches_casefold_range_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::And(vec![
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
        ]),
    );

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE LOWER(name) >= 'Al' AND LOWER(name) < 'Am'",
        "LOWER(field) ordered text range SQL query",
        &fluent_query,
        "fluent text-casefold range query",
        "LOWER(field) ordered text range SQL lowering and fluent text-casefold range query must produce identical normalized planned intent",
    );
}

#[test]
fn compile_sql_command_upper_ordered_text_range_parity_matches_casefold_range_intent() {
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::And(vec![
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
        ]),
    );

    assert_sql_lower_query_matches_fluent_plan(
        "SELECT * FROM SqlLowerEntity WHERE UPPER(name) >= 'AL' AND UPPER(name) < 'AM'",
        "UPPER(field) ordered text range SQL query",
        &fluent_query,
        "fluent text-casefold range query",
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
    let query = compile_sql_lower_query_command(
        "SELECT * FROM public.SqlLowerEntity",
        "schema-qualified entity SQL",
    );

    assert!(matches!(query.mode(), QueryMode::Load(_)));
}

#[test]
fn compile_sql_command_global_aggregate_select_lowers_to_dedicated_command() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate projection should lower to the dedicated aggregate command");

    assert!(
        matches!(command, SqlCommand::GlobalAggregate(_)),
        "global aggregate SELECT should lower to the dedicated aggregate command instead of failing through the scalar query lane",
    );
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
fn lower_aggregate_call_attaches_filter_expr_to_aggregate_identity() {
    let aggregate = super::aggregate::lower_aggregate_call(SqlAggregateCall {
        kind: SqlAggregateKind::Count,
        input: None,
        filter_expr: Some(Box::new(SqlExpr::Binary {
            op: SqlExprBinaryOp::Gt,
            left: Box::new(SqlExpr::Field("age".to_string())),
            right: Box::new(SqlExpr::Literal(Value::Int(1))),
        })),
        distinct: false,
    })
    .expect("aggregate FILTER should lower onto aggregate identity");

    assert_eq!(aggregate.kind(), AggregateKind::Count);
    assert_eq!(
        aggregate.filter_expr(),
        Some(&Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        }),
    );
}

#[test]
fn lower_aggregate_call_rejects_distinct_filter_pairing_in_0940() {
    let err = super::aggregate::lower_aggregate_call(SqlAggregateCall {
        kind: SqlAggregateKind::Count,
        input: Some(Box::new(SqlExpr::Field("age".to_string()))),
        filter_expr: Some(Box::new(SqlExpr::Binary {
            op: SqlExprBinaryOp::Gt,
            left: Box::new(SqlExpr::Field("age".to_string())),
            right: Box::new(SqlExpr::Literal(Value::Int(1))),
        })),
        distinct: true,
    })
    .expect_err("DISTINCT + FILTER should stay fail-closed in 0.94.0");

    assert!(matches!(err, SqlLoweringError::UnsupportedSelectProjection));
}

#[test]
fn lower_aggregate_call_rejects_aggregate_predicates_inside_filter() {
    let err = super::aggregate::lower_aggregate_call(SqlAggregateCall {
        kind: SqlAggregateKind::Count,
        input: None,
        filter_expr: Some(Box::new(SqlExpr::Binary {
            op: SqlExprBinaryOp::Gt,
            left: Box::new(SqlExpr::Aggregate(SqlAggregateCall {
                kind: SqlAggregateKind::Count,
                input: None,
                filter_expr: None,
                distinct: false,
            })),
            right: Box::new(SqlExpr::Literal(Value::Int(1))),
        })),
        distinct: false,
    })
    .expect_err("aggregate expressions inside FILTER should stay fail-closed");

    assert!(matches!(
        err,
        SqlLoweringError::UnsupportedAggregateInputExpressions
    ));
}

#[test]
fn compile_sql_command_rejects_subqueries_inside_filter_predicates() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FILTER (WHERE (SELECT age FROM SqlLowerEntity) > 1) FROM SqlLowerEntity",
        MissingRowPolicy::Ignore,
    )
    .expect_err("subqueries inside FILTER should stay fail-closed before execution");

    assert!(matches!(err, SqlLoweringError::Parse(_)));
}

#[test]
fn compile_sql_command_rejects_grouped_filter_alias_references_before_execution() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, \
         COUNT(*) FILTER (WHERE total_count > 0) AS total_count \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect_err("grouped FILTER alias leakage should stay fail-closed before execution");

    assert!(matches!(
        err,
        SqlLoweringError::UnknownField { field } if field == "total_count"
    ));
}

#[test]
fn compile_sql_command_rejects_grouped_filter_alias_references_inside_case_before_execution() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, \
         COUNT(*) FILTER ( \
           WHERE CASE \
             WHEN total_count > 0 THEN TRUE \
             ELSE FALSE \
           END \
         ) AS total_count \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect_err(
        "grouped FILTER alias leakage inside CASE should stay fail-closed before execution",
    );

    assert!(matches!(
        err,
        SqlLoweringError::UnknownField { field } if field == "total_count"
    ));
}

#[test]
fn compile_sql_command_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
    let query = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "grouped aggregate projection",
    );
    assert!(
        query.has_grouping(),
        "grouped aggregate SQL lowering should produce grouped query intent",
    );
}

#[test]
fn compile_sql_command_select_grouped_qualified_identifiers_match_unqualified_intent() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT SqlLowerEntity.age, COUNT(*) \
         FROM public.SqlLowerEntity \
         WHERE SqlLowerEntity.age >= 21 \
         GROUP BY SqlLowerEntity.age \
         ORDER BY SqlLowerEntity.age DESC LIMIT 2 OFFSET 1",
        "qualified grouped SQL query",
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         GROUP BY age \
         ORDER BY age DESC LIMIT 2 OFFSET 1",
        "unqualified grouped SQL query",
        "qualified grouped SQL identifiers should normalize to the same canonical planned intent as unqualified grouped SQL",
    );
}

#[test]
fn compile_sql_command_select_grouped_top_level_distinct_normalizes_to_grouped_query() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT DISTINCT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "top-level grouped SELECT DISTINCT",
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "plain grouped aggregate projection",
        "top-level grouped SELECT DISTINCT should normalize to the same grouped intent as the non-DISTINCT form",
    );
}

#[test]
fn compile_sql_command_allows_grouped_text_projection_over_grouped_field() {
    assert_sql_lower_query_plan_builds(
        "SELECT name, TRIM(name), COUNT(*) FROM SqlLowerEntity GROUP BY name",
        "grouped text projection over grouped field",
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
    assert_sql_lower_query_plan_builds(
        "SELECT age, age + 1, COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "grouped arithmetic projection over grouped field",
    );
}

#[test]
fn compile_sql_command_allows_grouped_round_projection_over_grouped_field() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, ROUND(age / 3, 2), COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "grouped ROUND projection over grouped field",
    );
}

#[test]
fn compile_sql_command_allows_grouped_round_projection_over_aggregate_output() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, ROUND(AVG(age), 2) FROM SqlLowerEntity GROUP BY age",
        "grouped ROUND projection over aggregate output",
    );
}

#[test]
fn compile_sql_command_allows_grouped_arithmetic_projection_over_aggregate_output() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, COUNT(*) + MAX(age) FROM SqlLowerEntity GROUP BY age",
        "grouped arithmetic projection over aggregate output",
    );
}

#[test]
fn compile_sql_command_deduplicates_repeated_grouped_aggregate_leaves_in_projection_expr() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, COUNT(*) + COUNT(*) FROM SqlLowerEntity GROUP BY age",
        "grouped arithmetic projection with repeated aggregate leaves",
    );
}

#[test]
fn compile_sql_command_deduplicates_repeated_grouped_aggregate_input_leaves_in_projection_expr() {
    let query = compile_sql_lower_query_command(
        "SELECT age, AVG(age + 1) + AVG(age + 1) \
         FROM SqlLowerEntity \
         GROUP BY age",
        "grouped arithmetic projection with repeated aggregate-input leaves",
    );
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
    assert_sql_lower_query_plan_builds(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age + 1 ASC LIMIT 1",
        "grouped additive ORDER BY over grouped field",
    );
}

#[test]
fn compile_sql_command_allows_grouped_subtractive_order_over_grouped_field() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age - 2 ASC LIMIT 1",
        "grouped subtractive ORDER BY over grouped field",
    );
}

#[test]
fn compile_sql_command_rejects_grouped_non_preserving_computed_order() {
    let query = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) FROM SqlLowerEntity GROUP BY age ORDER BY age + age ASC LIMIT 1",
        "grouped non-preserving computed ORDER BY",
    );

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
    assert_sql_lower_query_plan_builds(
        "SELECT age, AVG(age) \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY AVG(age) DESC, age ASC \
         LIMIT 1",
        "grouped aggregate ORDER BY with LIMIT",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_aggregate_order_by_alias_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, AVG(age) AS avg_age \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY avg_age DESC, age ASC \
             LIMIT 1",
            "grouped aggregate ORDER BY alias with LIMIT",
        ),
        "AVG(age)",
        "grouped aggregate ORDER BY aliases should normalize onto the canonical aggregate term",
    );
}

#[test]
fn compile_sql_command_allows_grouped_aggregate_order_with_multi_key_tie_breakers() {
    assert_sql_lower_query_plan_builds(
        "SELECT name, age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY name, age \
         ORDER BY COUNT(*) DESC, name ASC, age ASC \
         LIMIT 1",
        "grouped aggregate ORDER BY with grouped-key tie-breakers",
    );
}

#[test]
fn grouped_aggregate_order_with_multi_key_tie_breakers_preserves_lowered_shape() {
    let lowered = lower_sql_select_shape_for_test(
        "SELECT name, age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY name, age \
         ORDER BY COUNT(*) DESC, name ASC, age ASC \
         LIMIT 1",
        "grouped aggregate ORDER BY lowered shape",
    );

    assert_eq!(
        lowered.group_by_fields_for_test(),
        &["name".to_string(), "age".to_string()],
        "multi-key GROUP BY lowering should preserve both grouped keys in declaration order",
    );
    assert_eq!(
        lowered.order_labels_for_test(),
        vec![
            "COUNT(*)".to_string(),
            "name".to_string(),
            "age".to_string(),
        ],
        "grouped aggregate ORDER BY lowering should preserve the aggregate leader and grouped-key tie-breakers canonically",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_aggregate_input_order_by_alias_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, AVG(age + 1) AS avg_plus_one \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY avg_plus_one DESC, age ASC \
             LIMIT 1",
            "grouped aggregate input ORDER BY alias with LIMIT",
        ),
        "AVG(age + 1)",
        "grouped aggregate input ORDER BY aliases should normalize onto the canonical aggregate term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_wrapped_aggregate_input_order_by_alias_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, ROUND(AVG((age + age) / 2), 2) AS avg_balanced \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY avg_balanced DESC, age ASC \
             LIMIT 1",
            "grouped wrapped aggregate input ORDER BY alias with LIMIT",
        ),
        "ROUND(AVG((age + age) / 2), 2)",
        "grouped wrapped aggregate input ORDER BY aliases should preserve the canonical parenthesized aggregate term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_case_aggregate_input_order_by_alias_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY high_count DESC, age ASC \
             LIMIT 1",
            "grouped searched CASE aggregate input ORDER BY alias with LIMIT",
        ),
        "SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END)",
        "grouped searched CASE aggregate input ORDER BY aliases should normalize onto the canonical aggregate term",
    );
}

#[test]
fn compile_sql_command_accepts_grouped_wrapped_aggregate_order_terms_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, AVG(age) \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY COALESCE(NULLIF(AVG(age), 20), 99) DESC, age ASC \
             LIMIT 1",
            "grouped wrapped aggregate ORDER BY with LIMIT",
        ),
        "COALESCE(NULLIF(AVG(age), 20), 99)",
        "grouped wrapped aggregate ORDER BY terms should lower onto the canonical wrapped post-aggregate order expression",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_wrapped_aggregate_order_by_alias_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, COALESCE(NULLIF(AVG(age), 20), 99) AS adjusted_avg \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY adjusted_avg DESC, age ASC \
             LIMIT 1",
            "grouped wrapped aggregate ORDER BY alias with LIMIT",
        ),
        "COALESCE(NULLIF(AVG(age), 20), 99)",
        "grouped wrapped aggregate ORDER BY aliases should normalize onto the canonical wrapped post-aggregate value-selection term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_filtered_aggregate_order_by_alias_with_limit() {
    let sql_command = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT label, COUNT(*) FILTER (WHERE NOT active) AS inactive_count \
         FROM SqlLowerBoolEntity \
         GROUP BY label \
         ORDER BY inactive_count DESC, label ASC \
         LIMIT 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped filtered aggregate ORDER BY alias with LIMIT should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("grouped filtered aggregate ORDER BY alias should lower to a query command");
    };
    let plan = sql_query
        .plan()
        .expect("grouped filtered aggregate ORDER BY alias plan should build")
        .into_inner();

    assert_eq!(
        plan.scalar_plan()
            .order
            .as_ref()
            .expect("grouped filtered aggregate ORDER BY alias should keep ordering")
            .fields[0]
            .rendered_label(),
        "COUNT(*) FILTER (WHERE NOT active)",
        "grouped filtered aggregate ORDER BY aliases should normalize onto the canonical filtered aggregate term",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_count_order_by_alias_inside_expression_with_limit() {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, COUNT(*) AS total_count \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY total_count + 1 DESC, age ASC \
             LIMIT 1",
            "grouped COUNT ORDER BY alias inside expression with LIMIT",
        ),
        "COUNT(*) + 1",
        "grouped aggregate ORDER BY aliases should substitute recursively inside larger arithmetic order expressions",
    );
}

#[test]
fn compile_sql_command_normalizes_grouped_wrapped_aggregate_order_by_alias_inside_expression_with_limit()
 {
    assert_eq!(
        first_lowered_order_field(
            "SELECT age, ROUND(AVG(age), 2) AS avg_age \
             FROM SqlLowerEntity \
             GROUP BY age \
             ORDER BY avg_age + 1 DESC, age ASC \
             LIMIT 1",
            "grouped wrapped aggregate ORDER BY alias inside expression with LIMIT",
        ),
        "ROUND(AVG(age), 2) + 1",
        "grouped wrapped aggregate ORDER BY aliases should substitute recursively inside larger arithmetic order expressions",
    );
}

#[test]
fn compile_sql_command_accepts_grouped_aggregate_order_by_alias_with_field_compare_predicate() {
    assert_sql_lower_query_plan_builds(
        "SELECT age, ROUND(AVG(age), 2) AS avg_age \
         FROM SqlLowerEntity \
         WHERE name > name \
         GROUP BY age \
         ORDER BY avg_age DESC, age ASC \
         LIMIT 1",
        "grouped aggregate ORDER BY alias with grouped residual filter",
    );
}

#[test]
fn compile_sql_command_accepts_grouped_aggregate_order_with_offset() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, AVG(age) \
         FROM SqlLowerEntity \
         GROUP BY age \
         ORDER BY AVG(age) DESC \
         LIMIT 1 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped aggregate ORDER BY with OFFSET should lower structurally");

    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped query command");
    };

    query
        .plan()
        .expect("grouped aggregate ORDER BY with OFFSET should build through grouped Top-K");
}

#[test]
fn compile_sql_command_accepts_grouped_filtered_aggregate_order_by_alias_with_offset() {
    let sql_command = compile_sql_command::<SqlLowerBoolEntity>(
        "SELECT label, COUNT(*) FILTER (WHERE NOT active) AS inactive_count \
         FROM SqlLowerBoolEntity \
         GROUP BY label \
         ORDER BY inactive_count DESC, label ASC \
         LIMIT 1 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped filtered aggregate ORDER BY alias with OFFSET should lower");
    let SqlCommand::Query(sql_query) = sql_command else {
        panic!(
            "grouped filtered aggregate ORDER BY alias with OFFSET should lower to a query command"
        );
    };
    let plan = sql_query
        .plan()
        .expect("grouped filtered aggregate ORDER BY alias with OFFSET should plan")
        .into_inner();

    assert_eq!(
        plan.scalar_plan()
            .order
            .as_ref()
            .expect("grouped filtered aggregate ORDER BY alias with OFFSET should keep ordering")
            .fields[0]
            .rendered_label(),
        "COUNT(*) FILTER (WHERE NOT active)",
        "grouped filtered aggregate ORDER BY aliases with OFFSET should normalize onto the canonical filtered aggregate term",
    );
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
    assert_sql_lower_query_plan_builds(
        "SELECT age, COUNT(*) FROM SqlLowerEntity WHERE name > name GROUP BY age ORDER BY age ASC LIMIT 10",
        "grouped field-to-field predicate SQL",
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
            .rendered_label(),
        "age + 1",
        "projected direct ORDER BY arithmetic terms should normalize onto the canonical internal numeric expression",
    );
}

#[test]
fn compile_sql_command_accepts_distinct_order_by_expression_derived_from_projected_field() {
    let sql_command = compile_sql_command::<SqlLowerExpressionEntity>(
        "SELECT DISTINCT name FROM SqlLowerExpressionEntity ORDER BY LOWER(name) ASC",
        MissingRowPolicy::Ignore,
    )
    .expect("DISTINCT ORDER BY expressions derived from projected fields should lower");

    let SqlCommand::Query(sql_query) = sql_command else {
        panic!("expected lowered DISTINCT scalar query command");
    };

    let plan = sql_query
        .plan()
        .expect("DISTINCT derived ORDER BY plan should build")
        .into_inner();
    assert_eq!(
        plan.scalar_plan()
            .order
            .as_ref()
            .expect("DISTINCT derived ORDER BY should be present")
            .fields[0]
            .rendered_label(),
        "LOWER(name)",
        "DISTINCT ORDER BY expressions derived from projected fields should keep the canonical order expression",
    );
}

#[test]
fn compile_sql_command_rejects_distinct_order_by_non_projected_field() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT name FROM SqlLowerEntity ORDER BY age ASC",
        MissingRowPolicy::Ignore,
    )
    .expect_err("DISTINCT ORDER BY on a non-projected field should fail closed");

    assert!(
        err.to_string().contains(
            "SELECT DISTINCT ORDER BY terms must be derivable from the projected distinct tuple"
        ),
        "DISTINCT ORDER BY rejection should explain the projected-tuple boundary: {err}",
    );
}

#[test]
fn compile_sql_command_rejects_distinct_order_by_wrapped_non_projected_field() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT DISTINCT name FROM SqlLowerEntity ORDER BY LOWER(age) ASC",
        MissingRowPolicy::Ignore,
    )
    .expect_err("DISTINCT ORDER BY wrapping a non-projected field should fail closed");

    assert!(
        err.to_string().contains(
            "SELECT DISTINCT ORDER BY terms must be derivable from the projected distinct tuple"
        ),
        "wrapped DISTINCT ORDER BY rejection should preserve the projected-tuple boundary: {err}",
    );
}

#[test]
fn compile_sql_command_rejects_distinct_order_by_direct_field_from_expression_projection() {
    let err = compile_sql_command::<SqlLowerExpressionEntity>(
        "SELECT DISTINCT LOWER(name) FROM SqlLowerExpressionEntity ORDER BY name ASC",
        MissingRowPolicy::Ignore,
    )
    .expect_err(
        "DISTINCT ORDER BY on the source field behind an expression projection should fail closed",
    );

    assert!(
        err.to_string().contains(
            "SELECT DISTINCT ORDER BY terms must be derivable from the projected distinct tuple"
        ),
        "expression-projection DISTINCT ORDER BY rejection should preserve the projected-tuple boundary: {err}",
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
        .filter(FieldRef::new("age").gte(21_i64))
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_group(
            "age",
            CompareOp::Gte,
            crate::value::InputValue::from(Value::Int(21)),
        )
        .expect("fluent grouped HAVING group-field clause should be accepted")
        .having_aggregate(
            0,
            CompareOp::Gt,
            crate::value::InputValue::from(Value::Int(1)),
        )
        .expect("fluent grouped HAVING aggregate clause should be accepted")
        .order_term(crate::db::desc("age"))
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
        .having_group(
            "age",
            CompareOp::Ne,
            crate::value::InputValue::from(Value::Null),
        )
        .expect("fluent grouped HAVING group-field IS NOT NULL should be accepted")
        .having_aggregate(
            0,
            CompareOp::Ne,
            crate::value::InputValue::from(Value::Null),
        )
        .expect("fluent grouped HAVING aggregate IS NOT NULL should be accepted")
        .order_term(crate::db::desc("age"))
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
            Some(Expr::Binary { op: BinaryOp::Eq, left, right })
                if matches!(
                    left.as_ref(),
                    Expr::Case { else_expr, .. }
                        if else_expr.as_ref() == &Expr::Literal(Value::Int(0))
                ) && matches!(
                    right.as_ref(),
                    Expr::Literal(Value::Int(1))
                )
        ),
        "grouped searched CASE HAVING should lower through the shared post-aggregate value seam",
    );
}

#[test]
fn compile_sql_command_select_grouped_boolean_searched_case_having_canonicalizes() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped boolean searched CASE HAVING SQL query should lower");
    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped boolean searched CASE HAVING SQL query command");
    };

    let planned = query
        .plan()
        .expect("grouped boolean searched CASE HAVING SQL plan should build")
        .into_inner();
    let grouped = planned
        .grouped_plan()
        .expect("grouped boolean searched CASE HAVING SQL should keep grouped plan shape");

    let case_expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Bool(false))),
    };

    assert_eq!(
        grouped.having_expr.as_ref(),
        Some(&canonicalize_grouped_having_bool_expr(case_expr)),
        "grouped boolean searched CASE HAVING should lower onto the canonical grouped semantic form",
    );
}

#[test]
fn compile_sql_command_select_where_searched_case_hash_matches_fluent_canonical_hash() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT name \
         FROM SqlLowerEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC LIMIT 5",
        "searched CASE scalar WHERE SQL query",
    );
    let searched_case = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gte,
                left: Box::new(Expr::Field(FieldId::new("age"))),
                right: Box::new(Expr::Literal(Value::Int(30))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
    };
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name"])
        .filter_expr(canonicalize_scalar_where_bool_expr(searched_case))
        .order_term(crate::db::asc("age"))
        .limit(5);

    assert_sql_lower_queries_share_plan_identity(
        &sql_query,
        "searched CASE scalar WHERE SQL query",
        &fluent_query,
        "canonical-equivalent fluent scalar filter query",
        "searched CASE scalar WHERE should share normalized planned intent with the equivalent fluent canonical filter form",
    );
    assert_sql_lower_queries_share_plan_hash(
        &sql_query,
        "searched CASE scalar WHERE SQL query",
        &fluent_query,
        "canonical-equivalent fluent scalar filter query",
        "searched CASE scalar WHERE should share one plan hash with the equivalent fluent canonical filter form",
    );
}

#[test]
fn compile_sql_command_select_grouped_boolean_searched_case_truth_wrapper_keeps_same_canonical_shape()
 {
    let canonical = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("canonical grouped boolean searched CASE HAVING SQL query should lower");
    let wrapped = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE ELSE FALSE END \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("truth-wrapped grouped boolean searched CASE HAVING SQL query should lower");
    let SqlCommand::Query(canonical_query) = canonical else {
        panic!("expected canonical grouped boolean searched CASE HAVING SQL query command");
    };
    let SqlCommand::Query(wrapped_query) = wrapped else {
        panic!("expected truth-wrapped grouped boolean searched CASE HAVING SQL query command");
    };

    assert_sql_lower_queries_share_plan_identity(
        &canonical_query,
        "canonical grouped boolean searched CASE HAVING",
        &wrapped_query,
        "truth-wrapped grouped boolean searched CASE HAVING",
        "grouped searched CASE truth wrappers should lower onto the same canonical planned identity",
    );
    assert_sql_lower_queries_share_plan_hash(
        &canonical_query,
        "canonical grouped boolean searched CASE HAVING",
        &wrapped_query,
        "truth-wrapped grouped boolean searched CASE HAVING",
        "grouped searched CASE truth wrappers should keep the same plan hash once canonicalized",
    );
}

#[test]
fn compile_sql_command_select_grouped_boolean_searched_case_hash_matches_fluent_canonical_hash() {
    let sql_query = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END \
         ORDER BY age ASC LIMIT 10",
        "grouped searched CASE HAVING SQL query",
    );
    let grouped_case = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(canonicalize_grouped_having_bool_expr(grouped_case))
        .expect("fluent grouped query should accept canonical grouped searched CASE HAVING")
        .order_term(crate::db::asc("age"))
        .limit(10);

    assert_sql_lower_queries_share_plan_identity(
        &sql_query,
        "grouped searched CASE HAVING SQL query",
        &fluent_query,
        "canonical-equivalent fluent grouped HAVING query",
        "grouped searched CASE HAVING should share normalized planned intent with the equivalent fluent canonical grouped filter form",
    );
    assert_sql_lower_queries_share_plan_hash(
        &sql_query,
        "grouped searched CASE HAVING SQL query",
        &fluent_query,
        "canonical-equivalent fluent grouped HAVING query",
        "grouped searched CASE HAVING should share one plan hash with the equivalent fluent canonical grouped filter form",
    );
}

#[test]
fn compile_sql_command_select_grouped_boolean_searched_case_without_else_canonicalizes_to_null_family()
 {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE END \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect("grouped searched CASE HAVING without ELSE should lower");
    let SqlCommand::Query(query) = command else {
        panic!("expected lowered grouped searched CASE HAVING without ELSE SQL query command");
    };

    let planned = query
        .plan()
        .expect("grouped searched CASE HAVING without ELSE SQL plan should build")
        .into_inner();
    let grouped = planned
        .grouped_plan()
        .expect("grouped searched CASE HAVING without ELSE should keep grouped plan shape");

    let case_expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Null)),
    };

    assert_eq!(
        grouped.having_expr.as_ref(),
        Some(&canonicalize_grouped_having_bool_expr(case_expr)),
        "grouped searched CASE HAVING without ELSE should join the grouped null-family canonical form when the omitted-ELSE expansion is provably identical",
    );
}

#[test]
fn compile_sql_command_select_grouped_boolean_searched_case_without_else_truth_wrapper_keeps_same_null_family_shape()
 {
    let canonical = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE NULL END \
         ORDER BY age ASC LIMIT 10",
        "canonical grouped searched CASE HAVING with explicit ELSE NULL SQL query",
    );
    let wrapped = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE END \
         ORDER BY age ASC LIMIT 10",
        "truth-wrapped grouped searched CASE HAVING without ELSE SQL query",
    );

    assert_sql_lower_queries_share_plan_identity(
        &canonical,
        "canonical grouped searched CASE HAVING with explicit ELSE NULL SQL query",
        &wrapped,
        "truth-wrapped grouped searched CASE HAVING without ELSE SQL query",
        "grouped searched CASE HAVING without ELSE should keep the same canonical planned identity even when the admitted WHEN condition carries a redundant truth wrapper",
    );
    assert_sql_lower_queries_share_plan_hash(
        &canonical,
        "canonical grouped searched CASE HAVING with explicit ELSE NULL SQL query",
        &wrapped,
        "truth-wrapped grouped searched CASE HAVING without ELSE SQL query",
        "grouped searched CASE HAVING without ELSE should keep the same plan hash as the explicit ELSE NULL grouped boolean family even when the admitted WHEN condition carries a redundant truth wrapper",
    );
}

#[test]
fn compile_sql_command_select_grouped_value_searched_case_without_else_is_rejected() {
    let err = compile_sql_command::<SqlLowerEntity>(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN 1 END = 1 \
         ORDER BY age ASC LIMIT 10",
        MissingRowPolicy::Ignore,
    )
    .expect_err(
        "grouped omitted-ELSE searched CASE outside the admitted boolean family must fail closed",
    );

    assert!(
        matches!(err, SqlLoweringError::UnsupportedSelectHaving),
        "grouped omitted-ELSE searched CASE outside the admitted boolean family should reject with the grouped HAVING boundary error: {err:?}",
    );
}

#[test]
fn compile_sql_command_select_grouped_having_alias_matches_canonical_expr_plan() {
    assert_sql_lower_query_matches_sql_plan(
        "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING high_count > 0 \
         ORDER BY age ASC LIMIT 10",
        "grouped HAVING aggregate alias SQL query",
        "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
         FROM SqlLowerEntity \
         GROUP BY age \
         HAVING SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) > 0 \
         ORDER BY age ASC LIMIT 10",
        "canonical grouped HAVING aggregate expression SQL query",
        "grouped HAVING aliases should normalize onto the same canonical post-aggregate expression target",
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
fn compile_sql_command_select_global_aggregate_having_alias_lowers_to_global_aggregate_command() {
    let command = compile_sql_command::<SqlLowerEntity>(
        "SELECT COUNT(*) AS total_rows \
         FROM SqlLowerEntity \
         HAVING total_rows > 1",
        MissingRowPolicy::Ignore,
    )
    .expect("aliased global aggregate HAVING should lower through the dedicated aggregate lane");

    let SqlCommand::GlobalAggregate(command) = command else {
        panic!("aliased global aggregate HAVING should lower to the dedicated aggregate command");
    };

    assert!(
        command.having().is_some(),
        "aliased global aggregate HAVING should normalize onto the shared post-aggregate expression contract",
    );
    assert_eq!(
        command.terminals().len(),
        1,
        "aliased global aggregate HAVING should still reuse the single unique aggregate terminal",
    );
}

#[test]
fn compile_sql_command_select_grouped_aggregate_parity_matches_query_and_executable_identity() {
    // Phase 1: lower equivalent grouped SQL and fluent grouped intents.
    let sql_query = compile_sql_lower_query_command(
        "SELECT age, COUNT(*) \
         FROM SqlLowerEntity \
         WHERE age >= 21 \
         GROUP BY age \
         ORDER BY age DESC LIMIT 3 OFFSET 1",
        "grouped aggregate SQL query",
    );
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("age").gte(21_i64))
        .group_by("age")
        .expect("fluent grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .order_term(crate::db::desc("age"))
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
    assert_sql_lower_queries_share_executable_identity(
        &sql_query,
        "grouped SQL",
        &fluent_query,
        "fluent grouped",
        "equivalent grouped SQL and fluent grouped queries must produce identical executable family",
        "equivalent grouped SQL and fluent grouped queries must produce identical executable ordering",
    );
}

#[test]
fn compile_sql_command_select_field_projection_parity_matches_query_and_executable_identity() {
    // Phase 1: lower equivalent SQL and fluent field-list intents.
    let sql_query = compile_sql_lower_query_command(
        "SELECT name, age FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 5 OFFSET 1",
        "field-list SQL query",
    );
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .select_fields(["name", "age"])
        .filter(FieldRef::new("age").gte(21_i64))
        .order_term(crate::db::desc("age"))
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
    assert_sql_lower_queries_share_executable_identity(
        &sql_query,
        "SQL field-list",
        &fluent_query,
        "fluent field-list",
        "equivalent SQL and fluent field-list projections must produce identical executable family",
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
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(*) FROM SqlLowerEntity WHERE age >= 21",
        "global aggregate count SQL",
    );

    assert_count_rows_strategy(command.terminal());
    assert!(
        !command.query().has_grouping(),
        "global aggregate SQL command should lower to scalar base query shape",
    );
}

#[test]
fn compile_sql_global_aggregate_command_count_sum_avg_min_max_lower() {
    let count_by_command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(age) FROM SqlLowerEntity",
        "COUNT(field) SQL",
    );
    let sum_command = compile_sql_lower_global_aggregate_command(
        "SELECT SUM(age) FROM SqlLowerEntity",
        "SUM(field) SQL",
    );
    let avg_command = compile_sql_lower_global_aggregate_command(
        "SELECT AVG(age) FROM SqlLowerEntity",
        "AVG(field) SQL",
    );
    let min_command = compile_sql_lower_global_aggregate_command(
        "SELECT MIN(age) FROM SqlLowerEntity",
        "MIN(field) SQL",
    );
    let max_command = compile_sql_lower_global_aggregate_command(
        "SELECT MAX(age) FROM SqlLowerEntity",
        "MAX(field) SQL",
    );

    assert_field_aggregate_strategy(
        count_by_command.terminal(),
        AggregateKind::Count,
        "age",
        false,
    );
    assert_field_aggregate_strategy(sum_command.terminal(), AggregateKind::Sum, "age", false);
    assert_field_aggregate_strategy(avg_command.terminal(), AggregateKind::Avg, "age", false);
    assert_field_aggregate_strategy(min_command.terminal(), AggregateKind::Min, "age", false);
    assert_field_aggregate_strategy(max_command.terminal(), AggregateKind::Max, "age", false);
}

#[test]
fn compile_sql_global_aggregate_command_multiple_terminals_lower() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT MIN(age), MAX(age) FROM SqlLowerEntity",
        "multiple global aggregate terminals",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "multi-terminal global aggregate SQL should preserve both aggregate terminals",
    );
    assert_field_aggregate_strategy(&command.terminals()[0], AggregateKind::Min, "age", false);
    assert_field_aggregate_strategy(&command.terminals()[1], AggregateKind::Max, "age", false);
}

#[test]
fn compile_sql_global_aggregate_command_duplicate_terminals_dedup_to_unique_terminal_remap() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(age), COUNT(age), SUM(age), COUNT(age) FROM SqlLowerEntity",
        "duplicate global aggregate terminals",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "duplicate global aggregate SQL should keep only unique executable terminals",
    );
    assert_field_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, "age", false);
    assert_field_aggregate_strategy(&command.terminals()[1], AggregateKind::Sum, "age", false);
    assert_eq!(
        command.output_remap(),
        &[0, 0, 1, 0],
        "duplicate aggregate outputs should remap back to the original projection order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_mixed_duplicate_terminals_preserve_unique_order_remap() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(age), SUM(age), COUNT(age), SUM(age), MAX(age) FROM SqlLowerEntity",
        "mixed duplicate global aggregate terminals",
    );

    assert_eq!(
        command.terminals().len(),
        3,
        "mixed duplicate global aggregate SQL should keep one unique terminal per semantic aggregate",
    );
    assert_field_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, "age", false);
    assert_field_aggregate_strategy(&command.terminals()[1], AggregateKind::Sum, "age", false);
    assert_field_aggregate_strategy(&command.terminals()[2], AggregateKind::Max, "age", false);
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0, 1, 2],
        "mixed duplicate aggregate outputs should remap to the first-seen unique terminal order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_distinct_terminals_do_not_collapse_into_plain_count() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(age), COUNT(DISTINCT age), COUNT(age) FROM SqlLowerEntity",
        "distinct and non-distinct global aggregate terminals",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "COUNT(age) and COUNT(DISTINCT age) should remain separate executable terminals",
    );
    assert_field_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, "age", false);
    assert_field_aggregate_strategy(&command.terminals()[1], AggregateKind::Count, "age", true);
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0],
        "distinct and non-distinct aggregate outputs should only collapse exact duplicates",
    );
}

#[test]
fn compile_sql_global_aggregate_command_qualified_and_unqualified_duplicates_collapse() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(age), COUNT(SqlLowerEntity.age), COUNT(age) FROM SqlLowerEntity",
        "qualified and unqualified duplicate global aggregate terminals",
    );

    assert_eq!(
        command.terminals().len(),
        1,
        "qualified and unqualified aggregate terminals should normalize to one unique executable terminal",
    );
    assert_field_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, "age", false);
    assert_eq!(
        command.output_remap(),
        &[0, 0, 0],
        "qualified and unqualified duplicate outputs should remap to the same unique terminal",
    );
}

#[test]
fn compile_sql_global_aggregate_command_qualified_field_lowers_to_unqualified_terminal() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT SUM(SqlLowerEntity.age) FROM SqlLowerEntity",
        "qualified global aggregate field SQL",
    );

    assert_field_aggregate_strategy(command.terminal(), AggregateKind::Sum, "age", false);
}

#[test]
fn compile_sql_global_aggregate_command_accepts_expression_input_terminals() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(1), SUM(age + 1), AVG(age + 1) FROM SqlLowerEntity",
        "aggregate input expressions",
    );

    assert_eq!(
        command.terminals().len(),
        3,
        "expression aggregate inputs should preserve one prepared strategy per aggregate leaf",
    );
    assert_expr_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, false);
    assert_expr_aggregate_strategy(&command.terminals()[1], AggregateKind::Sum, false);
    assert_expr_aggregate_strategy(&command.terminals()[2], AggregateKind::Avg, false);
}

#[test]
fn compile_sql_global_aggregate_command_accepts_chained_expression_input_terminals() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT AVG(age + 1 * 2), ROUND(AVG((age + age) / 2), 2) FROM SqlLowerEntity",
        "chained aggregate input expressions",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "chained aggregate input expressions should lower onto one terminal per aggregate leaf",
    );
    assert!(
        matches!(
            assert_expr_aggregate_strategy(&command.terminals()[0], AggregateKind::Avg, false),
            Expr::Binary { op: BinaryOp::Add, left, right }
            if matches!(left.as_ref(), Expr::Field(field) if field.as_str() == "age")
                && matches!(right.as_ref(), Expr::Literal(Value::Decimal(value)) if *value == crate::types::Decimal::from(2_u64))
        ),
        "AVG(age + 1 * 2) should preserve the folded semantic input shape in the prepared aggregate strategy",
    );
    assert_eq!(
        command.projection().len(),
        2,
        "chained aggregate input expressions should still preserve the outward projection shape",
    );
}

#[test]
fn compile_sql_global_aggregate_command_accepts_post_aggregate_projection_expressions() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT ROUND(AVG(age), 4), COUNT(*) + 1, MAX(age) - MIN(age) FROM SqlLowerEntity",
        "post-aggregate scalar wrappers",
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
    let ordered = compile_sql_lower_global_aggregate_command(
        "SELECT AVG(age) AS avg_age FROM SqlLowerEntity ORDER BY avg_age DESC",
        "ordered singleton global aggregate output",
    );
    let canonical = compile_sql_lower_global_aggregate_command(
        "SELECT AVG(age) AS avg_age FROM SqlLowerEntity",
        "canonical singleton global aggregate",
    );

    assert_sql_lower_queries_share_plan_identity(
        ordered.query(),
        "ordered singleton global aggregate base query",
        canonical.query(),
        "canonical singleton global aggregate base query",
        "singleton global aggregate ORDER BY aliases should not leak into the base-row aggregate window query",
    );
}

#[test]
fn compile_sql_global_aggregate_command_ignores_singleton_wrapped_output_order_by_alias() {
    let ordered = compile_sql_lower_global_aggregate_command(
        "SELECT ROUND(AVG(age), 2) AS avg_age FROM SqlLowerEntity ORDER BY avg_age DESC",
        "ordered singleton wrapped global aggregate output",
    );
    let canonical = compile_sql_lower_global_aggregate_command(
        "SELECT ROUND(AVG(age), 2) AS avg_age FROM SqlLowerEntity",
        "canonical singleton wrapped global aggregate",
    );

    assert_sql_lower_queries_share_plan_identity(
        ordered.query(),
        "ordered singleton wrapped global aggregate base query",
        canonical.query(),
        "canonical singleton wrapped global aggregate base query",
        "singleton wrapped global aggregate ORDER BY aliases should not leak into the base-row aggregate window query",
    );
}

#[test]
fn compile_sql_global_aggregate_command_deduplicates_expression_input_terminals() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT COUNT(1), SUM(age + 1), COUNT(1), SUM(age + 1) FROM SqlLowerEntity",
        "duplicate expression aggregate inputs",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "duplicate expression aggregate inputs should keep one unique executable terminal per semantic aggregate",
    );
    assert_expr_aggregate_strategy(&command.terminals()[0], AggregateKind::Count, false);
    assert_expr_aggregate_strategy(&command.terminals()[1], AggregateKind::Sum, false);
    assert_eq!(
        command.output_remap(),
        &[0, 1, 0, 1],
        "duplicate expression aggregate outputs should remap to the first-seen unique terminal order",
    );
}

#[test]
fn compile_sql_global_aggregate_command_constant_folds_expression_input_terminals_before_dedup() {
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT SUM(2 * 3), SUM(6), AVG(ROUND(2 * 3, 1)), AVG(6.0) FROM SqlLowerEntity",
        "constant aggregate input expressions",
    );

    assert_eq!(
        command.terminals().len(),
        2,
        "constant-folded aggregate input expressions should dedupe onto one semantic terminal per aggregate kind",
    );
    assert_eq!(
        assert_expr_aggregate_strategy(&command.terminals()[0], AggregateKind::Sum, false),
        &Expr::Literal(Value::Decimal(crate::types::Decimal::from(6_u64))),
        "SUM(2 * 3) should fold onto the canonical SUM(6) strategy input",
    );
    assert_eq!(
        assert_expr_aggregate_strategy(&command.terminals()[1], AggregateKind::Avg, false),
        &Expr::Literal(Value::Decimal(crate::types::Decimal::from(6_u64))),
        "AVG(ROUND(2 * 3, 1)) should fold onto the canonical AVG(6) strategy input",
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
    let command = compile_sql_lower_global_aggregate_command(
        "SELECT SUM(CASE WHEN age >= 21 THEN 1 ELSE 0 END) FROM SqlLowerEntity",
        "searched CASE aggregate inputs",
    );
    let terminal = command.terminal();

    assert!(
        matches!(
            assert_expr_aggregate_strategy(terminal, AggregateKind::Sum, false),
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
                        == &Expr::Literal(Value::Decimal(crate::types::Decimal::from(0_u64)))
        ),
        "searched CASE aggregate inputs should lower through the shared pre-aggregate expression seam: {terminal:?}",
    );
}

fn assert_count_rows_strategy(strategy: &PreparedSqlScalarAggregateStrategy) {
    assert_eq!(
        strategy.descriptor_shape(),
        PreparedSqlScalarAggregateDescriptorShape::CountRows,
        "COUNT(*) should lower to the dedicated count-rows prepared strategy",
    );
    assert_eq!(
        strategy.aggregate_kind(),
        AggregateKind::Count,
        "COUNT(*) should preserve COUNT aggregate identity",
    );
    assert!(
        strategy.target_slot().is_none(),
        "COUNT(*) should not resolve a field target slot",
    );
    assert!(
        strategy.input_expr().is_none(),
        "COUNT(*) should not keep an input expression payload",
    );
    assert!(
        !strategy.is_distinct(),
        "COUNT(*) should not preserve distinct-input semantics",
    );
}

fn assert_field_aggregate_strategy(
    strategy: &PreparedSqlScalarAggregateStrategy,
    kind: AggregateKind,
    field: &str,
    distinct: bool,
) {
    assert_eq!(
        strategy.aggregate_kind(),
        kind,
        "field-target aggregate should preserve aggregate kind",
    );
    assert_eq!(
        strategy
            .target_slot()
            .expect("field-target aggregate should resolve target slot")
            .field(),
        field,
        "field-target aggregate should resolve the canonical target slot",
    );
    assert!(
        strategy.input_expr().is_none(),
        "field-target aggregate should not retain an input expression payload",
    );
    assert_eq!(
        strategy.is_distinct(),
        distinct,
        "field-target aggregate should preserve distinct-input semantics",
    );
}

fn assert_expr_aggregate_strategy(
    strategy: &PreparedSqlScalarAggregateStrategy,
    kind: AggregateKind,
    distinct: bool,
) -> &Expr {
    assert_eq!(
        strategy.aggregate_kind(),
        kind,
        "expression aggregate should preserve aggregate kind",
    );
    assert!(
        strategy.target_slot().is_none(),
        "expression aggregate should not resolve a field target slot",
    );
    assert_eq!(
        strategy.is_distinct(),
        distinct,
        "expression aggregate should preserve distinct-input semantics",
    );

    strategy
        .input_expr()
        .expect("expression aggregate should retain canonical input expression")
}

fn compile_prepared_sql_scalar_strategy(sql: &str) -> PreparedSqlScalarAggregateStrategy {
    let command = compile_sql_lower_global_aggregate_command(sql, "prepared scalar aggregate SQL");

    command.terminal().clone()
}

///
/// ExpectedPreparedSqlScalarAggregateStrategy
///
/// Test-only expectation bundle for prepared SQL scalar aggregate strategy
/// assertions. This keeps the descriptor/domain/runtime contract checks on one
/// helper seam instead of repeating the same assertion block per aggregate kind.
///

struct ExpectedPreparedSqlScalarAggregateStrategy {
    sql: &'static str,
    aggregate_kind: AggregateKind,
    domain: PreparedSqlScalarAggregateDomain,
    descriptor_shape: PreparedSqlScalarAggregateDescriptorShape,
    row_source: PreparedSqlScalarAggregateRowSource,
    ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement,
    empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior,
    runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor,
    target_field: Option<&'static str>,
    distinct: bool,
}

fn assert_prepared_sql_scalar_strategy(expected: &ExpectedPreparedSqlScalarAggregateStrategy) {
    let strategy = compile_prepared_sql_scalar_strategy(expected.sql);

    assert_eq!(
        strategy.aggregate_kind(),
        expected.aggregate_kind,
        "prepared aggregate strategy should preserve aggregate kind: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.domain(),
        expected.domain,
        "prepared aggregate strategy should preserve execution domain: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.descriptor_shape(),
        expected.descriptor_shape,
        "prepared aggregate strategy should preserve descriptor shape: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.row_source(),
        expected.row_source,
        "prepared aggregate strategy should preserve row source: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.ordering_requirement(),
        expected.ordering_requirement,
        "prepared aggregate strategy should preserve ordering requirement: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.empty_set_behavior(),
        expected.empty_set_behavior,
        "prepared aggregate strategy should preserve empty-set behavior: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.runtime_descriptor(),
        expected.runtime_descriptor,
        "prepared aggregate strategy should preserve runtime descriptor: {}",
        expected.sql,
    );
    assert_eq!(
        strategy.is_distinct(),
        expected.distinct,
        "prepared aggregate strategy should preserve distinct-input semantics: {}",
        expected.sql,
    );

    if let Some(field) = expected.target_field {
        assert_eq!(
            strategy
                .target_slot()
                .expect("field-target strategy should keep target slot")
                .field(),
            field,
            "prepared aggregate strategy should preserve canonical target slot: {}",
            expected.sql,
        );
        assert!(
            strategy.input_expr().is_none(),
            "field-target strategy should not retain input expression payload: {}",
            expected.sql,
        );
    } else {
        assert!(
            strategy.target_slot().is_none(),
            "non-field strategy should not resolve target slot: {}",
            expected.sql,
        );
        assert!(
            strategy.input_expr().is_none(),
            "non-field strategy should not retain input expression payload: {}",
            expected.sql,
        );
    }
}

#[test]
fn compile_sql_global_aggregate_command_prepares_scalar_strategies_for_field_and_row_shapes() {
    for expected in [
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT COUNT(*) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Count,
            domain: PreparedSqlScalarAggregateDomain::ExistingRows,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::CountRows,
            row_source: PreparedSqlScalarAggregateRowSource::ExistingRows,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Zero,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::CountRows,
            target_field: None,
            distinct: false,
        },
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT COUNT(age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Count,
            domain: PreparedSqlScalarAggregateDomain::ProjectionField,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::CountField,
            row_source: PreparedSqlScalarAggregateRowSource::ProjectedField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Zero,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::CountField,
            target_field: Some("age"),
            distinct: false,
        },
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT SUM(age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Sum,
            domain: PreparedSqlScalarAggregateDomain::NumericField,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::SumField,
            row_source: PreparedSqlScalarAggregateRowSource::NumericField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: AggregateKind::Sum,
            },
            target_field: Some("age"),
            distinct: false,
        },
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT MIN(age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Min,
            domain: PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::MinField,
            row_source: PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: AggregateKind::Min,
            },
            target_field: Some("age"),
            distinct: false,
        },
    ] {
        assert_prepared_sql_scalar_strategy(&expected);
    }
}

#[test]
fn compile_sql_global_aggregate_command_prepares_scalar_strategies_for_distinct_field_shapes() {
    for expected in [
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT COUNT(DISTINCT age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Count,
            domain: PreparedSqlScalarAggregateDomain::ProjectionField,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::CountField,
            row_source: PreparedSqlScalarAggregateRowSource::ProjectedField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Zero,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::CountField,
            target_field: Some("age"),
            distinct: true,
        },
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT SUM(DISTINCT age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Sum,
            domain: PreparedSqlScalarAggregateDomain::NumericField,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::SumField,
            row_source: PreparedSqlScalarAggregateRowSource::NumericField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::None,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::NumericField {
                kind: AggregateKind::Sum,
            },
            target_field: Some("age"),
            distinct: true,
        },
        ExpectedPreparedSqlScalarAggregateStrategy {
            sql: "SELECT MIN(DISTINCT age) FROM SqlLowerEntity",
            aggregate_kind: AggregateKind::Min,
            domain: PreparedSqlScalarAggregateDomain::ScalarExtremaValue,
            descriptor_shape: PreparedSqlScalarAggregateDescriptorShape::MinField,
            row_source: PreparedSqlScalarAggregateRowSource::ExtremalWinnerField,
            ordering_requirement: PreparedSqlScalarAggregateOrderingRequirement::FieldOrder,
            empty_set_behavior: PreparedSqlScalarAggregateEmptySetBehavior::Null,
            runtime_descriptor: PreparedSqlScalarAggregateRuntimeDescriptor::ExtremalWinnerField {
                kind: AggregateKind::Min,
            },
            target_field: Some("age"),
            distinct: false,
        },
    ] {
        assert_prepared_sql_scalar_strategy(&expected);
    }
}

#[test]
fn compile_sql_global_aggregate_command_preserves_base_query_window_semantics() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT SUM(age) FROM SqlLowerEntity WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate SQL command should lower");
    let fluent_query = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("age").gte(21_i64))
        .order_term(crate::db::desc("age"))
        .limit(2)
        .offset(1);

    assert_sql_lower_queries_share_plan_identity(
        command.query(),
        "SQL global aggregate base query",
        &fluent_query,
        "fluent base query",
        "global aggregate SQL lowering should preserve scalar base query predicate/order/window semantics",
    );
    assert_sql_lower_queries_share_plan_hash(
        command.query(),
        "SQL global aggregate base query",
        &fluent_query,
        "fluent base query",
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
        .filter(FieldRef::new("age").gte(21_i64))
        .order_term(crate::db::desc("age"))
        .limit(3)
        .offset(1);

    // Phase 2: assert aggregate-terminal contract and canonical planned identity + fingerprint parity.
    assert_field_aggregate_strategy(sql_command.terminal(), AggregateKind::Sum, "age", false);
    assert_sql_lower_queries_share_plan_identity(
        sql_command.query(),
        "global aggregate SQL base query",
        &fluent_query,
        "fluent scalar base query",
        "global aggregate SQL base query lowering and fluent scalar query must produce identical normalized planned intent",
    );
    assert_sql_lower_queries_share_plan_hash(
        sql_command.query(),
        "global aggregate SQL base query",
        &fluent_query,
        "fluent scalar base query",
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical fingerprints",
    );

    // Phase 3: assert executable-contract parity at route/runtime planning boundary.
    assert_sql_lower_queries_share_executable_identity(
        sql_command.query(),
        "global aggregate SQL base query",
        &fluent_query,
        "fluent scalar base query",
        "equivalent global aggregate SQL base query and fluent scalar query must produce identical executable family",
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
        command.terminals().len(),
        1,
        "global aggregate HAVING should reuse the same unique terminal list instead of introducing a second aggregate lane",
    );
    assert_count_rows_strategy(&command.terminals()[0]);
}

#[test]
fn compile_sql_global_aggregate_command_without_else_canonicalizes_to_null_family() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) \
         FROM SqlLowerEntity \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE END",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate omitted-ELSE grouped boolean HAVING should lower");

    let case_expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Null)),
    };

    assert_eq!(
        command.having(),
        Some(&canonicalize_grouped_having_bool_expr(case_expr)),
        "global aggregate omitted-ELSE grouped boolean HAVING should join the explicit ELSE NULL canonical family when the grouped boolean proof succeeds",
    );
}

#[test]
fn compile_sql_global_aggregate_command_without_else_truth_wrapper_keeps_same_null_family_shape() {
    let canonical = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) \
         FROM SqlLowerEntity \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE NULL END",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate explicit ELSE NULL grouped boolean HAVING should lower");
    let wrapped = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) \
         FROM SqlLowerEntity \
         HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE END",
        MissingRowPolicy::Ignore,
    )
    .expect("truth-wrapped global aggregate omitted-ELSE grouped boolean HAVING should lower");

    assert_eq!(
        canonical.having(),
        wrapped.having(),
        "truth-wrapped global aggregate omitted-ELSE grouped boolean HAVING should join the same explicit ELSE NULL canonical family",
    );
    assert_eq!(
        canonical.terminals(),
        wrapped.terminals(),
        "truth-wrapped global aggregate omitted-ELSE grouped boolean HAVING should keep the same unique terminal contract as the explicit ELSE NULL family",
    );
}

#[test]
fn compile_sql_global_aggregate_command_rejects_value_case_without_else() {
    let err = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) \
         FROM SqlLowerEntity \
         HAVING CASE WHEN COUNT(*) > 1 THEN 1 END = 1",
        MissingRowPolicy::Ignore,
    )
    .expect_err(
        "global aggregate omitted-ELSE searched CASE outside the admitted boolean family must fail closed",
    );

    assert!(
        matches!(err, SqlLoweringError::UnsupportedSelectHaving),
        "global aggregate omitted-ELSE searched CASE outside the admitted boolean family should reject with the grouped/global HAVING boundary error: {err:?}",
    );
}

#[test]
fn compile_sql_global_aggregate_having_matches_fluent_global_aggregate_intent() {
    let command = compile_sql_global_aggregate_command::<SqlLowerEntity>(
        "SELECT COUNT(*) FROM SqlLowerEntity HAVING COUNT(*) > 1",
        MissingRowPolicy::Ignore,
    )
    .expect("global aggregate SQL HAVING should lower");
    let fluent = Query::<SqlLowerEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count())
        .having_aggregate(
            0,
            CompareOp::Gt,
            crate::value::InputValue::from(Value::Int(1)),
        )
        .expect("global aggregate fluent HAVING should append")
        .plan()
        .expect("global aggregate fluent HAVING should plan")
        .into_inner();
    let Some(grouped) = fluent.grouped_plan() else {
        panic!("global aggregate fluent HAVING should compile to grouped logical plan");
    };

    assert_eq!(
        command.having(),
        grouped.having_expr.as_ref(),
        "global aggregate SQL and fluent HAVING should share the same post-aggregate expression shape",
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
