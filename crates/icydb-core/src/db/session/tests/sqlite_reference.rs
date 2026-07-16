//! Module: db::session::tests::sqlite_reference
//! Responsibility: required native IcyDB/bundled-SQLite SELECT differential evidence.
//! Does not own: SQLite environment policy, manifest eligibility, or production SQL behavior.
//! Boundary: independently executes one shared typed profile and compares exact normalized results.

use super::*;
use crate::db::schema::{AcceptedFieldKind, AcceptedSchemaSnapshot};
use icydb_testing_sql_generator::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedSelectCase, GeneratedValue,
    SelectExpectedRejection, SelectField, SelectFieldKind, SelectGeneratorFamily, SelectIndex,
    SelectQueryShape, SelectResultOrder, SelectSnapshot, SelectValueKind, SelectViolation,
    TIER_A_INVALID_CASES_PER_VIOLATION, TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS,
    TIER_A_VALID_CASES_PER_FAMILY, generate_invalid_select_case, generate_valid_select_case,
};
use icydb_testing_sqlite_reference::{
    SQLITE_REFERENCE_FIXTURE_ROWS, SqliteReferenceColumnKind, SqliteReferenceResult,
    SqliteReferenceRowOrder, SqliteReferenceScenario, SqliteReferenceValue,
    execute_generated_select_case, execute_sqlite_reference_scenario,
    required_sqlite_reference_scenarios,
};

#[test]
fn required_sqlite_reference_profile_matches_native_icydb() {
    reset_session_sql_store();
    let session = sql_session();
    insert_session_fixture_rows(
        &session,
        SQLITE_REFERENCE_FIXTURE_ROWS.iter().copied(),
        |row| SessionSqliteReferenceEntity {
            id: Ulid::generate(),
            name: row.name().to_string(),
            age: row.age(),
            rank: row.rank(),
            active: row.age() % 2 == 0,
        },
        "SQLite reference",
    );

    for scenario in required_sqlite_reference_scenarios() {
        let expected = execute_sqlite_reference_scenario(*scenario).unwrap_or_else(|error| {
            panic!(
                "SQLite reference scenario {:?} should execute: {error}",
                scenario.id()
            )
        });
        let sql = scenario
            .render_sql("SessionSqliteReferenceEntity")
            .expect("maintained SQLite reference entity identifier should render");
        let result =
            execute_sql_statement_for_tests::<SessionSqliteReferenceEntity>(&session, &sql)
                .unwrap_or_else(|error| {
                    panic!(
                        "IcyDB SQLite reference scenario {:?} should execute: {error}",
                        scenario.id()
                    )
                });
        let actual = normalize_icydb_result(*scenario, result).unwrap_or_else(|error| {
            panic!(
                "IcyDB SQLite reference scenario {:?} should normalize: {error}",
                scenario.id()
            )
        });

        assert_eq!(
            actual,
            expected,
            "native IcyDB should agree with bundled SQLite for scenario {:?}",
            scenario.id(),
        );
    }
}

#[test]
fn tier_a_generated_select_profile_matches_native_icydb() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_select_snapshot_from_accepted_authority(&session)
        .expect("accepted session snapshot should map into generator facts");
    let mut executed = 0_u32;
    let mut route_distribution = GeneratedRouteDistribution::default();

    for root_seed in TIER_A_ROOT_SEEDS {
        for family in ALL_SELECT_GENERATOR_FAMILIES {
            for case_index in 0..TIER_A_VALID_CASES_PER_FAMILY {
                reset_session_sql_store();
                let session = sql_session();
                let route = execute_generated_native_reference_case(
                    &session, &snapshot, *root_seed, *family, case_index,
                );
                route_distribution.record(route);
                executed = executed.saturating_add(1);
            }
        }
    }

    assert_eq!(executed, 128);
    route_distribution.assert_complete(executed);
}

fn execute_generated_native_reference_case(
    session: &DbSession<SessionSqlCanister>,
    snapshot: &SelectSnapshot,
    root_seed: u64,
    family: SelectGeneratorFamily,
    case_index: u64,
) -> GeneratedRouteFact {
    let generated = generate_valid_select_case(
        snapshot,
        root_seed,
        family,
        case_index,
        TIER_A_SELECT_BUDGETS,
    )
    .expect("Tier A native differential case should generate");
    seed_generated_select_fixture(session, &generated);
    let expected = execute_generated_select_case(&generated).unwrap_or_else(|error| {
        panic!(
            "generated SQLite case {:?} should execute: {error}",
            generated.identity().id(),
        )
    });
    let (cold_actual, warm_actual, warm_context) =
        execute_generated_cold_warm_modes(session, &generated);

    assert_eq!(
        cold_actual,
        warm_actual,
        "cold and warm execution must preserve generated result semantics for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        warm_actual,
        expected,
        "native IcyDB should agree with SQLite for generated case {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "warm generated compile must reuse one current command artifact",
    );
    assert_eq!(
        generated_compiled_command_shape(warm_context.command()),
        Some(generated.query().shape()),
        "warm compiled command family must preserve typed generated query shape",
    );
    generated_route_fact(session, &generated, warm_context.command()).unwrap_or_else(|error| {
        panic!(
            "generated IcyDB case {:?} should expose typed route facts: {error}",
            generated.identity().id(),
        )
    })
}

fn execute_generated_cold_warm_modes(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) -> (
    SqliteReferenceResult,
    SqliteReferenceResult,
    crate::db::session::sql::SqlCompiledCommandExecutionContext,
) {
    let (cold_context, cold_compile_cache, _) = session
        .compile_sql_query_with_execution_context::<SessionSqliteReferenceEntity>(
            generated.rendered_sql(),
        )
        .unwrap_or_else(|error| {
            panic!(
                "generated IcyDB case {:?} should compile: {error}",
                generated.identity().id(),
            )
        });
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "fresh generated case should occupy one compiled SQL cache entry",
    );
    let (cold_result, cold_cache) = session
        .execute_compiled_sql_context_with_cache_attribution::<SessionSqliteReferenceEntity>(
            &cold_context,
        )
        .unwrap_or_else(|error| {
            panic!(
                "cold generated IcyDB case {:?} should execute: {error}",
                generated.identity().id(),
            )
        });
    let (warm_context, warm_compile_cache, _) = session
        .compile_sql_query_with_execution_context::<SessionSqliteReferenceEntity>(
            generated.rendered_sql(),
        )
        .unwrap_or_else(|error| {
            panic!(
                "warm generated IcyDB case {:?} should recompile from cache: {error}",
                generated.identity().id(),
            )
        });
    let (warm_result, warm_cache) = session
        .execute_compiled_sql_context_with_cache_attribution::<SessionSqliteReferenceEntity>(
            &warm_context,
        )
        .unwrap_or_else(|error| {
            panic!(
                "warm generated IcyDB case {:?} should execute: {error}",
                generated.identity().id(),
            )
        });
    assert_generated_cold_warm_cache_attribution(
        generated,
        cold_compile_cache,
        cold_cache,
        warm_compile_cache,
        warm_cache,
    );
    let cold_actual =
        normalize_generated_icydb_result(generated, cold_result).unwrap_or_else(|error| {
            panic!(
                "cold generated IcyDB case {:?} should normalize: {error}",
                generated.identity().id(),
            )
        });
    let warm_actual =
        normalize_generated_icydb_result(generated, warm_result).unwrap_or_else(|error| {
            panic!(
                "warm generated IcyDB case {:?} should normalize: {error}",
                generated.identity().id(),
            )
        });

    (cold_actual, warm_actual, warm_context)
}

///
/// GeneratedRouteFact
///
/// Typed execution-family observation taken from the compiled command lane or
/// the existing query trace. It deliberately carries no textual EXPLAIN data.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GeneratedRouteFact {
    /// Dedicated whole-input aggregate command lane.
    GlobalAggregate,

    /// Grouped executor family selected after planning.
    Grouped,

    /// Ordered scalar executor family selected after planning.
    Ordered,

    /// Primary-key scalar executor family selected after planning.
    PrimaryKey,
}

///
/// GeneratedRouteDistribution
///
/// Test-owned route counts used only to prove every generated case yielded a
/// typed route fact. Counts are evidence and never an optimization target.
///

#[derive(Debug, Default)]
struct GeneratedRouteDistribution {
    counts: BTreeMap<GeneratedRouteFact, u32>,
}

impl GeneratedRouteDistribution {
    fn record(&mut self, route: GeneratedRouteFact) {
        let count = self.counts.entry(route).or_default();
        *count = count.saturating_add(1);
    }

    fn count(&self, route: GeneratedRouteFact) -> u32 {
        self.counts.get(&route).copied().unwrap_or_default()
    }

    fn assert_complete(&self, expected_total: u32) {
        let total = self
            .counts
            .values()
            .copied()
            .fold(0_u32, u32::saturating_add);
        let scalar = self
            .count(GeneratedRouteFact::Ordered)
            .saturating_add(self.count(GeneratedRouteFact::PrimaryKey));

        assert_eq!(
            total, expected_total,
            "every generated case must contribute exactly one typed route fact: {self:?}",
        );
        assert!(
            self.count(GeneratedRouteFact::GlobalAggregate) > 0,
            "generated route evidence must include the global aggregate lane: {self:?}",
        );
        assert!(
            self.count(GeneratedRouteFact::Grouped) > 0,
            "generated route evidence must include the grouped family: {self:?}",
        );
        assert!(
            scalar > 0,
            "generated route evidence must include a scalar execution family: {self:?}",
        );
    }
}

fn assert_generated_cold_warm_cache_attribution(
    generated: &GeneratedSelectCase,
    cold_compile: crate::db::session::sql::SqlCacheAttribution,
    cold_execute: crate::db::session::sql::SqlCacheAttribution,
    warm_compile: crate::db::session::sql::SqlCacheAttribution,
    warm_execute: crate::db::session::sql::SqlCacheAttribution,
) {
    assert_eq!(
        (
            cold_compile.sql_compiled_command_cache_hits,
            cold_compile.sql_compiled_command_cache_misses,
        ),
        (0, 1),
        "first generated compile must report one typed cold SQL-cache lookup for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        (
            warm_compile.sql_compiled_command_cache_hits,
            warm_compile.sql_compiled_command_cache_misses,
        ),
        (1, 0),
        "second generated compile must report one typed warm SQL-cache lookup for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        (
            cold_execute.sql_compiled_command_cache_hits,
            cold_execute.sql_compiled_command_cache_misses,
        ),
        (0, 0),
        "explicit cold execution must not claim SQL compile-cache work for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        cold_execute
            .shared_query_plan_cache_hits
            .saturating_add(cold_execute.shared_query_plan_cache_misses),
        1,
        "first generated execution must report exactly one typed plan-cache event for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        (
            warm_execute.sql_compiled_command_cache_hits,
            warm_execute.sql_compiled_command_cache_misses,
        ),
        (0, 0),
        "explicit warm execution must not claim SQL compile-cache work for {:?}",
        generated.identity().id(),
    );
    assert_eq!(
        (
            warm_execute.shared_query_plan_cache_hits,
            warm_execute.shared_query_plan_cache_misses,
        ),
        (1, 0),
        "second generated execution must report one typed warm plan lookup for {:?}",
        generated.identity().id(),
    );
}

fn generated_compiled_command_shape(
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) -> Option<SelectQueryShape> {
    match compiled {
        crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. } => {
            Some(SelectQueryShape::GlobalAggregate)
        }
        crate::db::session::sql::CompiledSqlCommand::Select { query, .. } => {
            if query.has_grouping() {
                Some(SelectQueryShape::GroupedAggregate)
            } else {
                Some(SelectQueryShape::Scalar)
            }
        }
        _ => None,
    }
}

fn generated_route_fact(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) -> Result<GeneratedRouteFact, String> {
    let expected_shape = generated.query().shape();
    let compiled_shape = generated_compiled_command_shape(compiled)
        .ok_or_else(|| "generated SELECT compiled into a non-query command family".to_string())?;
    if compiled_shape != expected_shape {
        return Err(format!(
            "typed query shape {expected_shape:?} compiled as {compiled_shape:?}",
        ));
    }
    let crate::db::session::sql::CompiledSqlCommand::Select { query, .. } = compiled else {
        return Ok(GeneratedRouteFact::GlobalAggregate);
    };
    let query = Query::<SessionSqliteReferenceEntity>::from_inner(query.as_ref().clone());
    let trace = session
        .trace_query(&query)
        .map_err(|error| error.to_string())?;
    let route = match trace.execution_family() {
        Some(crate::db::TraceExecutionFamily::Grouped) => GeneratedRouteFact::Grouped,
        Some(crate::db::TraceExecutionFamily::Ordered) => GeneratedRouteFact::Ordered,
        Some(crate::db::TraceExecutionFamily::PrimaryKey) => GeneratedRouteFact::PrimaryKey,
        None => return Err("generated SELECT trace omitted its execution family".to_string()),
    };
    match (expected_shape, route) {
        (SelectQueryShape::GroupedAggregate, GeneratedRouteFact::Grouped)
        | (
            SelectQueryShape::Scalar,
            GeneratedRouteFact::Ordered | GeneratedRouteFact::PrimaryKey,
        ) => Ok(route),
        _ => Err(format!(
            "typed query shape {expected_shape:?} selected incompatible route {route:?}",
        )),
    }
}

#[test]
fn tier_a_generated_invalid_profile_rejects_with_owned_typed_classes() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_select_snapshot_from_accepted_authority(&session)
        .expect("accepted session snapshot should map into generator facts");
    let mut rejected = 0_u32;

    for root_seed in TIER_A_ROOT_SEEDS {
        for violation in ALL_SELECT_VIOLATIONS {
            for case_index in 0..TIER_A_INVALID_CASES_PER_VIOLATION {
                let generated = generate_invalid_select_case(
                    &snapshot,
                    *root_seed,
                    *violation,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A invalid case should generate from one valid base");
                let error = execute_sql_statement_for_tests::<SessionSqliteReferenceEntity>(
                    &session,
                    generated.rendered_sql(),
                )
                .expect_err("classified invalid generated SQL must reject");
                assert_generated_rejection(error, *violation);
                rejected = rejected.saturating_add(1);
            }
        }
    }

    assert_eq!(rejected, 40);
}

fn generated_select_snapshot_from_accepted_authority(
    session: &DbSession<SessionSqlCanister>,
) -> Result<SelectSnapshot, String> {
    let context = session
        .accepted_schema_catalog_context_for_query::<SessionSqliteReferenceEntity>()
        .map_err(|error| error.to_string())?;
    select_snapshot_from_accepted(context.snapshot())
}

fn select_snapshot_from_accepted(
    accepted: &AcceptedSchemaSnapshot,
) -> Result<SelectSnapshot, String> {
    let persisted = accepted.persisted_snapshot();
    let fields = persisted
        .fields()
        .iter()
        .map(|field| {
            let kind = match field.kind() {
                AcceptedFieldKind::Blob { .. } => SelectFieldKind::Blob,
                AcceptedFieldKind::Bool => SelectFieldKind::Boolean,
                AcceptedFieldKind::Int64 => SelectFieldKind::Integer,
                AcceptedFieldKind::Text { .. } => SelectFieldKind::Text,
                AcceptedFieldKind::Ulid => SelectFieldKind::Ulid,
                unsupported => {
                    return Err(format!(
                        "accepted generated-test field {:?} has unsupported kind {unsupported:?}",
                        field.name(),
                    ));
                }
            };
            let primary_key = persisted.primary_key_field_ids().contains(&field.id());
            let write_policy = field.write_policy();
            let database_managed = write_policy.insert_generation().is_some()
                || write_policy.write_management().is_some();
            Ok(SelectField::new(
                field.id().get(),
                field.name(),
                kind,
                field.nullable(),
                primary_key,
                database_managed,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let indexes = persisted
        .indexes()
        .iter()
        .map(|index| {
            let field_ids = index
                .key()
                .field_paths()
                .iter()
                .map(|path| path.field_id().get())
                .collect::<Vec<_>>();
            if field_ids.is_empty() {
                return Err(format!(
                    "accepted generated-test index {:?} is not a direct field-path index",
                    index.name(),
                ));
            }
            Ok(SelectIndex::new(index.ordinal(), index.name(), field_ids))
        })
        .collect::<Result<Vec<_>, String>>()?;

    SelectSnapshot::try_new(
        "session-accepted-snapshot-v1",
        accepted.entity_path(),
        accepted.entity_name(),
        persisted.version().get(),
        fields,
        indexes,
    )
    .map_err(|error| error.to_string())
}

fn seed_generated_select_fixture(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) {
    insert_session_fixture_rows(
        session,
        generated.fixture().rows().iter(),
        |row| SessionSqliteReferenceEntity {
            id: Ulid::generate(),
            name: generated_text_value(row, generated, "name"),
            age: generated_integer_value(row, generated, "age"),
            rank: generated_integer_value(row, generated, "rank"),
            active: generated_boolean_value(row, generated, "active"),
        },
        generated.identity().id(),
    );
}

fn generated_text_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> String {
    let Some(GeneratedValue::Text(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry text field {field:?}",
            generated.identity().id(),
        );
    };
    value.clone()
}

fn generated_integer_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> i64 {
    let Some(GeneratedValue::Integer(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry integer field {field:?}",
            generated.identity().id(),
        );
    };
    *value
}

fn generated_boolean_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> bool {
    let Some(GeneratedValue::Boolean(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry boolean field {field:?}",
            generated.identity().id(),
        );
    };
    *value
}

fn assert_generated_rejection(error: QueryError, violation: SelectViolation) {
    let expected = violation.expected_rejection();
    match expected {
        SelectExpectedRejection::InvalidClauseOrder | SelectExpectedRejection::LimitOverflow => {
            assert_eq!(
                error.diagnostic_code(),
                DiagnosticCode::RuntimeUnsupported,
                "syntax-family generated rejection should remain fail-closed",
            );
        }
        SelectExpectedRejection::UnknownField
        | SelectExpectedRejection::UnsupportedFunctionSignature
        | SelectExpectedRejection::WrongOperatorType => {
            let plan = match error {
                QueryError::Plan(plan) => plan,
                actual => panic!(
                    "semantic generated rejection {expected:?} should remain planner-owned: {actual:?}",
                ),
            };
            let user = match *plan {
                PlanError::User(user) => user,
                actual => panic!(
                    "semantic generated rejection {expected:?} should remain a user-shape plan error: {actual:?}",
                ),
            };
            let expression = match *user {
                PlanUserError::Expr(expression) => expression,
                actual => panic!(
                    "semantic generated rejection {expected:?} should remain expression-typed: {actual:?}",
                ),
            };
            match (expected, *expression) {
                (SelectExpectedRejection::UnknownField, ExprPlanError::UnknownField { .. })
                | (
                    SelectExpectedRejection::UnsupportedFunctionSignature,
                    ExprPlanError::InvalidFunctionArgument { .. },
                )
                | (
                    SelectExpectedRejection::WrongOperatorType,
                    ExprPlanError::InvalidBinaryOperands { .. },
                ) => {}
                (expected, actual) => {
                    panic!("generated rejection {expected:?} produced {actual:?}");
                }
            }
        }
    }
}

fn normalize_icydb_result(
    scenario: SqliteReferenceScenario,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    normalize_icydb_result_with_contract(
        scenario.id(),
        scenario.columns(),
        scenario.row_order(),
        result,
    )
}

fn normalize_generated_icydb_result(
    generated: &GeneratedSelectCase,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    let kinds = generated
        .query()
        .projection_kinds(generated.snapshot())
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|kind| match kind {
            SelectValueKind::Boolean => SqliteReferenceColumnKind::Boolean,
            SelectValueKind::Decimal => SqliteReferenceColumnKind::Decimal,
            SelectValueKind::Integer => SqliteReferenceColumnKind::Integer,
            SelectValueKind::Text => SqliteReferenceColumnKind::Text,
        })
        .collect::<Vec<_>>();
    let row_order = match generated.query().result_order() {
        SelectResultOrder::Ordered => SqliteReferenceRowOrder::Ordered,
        SelectResultOrder::Unordered => SqliteReferenceRowOrder::Unordered,
    };
    normalize_icydb_result_with_contract(generated.identity().id(), &kinds, row_order, result)
}

fn normalize_icydb_result_with_contract(
    scenario_id: &str,
    kinds: &[SqliteReferenceColumnKind],
    row_order: SqliteReferenceRowOrder,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    let (columns, rows) = match result {
        SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
            ..
        } => {
            verify_row_count(scenario_id, row_count, rows.len())?;
            (columns, rows)
        }
        SqlStatementResult::Grouped {
            columns,
            rows,
            row_count,
            next_cursor,
            ..
        } => {
            verify_row_count(scenario_id, row_count, rows.len())?;
            if next_cursor.is_some() {
                return Err(
                    "compact SQLite reference profile unexpectedly produced a cursor".into(),
                );
            }
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.group_key()
                        .iter()
                        .chain(row.aggregate_values())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect();
            (columns, rows)
        }
        other => {
            return Err(format!(
                "scenario {scenario_id:?} returned unsupported statement payload {other:?}",
            ));
        }
    };
    if columns.len() != kinds.len() {
        return Err(format!(
            "scenario {scenario_id:?} returned {} columns for {} declared mappings",
            columns.len(),
            kinds.len(),
        ));
    }
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(row_index, row)| normalize_icydb_row(scenario_id, kinds, row_index, row))
        .collect::<Result<Vec<_>, _>>()?;

    SqliteReferenceResult::try_new(columns, rows, row_order).map_err(|error| error.to_string())
}

fn normalize_icydb_row(
    scenario_id: &str,
    kinds: &[SqliteReferenceColumnKind],
    row_index: usize,
    row: Vec<OutputValue>,
) -> Result<Vec<SqliteReferenceValue>, String> {
    if row.len() != kinds.len() {
        return Err(format!(
            "scenario {scenario_id:?} row {row_index} returned {} values for {} declared mappings",
            row.len(),
            kinds.len(),
        ));
    }

    row.into_iter()
        .zip(kinds.iter().copied())
        .enumerate()
        .map(|(column, (value, kind))| normalize_icydb_value(scenario_id, column, kind, value))
        .collect()
}

fn normalize_icydb_value(
    scenario_id: &str,
    column: usize,
    kind: SqliteReferenceColumnKind,
    value: OutputValue,
) -> Result<SqliteReferenceValue, String> {
    let normalized = match (kind, value) {
        (_, OutputValue::Null) => SqliteReferenceValue::Null,
        (SqliteReferenceColumnKind::Blob, OutputValue::Blob(value)) => {
            SqliteReferenceValue::Blob(value)
        }
        (SqliteReferenceColumnKind::Boolean, OutputValue::Bool(value)) => {
            SqliteReferenceValue::Boolean(value)
        }
        (SqliteReferenceColumnKind::Decimal, OutputValue::Decimal(value)) => {
            SqliteReferenceValue::Decimal {
                mantissa: value.mantissa(),
                scale: value.scale(),
            }
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int64(value)) => {
            SqliteReferenceValue::Integer(value)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int128(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Int128 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat64(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Nat64 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat128(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Nat128 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Text, OutputValue::Text(value)) => {
            SqliteReferenceValue::Text(value)
        }
        (kind, value) => {
            return Err(format!(
                "scenario {scenario_id:?} column {column} returned {value:?} for {kind:?}",
            ));
        }
    };

    Ok(normalized)
}

fn verify_row_count(scenario_id: &str, row_count: u32, rows_len: usize) -> Result<(), String> {
    let rows_len = u32::try_from(rows_len).map_err(|_| {
        format!("scenario {scenario_id:?} returned a row vector too large for its public count")
    })?;
    if row_count != rows_len {
        return Err(format!(
            "scenario {scenario_id:?} reported {row_count} rows but returned {rows_len}",
        ));
    }

    Ok(())
}
