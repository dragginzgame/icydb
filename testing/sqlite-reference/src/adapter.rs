//! Module: sqlite_reference::adapter
//! Responsibility: checked SQLite connection, fixture, query, and result execution.
//! Does not own: IcyDB execution, feature coverage, or correctness verdicts.
//! Boundary: runs every reference scenario in a fresh transaction and in-memory database.

use crate::{
    SQLITE_REFERENCE_FIXTURE_ROWS, SqliteAdapterError, SqliteAdapterErrorKind,
    SqliteReferenceColumnKind, SqliteReferenceResult, SqliteReferenceScenario,
    SqliteReferenceValue, environment::verify_sqlite_environment, profile::SQLITE_REFERENCE_ENTITY,
};
use icydb_testing_sql_generator::{
    GeneratedSelectCase, GeneratedValue, SelectExpectedOutcome, SelectFieldKind, SelectProvider,
    SelectResultOrder, SelectValueKind,
};
use rusqlite::{
    Connection, Transaction,
    functions::FunctionFlags,
    params,
    types::{Value, ValueRef},
};

const CONNECTION_POLICY_SQL: &str = "
PRAGMA encoding = 'UTF-8';
PRAGMA foreign_keys = ON;
PRAGMA case_sensitive_like = ON;
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA trusted_schema = OFF;
PRAGMA recursive_triggers = OFF;
";

/// Execute one required scenario against the checked bundled SQLite reference.
///
/// # Errors
///
/// Returns a typed adapter error when environment verification, fixture setup,
/// statement execution, value mapping, or transaction completion fails.
pub fn execute_sqlite_reference_scenario(
    scenario: SqliteReferenceScenario,
) -> Result<SqliteReferenceResult, SqliteAdapterError> {
    let mut connection = open_checked_connection()?;
    let transaction = connection.transaction().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            "failed to begin SQLite reference transaction",
            source,
        )
    })?;
    create_and_seed_reference_fixture(&transaction)?;
    let result = execute_reference_query(&transaction, scenario)?;
    transaction.commit().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            "failed to commit SQLite reference transaction",
            source,
        )
    })?;

    Ok(result)
}

/// Execute one valid generated SELECT against its embedded accepted snapshot
/// and bounded fixture in the checked bundled SQLite reference.
///
/// # Errors
///
/// Returns a typed adapter error when the generated case is not reference-
/// eligible or environment, fixture, query, result, or transaction work fails.
pub fn execute_generated_select_case(
    generated: &GeneratedSelectCase,
) -> Result<SqliteReferenceResult, SqliteAdapterError> {
    validate_generated_reference_case(generated)?;
    let mut connection = open_checked_connection()?;
    register_generated_select_functions(&connection)?;
    let transaction = connection.transaction().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            format!(
                "failed to begin generated SQLite transaction {:?}",
                generated.identity().id(),
            ),
            source,
        )
    })?;
    create_and_seed_generated_fixture(&transaction, generated)?;
    let kinds = generated
        .query()
        .projection_kinds(generated.snapshot())
        .map_err(|error| {
            SqliteAdapterError::new(
                SqliteAdapterErrorKind::GeneratedCase,
                format!(
                    "generated SQLite case {:?} has invalid projection kinds: {error}",
                    generated.identity().id(),
                ),
            )
        })?
        .into_iter()
        .map(sqlite_column_kind)
        .collect::<Vec<_>>();
    let row_order = match generated.query().result_order() {
        SelectResultOrder::Ordered => crate::SqliteReferenceRowOrder::Ordered,
        SelectResultOrder::Unordered => crate::SqliteReferenceRowOrder::Unordered,
    };
    let result = execute_query(
        &transaction,
        generated.identity().id(),
        generated.rendered_sql(),
        &kinds,
        row_order,
    )?;
    transaction.commit().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            format!(
                "failed to commit generated SQLite transaction {:?}",
                generated.identity().id(),
            ),
            source,
        )
    })?;

    Ok(result)
}

pub(crate) fn open_checked_connection() -> Result<Connection, SqliteAdapterError> {
    let connection = Connection::open_in_memory().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Connection,
            "failed to open bundled in-memory SQLite",
            source,
        )
    })?;
    verify_sqlite_environment(&connection)?;
    apply_and_verify_connection_policy(&connection)?;

    Ok(connection)
}

fn apply_and_verify_connection_policy(connection: &Connection) -> Result<(), SqliteAdapterError> {
    connection.load_extension_disable().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Environment,
            "failed to disable SQLite loadable extensions",
            source,
        )
    })?;
    connection
        .execute_batch(CONNECTION_POLICY_SQL)
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to apply required SQLite connection policy",
                source,
            )
        })?;

    verify_text_pragma(connection, "encoding", "UTF-8")?;
    verify_integer_pragma(connection, "foreign_keys", 1)?;
    verify_text_pragma(connection, "journal_mode", "memory")?;
    verify_integer_pragma(connection, "synchronous", 0)?;
    verify_integer_pragma(connection, "temp_store", 2)?;
    verify_integer_pragma(connection, "trusted_schema", 0)?;
    verify_integer_pragma(connection, "recursive_triggers", 0)?;
    let case_sensitive_like = connection
        .query_row("SELECT 'a' LIKE 'A'", [], |row| row.get::<_, i64>(0))
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to verify case-sensitive SQLite LIKE behavior",
                source,
            )
        })?;
    if case_sensitive_like != 0 {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::Environment,
            "SQLite case_sensitive_like policy did not take effect",
        ));
    }

    Ok(())
}

fn verify_text_pragma(
    connection: &Connection,
    name: &str,
    expected: &str,
) -> Result<(), SqliteAdapterError> {
    let actual = connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, String>(0))
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                format!("failed to verify SQLite PRAGMA {name}"),
                source,
            )
        })?;
    if actual != expected {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::Environment,
            format!("SQLite PRAGMA {name} is {actual:?}, expected {expected:?}"),
        ));
    }

    Ok(())
}

fn verify_integer_pragma(
    connection: &Connection,
    name: &str,
    expected: i64,
) -> Result<(), SqliteAdapterError> {
    let actual = connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, i64>(0))
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                format!("failed to verify SQLite PRAGMA {name}"),
                source,
            )
        })?;
    if actual != expected {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::Environment,
            format!("SQLite PRAGMA {name} is {actual}, expected {expected}"),
        ));
    }

    Ok(())
}

fn validate_generated_reference_case(
    generated: &GeneratedSelectCase,
) -> Result<(), SqliteAdapterError> {
    generated.validate().map_err(|error| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::GeneratedCase,
            format!("generated SQLite case failed validation: {error}"),
        )
    })?;
    if generated.expected() != SelectExpectedOutcome::Accepted
        || generated.provider() != SelectProvider::SqliteReference
    {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::GeneratedCase,
            "SQLite generated-case execution requires accepted reference evidence",
        ));
    }

    Ok(())
}

fn register_generated_select_functions(connection: &Connection) -> Result<(), SqliteAdapterError> {
    connection
        .create_scalar_function(
            "STARTS_WITH",
            2,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_INNOCUOUS,
            |context| {
                let value = context.get::<String>(0)?;
                let prefix = context.get::<String>(1)?;
                Ok(value.starts_with(prefix.as_str()))
            },
        )
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Environment,
                "failed to register generated SELECT reference functions",
                source,
            )
        })
}

fn create_and_seed_generated_fixture(
    transaction: &Transaction<'_>,
    generated: &GeneratedSelectCase,
) -> Result<(), SqliteAdapterError> {
    let fields = generated
        .snapshot()
        .fields()
        .iter()
        .filter(|field| {
            !field.primary_key() && !field.generated() && field.kind().is_generated_scalar()
        })
        .collect::<Vec<_>>();
    let definitions = fields
        .iter()
        .map(|field| {
            let data_type = match field.kind() {
                SelectFieldKind::Boolean | SelectFieldKind::Integer => "INTEGER",
                SelectFieldKind::Text => "TEXT COLLATE BINARY",
                SelectFieldKind::Blob | SelectFieldKind::Ulid => {
                    return Err(SqliteAdapterError::new(
                        SqliteAdapterErrorKind::GeneratedCase,
                        "generated SQLite fixture included an ineligible field kind",
                    ));
                }
            };
            let nullability = if field.nullable() { "" } else { " NOT NULL" };
            let boolean_check = if field.kind() == SelectFieldKind::Boolean {
                format!(" CHECK ({} IN (0, 1))", field.name())
            } else {
                String::new()
            };
            Ok(format!(
                "{} {data_type}{nullability}{boolean_check}",
                field.name(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let create_sql = format!(
        "CREATE TABLE {} ({}) STRICT;",
        generated.snapshot().entity_name(),
        definitions.join(", "),
    );
    transaction.execute_batch(&create_sql).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Fixture,
            format!(
                "failed to create generated SQLite fixture {:?}",
                generated.identity().id(),
            ),
            source,
        )
    })?;

    let columns = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
    let parameters = (1..=fields.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>();
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        generated.snapshot().entity_name(),
        columns.join(", "),
        parameters.join(", "),
    );
    let mut insert = transaction.prepare(&insert_sql).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Fixture,
            format!(
                "failed to prepare generated SQLite fixture insert {:?}",
                generated.identity().id(),
            ),
            source,
        )
    })?;
    for (row_index, row) in generated.fixture().rows().iter().enumerate() {
        let values = fields
            .iter()
            .map(|field| {
                row.value_by_field_id(field.id())
                    .ok_or_else(|| {
                        SqliteAdapterError::new(
                            SqliteAdapterErrorKind::GeneratedCase,
                            format!(
                                "generated SQLite fixture row {row_index} lacks field {:?}",
                                field.name(),
                            ),
                        )
                    })
                    .map(sqlite_fixture_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        insert
            .execute(rusqlite::params_from_iter(values))
            .map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Fixture,
                    format!(
                        "failed to insert generated SQLite fixture row {row_index} for {:?}",
                        generated.identity().id(),
                    ),
                    source,
                )
            })?;
    }

    Ok(())
}

fn sqlite_fixture_value(value: &GeneratedValue) -> Value {
    match value {
        GeneratedValue::Boolean(value) => Value::Integer(i64::from(*value)),
        GeneratedValue::Integer(value) => Value::Integer(*value),
        GeneratedValue::Null(_) => Value::Null,
        GeneratedValue::Text(value) => Value::Text(value.clone()),
    }
}

const fn sqlite_column_kind(kind: SelectValueKind) -> SqliteReferenceColumnKind {
    match kind {
        SelectValueKind::Boolean => SqliteReferenceColumnKind::Boolean,
        SelectValueKind::Decimal => SqliteReferenceColumnKind::Decimal,
        SelectValueKind::Integer => SqliteReferenceColumnKind::Integer,
        SelectValueKind::Text => SqliteReferenceColumnKind::Text,
    }
}

fn create_and_seed_reference_fixture(
    transaction: &Transaction<'_>,
) -> Result<(), SqliteAdapterError> {
    transaction
        .execute_batch(
            "CREATE TABLE IcyDbSqliteReferenceUser (\
             name TEXT NOT NULL COLLATE BINARY, \
             age INTEGER NOT NULL, \
             rank INTEGER NOT NULL\
             ) STRICT;",
        )
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                "failed to create SQLite reference fixture schema",
                source,
            )
        })?;
    let mut insert = transaction
        .prepare("INSERT INTO IcyDbSqliteReferenceUser(name, age, rank) VALUES (?1, ?2, ?3)")
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                "failed to prepare SQLite reference fixture insert",
                source,
            )
        })?;
    for row in SQLITE_REFERENCE_FIXTURE_ROWS {
        insert
            .execute(params![row.name(), row.age(), row.rank()])
            .map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Fixture,
                    format!(
                        "failed to insert SQLite reference fixture row {:?}",
                        row.name()
                    ),
                    source,
                )
            })?;
    }

    Ok(())
}

fn execute_reference_query(
    transaction: &Transaction<'_>,
    scenario: SqliteReferenceScenario,
) -> Result<SqliteReferenceResult, SqliteAdapterError> {
    let sql = scenario.render_sqlite_sql()?;
    debug_assert!(sql.contains(SQLITE_REFERENCE_ENTITY));
    execute_query(
        transaction,
        scenario.id(),
        sql.as_str(),
        scenario.columns(),
        scenario.row_order(),
    )
}

fn execute_query(
    transaction: &Transaction<'_>,
    scenario_id: &str,
    sql: &str,
    kinds: &[SqliteReferenceColumnKind],
    row_order: crate::SqliteReferenceRowOrder,
) -> Result<SqliteReferenceResult, SqliteAdapterError> {
    let mut statement = transaction.prepare(sql).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Query,
            format!("failed to prepare SQLite reference scenario {scenario_id:?}"),
            source,
        )
    })?;
    let column_count = statement.column_count();
    if column_count != kinds.len() {
        return Err(SqliteAdapterError::new(
            SqliteAdapterErrorKind::Result,
            format!(
                "SQLite scenario {scenario_id:?} returned {column_count} columns, expected {}",
                kinds.len(),
            ),
        ));
    }
    let columns = statement
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let mut query = statement.query([]).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Query,
            format!("failed to execute SQLite reference scenario {scenario_id:?}"),
            source,
        )
    })?;
    let mut rows = Vec::new();
    while let Some(row) = query.next().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Result,
            format!("failed to read SQLite reference scenario {scenario_id:?}"),
            source,
        )
    })? {
        let mut values = Vec::with_capacity(column_count);
        for (column, kind) in kinds.iter().copied().enumerate() {
            let value = row.get_ref(column).map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Result,
                    format!("failed to read SQLite scenario {scenario_id:?} column {column}"),
                    source,
                )
            })?;
            values.push(sqlite_reference_value(scenario_id, column, kind, value)?);
        }
        rows.push(values);
    }
    drop(query);
    drop(statement);

    SqliteReferenceResult::try_new(columns, rows, row_order)
}

fn sqlite_reference_value(
    scenario_id: &str,
    column: usize,
    kind: SqliteReferenceColumnKind,
    value: ValueRef<'_>,
) -> Result<SqliteReferenceValue, SqliteAdapterError> {
    let mapped = match (kind, value) {
        (_, ValueRef::Null) => SqliteReferenceValue::Null,
        (SqliteReferenceColumnKind::Blob, ValueRef::Blob(value)) => {
            SqliteReferenceValue::Blob(value.to_vec())
        }
        (SqliteReferenceColumnKind::Boolean, ValueRef::Integer(0)) => {
            SqliteReferenceValue::Boolean(false)
        }
        (SqliteReferenceColumnKind::Boolean, ValueRef::Integer(1)) => {
            SqliteReferenceValue::Boolean(true)
        }
        (SqliteReferenceColumnKind::Decimal, ValueRef::Integer(value)) => {
            SqliteReferenceValue::Decimal {
                mantissa: i128::from(value),
                scale: 0,
            }
        }
        (SqliteReferenceColumnKind::Integer, ValueRef::Integer(value)) => {
            SqliteReferenceValue::Integer(value)
        }
        (SqliteReferenceColumnKind::Text, ValueRef::Text(value)) => {
            let text = std::str::from_utf8(value).map_err(|error| {
                SqliteAdapterError::new(
                    SqliteAdapterErrorKind::Result,
                    format!(
                        "SQLite scenario {scenario_id:?} column {column} returned invalid UTF-8: {error}",
                    ),
                )
            })?;
            SqliteReferenceValue::Text(text.to_string())
        }
        (_, value) => {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Result,
                format!(
                    "SQLite scenario {scenario_id:?} column {column} returned {:?} for {kind:?}",
                    value.data_type(),
                ),
            ));
        }
    };

    Ok(mapped)
}

/// Exercise every declared common value family through the real bundled
/// extraction path without expanding the product differential profile.
#[cfg(test)]
pub(crate) fn execute_value_mapping_probe() -> Result<Vec<SqliteReferenceValue>, SqliteAdapterError>
{
    let connection = Connection::open_in_memory().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Connection,
            "failed to open bundled SQLite value-mapping probe",
            source,
        )
    })?;
    verify_sqlite_environment(&connection)?;
    apply_and_verify_connection_policy(&connection)?;
    let scenario = crate::required_sqlite_reference_scenarios()[0];
    let kinds = [
        SqliteReferenceColumnKind::Blob,
        SqliteReferenceColumnKind::Boolean,
        SqliteReferenceColumnKind::Integer,
        SqliteReferenceColumnKind::Text,
        SqliteReferenceColumnKind::Text,
    ];

    let mut statement = connection
        .prepare("SELECT X'0001ff', TRUE, 7, NULL, 'text'")
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Result,
                "failed to prepare bundled SQLite value-mapping probe",
                source,
            )
        })?;
    let mut query = statement.query([]).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Result,
            "failed to execute bundled SQLite value-mapping probe",
            source,
        )
    })?;
    let row = query
        .next()
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Result,
                "failed to read bundled SQLite value-mapping probe",
                source,
            )
        })?
        .ok_or_else(|| {
            SqliteAdapterError::new(
                SqliteAdapterErrorKind::Result,
                "bundled SQLite value-mapping probe returned no row",
            )
        })?;
    let mut values = Vec::with_capacity(kinds.len());
    for (column, kind) in kinds.into_iter().enumerate() {
        let value = row.get_ref(column).map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Result,
                format!("failed to read bundled SQLite value-mapping column {column}"),
                source,
            )
        })?;
        values.push(sqlite_reference_value(scenario.id(), column, kind, value)?);
    }

    Ok(values)
}
