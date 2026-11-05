//! Test-only helpers for the parent module.

use super::*;

/// Exercise every declared common value family through the real bundled
/// extraction path without expanding the product differential profile.
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
