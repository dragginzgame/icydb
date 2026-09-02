//!
//! Frozen direct-versus-record-path SQL grouping comparison actor.
//!

#[cfg(feature = "sql")]
use ic_cdk::{query, update};
#[cfg(feature = "sql")]
use icydb::{
    ErrorCode, ErrorOrigin,
    db::{StructuralPatch, WriteCell, sql::SqlQueryResult},
    value::InputValue,
};

icydb::start!();

#[cfg(feature = "sql")]
const ENTITY: &str = "GroupPathAuditRow";
#[cfg(feature = "sql")]
const MAX_FIXTURE_ROWS: u32 = 2_048;
#[cfg(feature = "sql")]
const INSERT_BATCH_ROWS: u32 = 64;

#[cfg(feature = "sql")]
const fn query_validate_error() -> icydb::Error {
    icydb::Error::from_error_code(ErrorCode::QUERY_VALIDATE, ErrorOrigin::Query)
}

#[cfg(feature = "sql")]
fn authored(value: impl Into<InputValue>) -> WriteCell<InputValue> {
    WriteCell::Value(value.into())
}

#[cfg(feature = "sql")]
fn profile(rank: i32, optional_rank: Option<i32>) -> InputValue {
    InputValue::map(vec![
        (InputValue::from("rank"), InputValue::from(rank)),
        (
            InputValue::from("optional_rank"),
            optional_rank.map_or_else(InputValue::null, InputValue::from),
        ),
    ])
}

#[cfg(feature = "sql")]
fn fixture_patch(id: i32) -> StructuralPatch {
    let rank = id.rem_euclid(127);
    let optional_profile = match id.rem_euclid(3) {
        0 => InputValue::null(),
        1 => profile(rank, None),
        _ => profile(rank, Some(rank)),
    };

    StructuralPatch::new()
        .field("id", authored(id))
        .field("direct_rank", authored(rank))
        .field("profile", authored(profile(rank, Some(rank))))
        .field("optional_profile", authored(optional_profile))
}

/// Reset and load the frozen deterministic comparison rows.
#[cfg(feature = "sql")]
#[update]
fn load_group_path_fixture(row_count: u32) -> Result<u32, icydb::Error> {
    icydb::db::with_request_execution(|| {
        if row_count == 0 || row_count > MAX_FIXTURE_ROWS {
            return Err(query_validate_error());
        }

        let session = db()?;
        let _ = session.execute_trusted_sql_mutation("DELETE FROM GroupPathAuditRow")?;
        let mut inserted = 0u32;
        while inserted < row_count {
            let batch_end = inserted.saturating_add(INSERT_BATCH_ROWS).min(row_count);
            let patches = (inserted..batch_end)
                .map(|ordinal| {
                    i32::try_from(ordinal)
                        .map(fixture_patch)
                        .map_err(|_| query_validate_error())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let result = session.execute_trusted_structural_insert_batch(ENTITY, patches)?;
            inserted = inserted.saturating_add(result.affected_rows);
            if inserted != batch_end {
                return Err(query_validate_error());
            }
        }

        Ok(inserted)
    })
}

/// Execute one mutation statement for fixture control.
#[cfg(feature = "sql")]
#[update]
fn mutate_group_path(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_mutation(sql.as_str()))
}

/// Execute one admin DDL statement for fixed nested-index setup.
#[cfg(feature = "sql")]
#[update]
fn ddl_group_path(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_admin_sql_ddl(sql.as_str()))
}

/// Execute one direct or record-path comparison query.
#[cfg(feature = "sql")]
#[query]
fn query_group_path(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    icydb::db::with_request_execution(|| db()?.execute_trusted_sql_query(sql.as_str()))
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
