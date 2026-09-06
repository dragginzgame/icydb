//!
//! One-entity SQL query endpoint used for wasm-footprint auditing.
//!

#[cfg(feature = "sql")]
use icydb::{
    Error, ErrorCode, ErrorOrigin,
    db::{SqlStatementDispatch, sql::SqlQueryResult, sql_statement_dispatch},
    types::Ulid,
    value::InputValue,
};
#[cfg(feature = "sql")]
use std::cell::OnceCell;

#[cfg(not(feature = "sql"))]
icydb::start!();

#[cfg(feature = "sql")]
icydb::start! {
    init() => initialize_application_queries;
    post_upgrade() => initialize_application_queries;
}

#[cfg(feature = "sql")]
thread_local! {
    static ROW_QUERY: OnceCell<Result<SqlStatementDispatch<'static>, Error>> = const { OnceCell::new() };
}

// Syntax is initialized in committed lifecycle messages, never lazily in a
// query whose heap changes would be discarded. It carries no session/authority.
#[cfg(feature = "sql")]
pub(crate) fn initialize_application_queries() {
    ROW_QUERY.with(|query| {
        let _ = query.set(
            sql_statement_dispatch("SELECT * FROM OneSimpleEntity01 WHERE id = ?")
                .map_err(Error::from),
        );
    });
}

// Application-owned query taking a domain argument, not caller-authored SQL.
#[cfg(feature = "sql")]
fn get_row(id: Ulid) -> Result<SqlQueryResult, Error> {
    icydb::db::with_request_execution(|| {
        let database = db()?;
        ROW_QUERY.with(|query| {
            let dispatch = query
                .get()
                .ok_or_else(|| {
                    Error::from_error_code(ErrorCode::QUERY_VALIDATE, ErrorOrigin::Query)
                })?
                .as_ref()
                .map_err(Clone::clone)?;
            database.execute_trusted_sql_query_dispatch(dispatch, &[InputValue::ulid(id)])
        })
    })
}

#[cfg(feature = "sql")]
#[ic_cdk::query]
fn query_one_entity_sql() -> u32 {
    match get_row(Ulid::MIN) {
        Ok(SqlQueryResult::Projection(output)) => output.row_count,
        _ => u32::MAX,
    }
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
