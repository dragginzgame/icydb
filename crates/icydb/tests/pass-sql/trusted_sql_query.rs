use icydb::{
    Error,
    db::{DbSession, SqlStatementDispatch, sql::SqlQueryResult, sql_statement_dispatch},
    traits::CanisterKind,
    types::Ulid,
    value::InputValue,
};

// The application owns syntax independently of any session or binding values.
fn application_owned_query() -> Result<SqlStatementDispatch<'static>, Error> {
    Ok(sql_statement_dispatch(
        "SELECT id FROM Transfers WHERE id = ? AND amount >= ?",
    )?)
}

#[allow(dead_code)]
fn execute_application_query<C: CanisterKind>(
    db: &DbSession<C>,
    dispatch: &SqlStatementDispatch<'_>,
    id: Ulid,
    minimum_amount: u64,
) -> Result<SqlQueryResult, Error> {
    // The caller authorizes its domain operation before entering this helper.
    let bindings = [InputValue::ulid(id), InputValue::nat64(minimum_amount)];
    db.execute_trusted_sql_query_dispatch(dispatch, &bindings)
}

#[test]
fn application_owned_syntax_needs_no_session() {
    let dispatch = application_owned_query().expect("fixed application SQL should parse");
    assert_eq!(dispatch.entity_name(), Some("Transfers"));
}

#[allow(dead_code)]
fn trusted_sql_query_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let _ = db.execute_trusted_sql_query(sql);
}

#[allow(dead_code)]
fn trusted_sql_mutation_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let _ = db.execute_trusted_sql_mutation(sql);
}

#[allow(dead_code)]
fn trusted_sql_update_contracts_compile<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let _ = db.execute_trusted_sql_exact_update(sql, 10);
    let _ = db.execute_trusted_sql_prefix_update(sql);
    if let (Ok(job_id), Ok(idempotency_key)) = (
        icydb::db::MutationJobId::try_from_bytes([1; 32]),
        icydb::db::MutationJobIdempotencyKey::new("advance-0"),
    ) {
        let _ = db.start_trusted_sql_mutation_job(job_id, sql);
        let request = icydb::db::MutationJobAdvanceRequest::new(job_id, 0, idempotency_key);
        let _ = db.advance_trusted_mutation_job(&request);
    }
}

#[allow(dead_code)]
fn admin_sql_ddl_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let _ = db.execute_admin_sql_ddl(sql);
}

#[allow(dead_code)]
fn admin_integrity_sql_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let owner =
        icydb::db::IntegrityJobOwner::new("compile-test").expect("static owner should admit");
    let _ = db.execute_admin_integrity_sql(sql, owner);
}

#[test]
fn public_trusted_sql_facade_compile_contract() {}
