use icydb::{db::DbSession, traits::CanisterKind};

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
