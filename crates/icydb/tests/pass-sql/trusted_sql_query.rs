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
    let operation_id = icydb::types::Ulid::MIN;
    if let Ok(continuation) = db.prepare_trusted_sql_resumable_update(operation_id, sql) {
        let _ = icydb::db::TrustedResumableUpdateContinuation::try_from_bytes(
            continuation.as_bytes().to_vec(),
        );
        let _ = db.resume_trusted_sql_resumable_update(operation_id, sql, &continuation);
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
