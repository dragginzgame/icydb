use icydb::{
    db::DbSession,
    traits::{CanisterKind, EntityFor},
};

fn trusted_sql_query_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_trusted_sql_query::<E>(sql);
}

fn trusted_sql_mutation_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_trusted_sql_mutation::<E>(sql);
}

fn trusted_sql_update_contracts_compile<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_trusted_sql_exact_update::<E>(sql, 10);
    let _ = db.execute_trusted_sql_prefix_update::<E>(sql);
    if let Ok(continuation) =
        db.prepare_trusted_sql_resumable_update::<E>(icydb::types::Ulid::MIN, sql)
    {
        let _ = icydb::db::TrustedResumableUpdateContinuation::try_from_bytes(
            continuation.as_bytes().to_vec(),
        );
        let _ = db.resume_trusted_sql_resumable_update::<E>(sql, &continuation);
    }
}

fn admin_sql_ddl_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_admin_sql_ddl::<E>(sql);
}

fn main() {}
