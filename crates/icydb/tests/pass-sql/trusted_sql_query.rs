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

fn admin_sql_ddl_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_admin_sql_ddl::<E>(sql);
}

fn main() {}
