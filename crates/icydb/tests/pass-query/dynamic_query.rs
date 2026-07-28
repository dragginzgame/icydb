use icydb::{
    db::{DbSession, DynamicQuery},
    traits::CanisterKind,
};

fn dynamic_queries_compile_without_sql<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let request = DynamicQuery::new("app::User")
        .select(["name", "age"])
        .limit(25);
    let _ = db.execute_public_dynamic_query(&request);
    let _ = db.execute_trusted_dynamic_query(&request);
}

fn main() {}
