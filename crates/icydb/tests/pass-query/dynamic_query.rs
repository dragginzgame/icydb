use icydb::{
    db::{DbSession, DynamicQuery, TypedEntityAdapter, query::{Query, count}},
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

    let grouped = DynamicQuery::new("app::User")
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25);
    let _ = db.execute_public_dynamic_grouped_query(&grouped);
    let _ = db.execute_trusted_dynamic_grouped_query(&grouped);
}

fn typed_grouped_queries_compile_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25)
        .execute_grouped();
}

fn main() {}
