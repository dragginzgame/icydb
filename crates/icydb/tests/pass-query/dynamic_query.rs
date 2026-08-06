use icydb::{
    db::{
        DbSession, DynamicQuery, TypedEntityAdapter,
        query::{Query, count},
    },
    traits::CanisterKind,
};

#[allow(dead_code)]
fn dynamic_queries_compile_without_sql<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let request = DynamicQuery::new("app::User")
        .select(["name", "age"])
        .limit(25);
    let _ = db.execute_live_page(&request, None);
    let _ = db.execute_trusted_live_page(&request, None);

    let grouped = DynamicQuery::new("app::User")
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25);
    let _ = db.execute_public_dynamic_grouped_query(&grouped);
    let _ = db.execute_trusted_dynamic_grouped_query(&grouped);
}

#[allow(dead_code)]
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

#[test]
fn public_query_facade_compile_contract() {}
