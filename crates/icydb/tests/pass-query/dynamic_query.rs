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
    let _ = db.execute_exhaustive_page(&request, None, None);
    let _ = db.execute_trusted_exhaustive_page(&request, None, None);
    let _ = db.capture_read_set_revision_proof(&["app::User"]);
    let exact = DynamicQuery::new("app::User");
    let _ = db.execute_exact_count(&exact);
    if let Ok(job_id) = icydb::db::ResumableJobId::try_from_bytes([1; 32]) {
        let _ = db.acknowledge_resumable_job(job_id, 1);
    }

    let grouped = DynamicQuery::new("app::User")
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25);
    let _ = db.execute_public_dynamic_grouped_query(&grouped);
    let _ = db.execute_trusted_dynamic_grouped_query(&grouped);
}

#[allow(dead_code)]
fn typed_exhaustive_queries_compile_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query.limit(25).execute_exhaustive_page(None, None);
}

#[allow(dead_code)]
fn typed_exact_count_compiles_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query.execute_exact_count();
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
