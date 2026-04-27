use super::*;
use crate::db::FieldRef;

fn assert_explain_hash_and_route_use_cached_plan<E>(
    session: &DbSession<SessionSqlCanister>,
    query: &Query<E>,
    label: &str,
) where
    E: EntityValue + EntityKind<Canister = SessionSqlCanister>,
{
    let (uncached_explain, uncached_hash, uncached_execution) = session
        .with_query_visible_indexes(query, |query, visible_indexes| {
            Ok((
                query.explain_with_visible_indexes(visible_indexes)?,
                query.plan_hash_hex_with_visible_indexes(visible_indexes)?,
                query.explain_execution_with_visible_indexes(visible_indexes)?,
            ))
        })
        .unwrap_or_else(|err| {
            panic!("{label}: uncached query-owned planner surfaces should build: {err:?}")
        });

    let cached_explain = session
        .explain_query_with_visible_indexes(query)
        .unwrap_or_else(|err| panic!("{label}: cached explain should build: {err:?}"));
    let cached_hash = session
        .query_plan_hash_hex_with_visible_indexes(query)
        .unwrap_or_else(|err| panic!("{label}: cached hash should build: {err:?}"));
    let cached_execution = session
        .explain_query_execution_with_visible_indexes(query)
        .unwrap_or_else(|err| panic!("{label}: cached execution explain should build: {err:?}"));
    let verbose = session
        .explain_query_execution_verbose_with_visible_indexes(query)
        .unwrap_or_else(|err| {
            panic!("{label}: cached verbose execution explain should build: {err:?}")
        });
    let trace = session
        .trace_query(query)
        .unwrap_or_else(|err| panic!("{label}: trace should build: {err:?}"));

    assert_eq!(
        cached_explain, uncached_explain,
        "{label}: cached explain must match uncached query-owned visible-index planning",
    );
    assert_eq!(
        cached_hash, uncached_hash,
        "{label}: cached plan hash must match uncached query-owned visible-index planning",
    );
    assert_eq!(
        trace.plan_hash(),
        cached_hash,
        "{label}: trace plan hash must come from the same cached plan",
    );
    assert_eq!(
        cached_execution, uncached_execution,
        "{label}: cached execution explain must match uncached query-owned visible-index planning",
    );
    assert!(
        verbose.contains(&format!(
            "{:?} execution_mode=",
            cached_execution.node_type()
        )),
        "{label}: verbose execution explain should render the same root route family",
    );
}

#[test]
fn session_query_explain_hash_and_trace_converge_on_cached_scalar_plans() {
    reset_session_sql_store();
    let session = sql_session();

    let full_scan = Query::<SessionSqlEntity>::new(MissingRowPolicy::Ignore);
    assert_explain_hash_and_route_use_cached_plan(&session, &full_scan, "full scan");

    let by_primary_key = Query::<SessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").eq(Ulid::from_u128(9_901)))
        .order_term(crate::db::asc("id"));
    assert_explain_hash_and_route_use_cached_plan(&session, &by_primary_key, "primary key");

    let ordered = Query::<SessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"));
    assert_explain_hash_and_route_use_cached_plan(&session, &ordered, "ordered");

    let paged = Query::<SessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("id"))
        .limit(2)
        .offset(1);
    assert_explain_hash_and_route_use_cached_plan(&session, &paged, "paged");
}

#[test]
fn session_query_explain_hash_and_trace_converge_on_cached_index_plan() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let secondary_index = Query::<IndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"));

    assert_explain_hash_and_route_use_cached_plan(&session, &secondary_index, "secondary index");
}
