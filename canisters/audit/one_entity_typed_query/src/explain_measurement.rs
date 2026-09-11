//! Fixed application-owned workloads; never accepts SQL or query syntax over Candid.

#[cfg(feature = "typed-explain-measurement")]
use icydb::{
    db::query::{FieldRef, asc, count},
    types::Ulid,
};
#[cfg(feature = "typed-explain-measurement")]
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

#[cfg(feature = "typed-explain-measurement")]
#[ic_cdk::query]
fn measure_typed_explain(kind: u8) -> Result<Vec<(u64, u64, String, u64)>, u16> {
    icydb::db::with_request_execution(|| {
        let database = crate::db().map_err(|error| error.code().raw())?;
        let mut results = Vec::with_capacity(3);
        for _ in 0..3 {
            let start = ic_cdk::api::performance_counter(1);
            let query = database.query::<OneSimpleEntity01>().map_err(|_| 1_u16)?;
            let bound = ic_cdk::api::performance_counter(1);
            let query = match kind {
                0 => query.filter(FieldRef::new("id").eq(Ulid::MIN)),
                1 => query.order_by(asc("name")),
                2 => query
                    .group_by("name")
                    .aggregate(count())
                    .grouped_limits(10_000, 16 * 1024 * 1024),
                _ => return Err(2),
            };
            let plan = query.explain().map_err(|_| 3_u16)?;
            let planned = ic_cdk::api::performance_counter(1);
            let json = plan.render_json_canonical().map_err(|_| 4_u16)?;
            let rendered = ic_cdk::api::performance_counter(1);
            results.push((
                planned.saturating_sub(start),
                rendered.saturating_sub(planned),
                json,
                bound.saturating_sub(start),
            ));
        }
        Ok(results)
    })
}

#[cfg(feature = "sql-explain-measurement")]
#[ic_cdk::query]
fn measure_sql_explain(kind: u8) -> Result<Vec<(u64, u64, String)>, u16> {
    icydb::db::with_request_execution(|| {
        let database = crate::db().map_err(|error| error.code().raw())?;
        let sql = match kind {
            0 => {
                "EXPLAIN JSON SELECT * FROM OneSimpleEntity01 WHERE id = '00000000000000000000000000'"
            }
            1 => "EXPLAIN JSON SELECT * FROM OneSimpleEntity01 ORDER BY name",
            2 => "EXPLAIN JSON SELECT name, COUNT(*) FROM OneSimpleEntity01 GROUP BY name",
            _ => return Err(2),
        };
        let mut results = Vec::with_capacity(3);
        for _ in 0..3 {
            let start = ic_cdk::api::performance_counter(1);
            let result = database.execute_trusted_sql_query(sql).map_err(|_| 3_u16)?;
            let elapsed = ic_cdk::api::performance_counter(1).saturating_sub(start);
            let icydb::db::sql::SqlQueryResult::Explain { explain, .. } = result else {
                return Err(4);
            };
            // SQL's public terminal includes rendering; do not invent a split.
            results.push((elapsed, 0, explain));
        }
        Ok(results)
    })
}

// Retain a real SQL operation in mixed subjects, not just an unused feature.
#[cfg(all(feature = "sql", not(feature = "sql-explain-measurement")))]
#[ic_cdk::query]
fn retained_sql_read() -> Result<u32, u16> {
    icydb::db::with_request_execution(|| {
        let database = crate::db().map_err(|error| error.code().raw())?;
        let result = database
            .execute_trusted_sql_query("SELECT * FROM OneSimpleEntity01 ORDER BY id LIMIT 1")
            .map_err(|error| error.code().raw())?;
        match result {
            icydb::db::sql::SqlQueryResult::Projection(output) => Ok(output.row_count),
            _ => Err(1),
        }
    })
}
