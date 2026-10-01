//! Bounded live-intersection qualification using the maintained streaming entity.

use crate::{db, insert_fixture_rows, query_validate_error, reset_perf_fixtures};
use candid::CandidType;
use ic_cdk::update;
use icydb::{
    Error,
    db::{
        DynamicQuery, ScalarPageWork,
        query::{FieldRef, FilterExpr, asc, desc},
    },
    types::{Blob, Timestamp},
    value::{OutputValue, PublicValue},
};
use icydb_testing_audit_sql_perf_fixtures::sql_perf::PerfAuditStreamingRow;

/// Compact audit receipt. Payload projection is executed inside the sample,
/// then discarded before reply encoding to stay below the IC response limit.
#[derive(CandidType)]
pub(crate) struct IntersectionPageSample {
    ids: Vec<i32>,
    continuation: Option<String>,
    work: ScalarPageWork,
    instructions: u64,
}

/// Cases: spaced sparse, disjoint, late overlap, dense, payload pagination,
/// and rotated prefixes that retain a large first child in three-way plans.
/// All populations have 160 rows. Loading is outside query cost intervals.
#[update]
fn load_seek_intersection_fixture(case: u8, start: u16) -> Result<u32, Error> {
    if case > 6 || start >= 160 || !start.is_multiple_of(4) {
        return Err(query_validate_error());
    }
    icydb::db::with_request_execution(|| {
        if start == 0 {
            reset_perf_fixtures()?;
        }
        // One batch per update lets the caller discharge journal byte debt.
        let start = i32::from(start);
        {
            let rows = (start..start + 4)
                .map(|id| {
                    let (a, b, c) = match case {
                        0 => (id < 128, id < 128 && id % 8 == 7, id < 128 && id % 16 == 15),
                        1 => (id < 128, (128..144).contains(&id), (128..136).contains(&id)),
                        2 => (id < 128, (112..128).contains(&id), (120..128).contains(&id)),
                        3 => (id < 128, id < 128, id < 128),
                        4 => (id < 128, (112..128).contains(&id), (116..128).contains(&id)),
                        5 => ((112..128).contains(&id), (120..128).contains(&id), id < 128),
                        _ => ((112..128).contains(&id), (116..128).contains(&id), id < 128),
                    };
                    PerfAuditStreamingRow {
                        id,
                        lane_a: i32::from(!a),
                        lane_b: i32::from(!b),
                        group_key: i32::from(!c),
                        sort_key: id % 2,
                        label: "seek-qualification".into(),
                        payload: Blob::from(vec![
                            7;
                            if [4, 6].contains(&case) && (112..128).contains(&id) {
                                1024 * 1024
                            } else {
                                16
                            }
                        ]),
                        created_at: Timestamp::default(),
                        updated_at: Timestamp::default(),
                    }
                })
                .collect();
            insert_fixture_rows(rows)?;
        }
        Ok(4)
    })
}

/// Measure a real engine-issued page. Inputs select a fixed audit workload;
/// they do not alter planner admission, physical routes or page budgets.
#[update]
fn measure_seek_intersection_page(
    children: u8,
    descending: bool,
    residual: bool,
    wide: bool,
    limit: Option<u32>,
    continuation: Option<String>,
) -> Result<IntersectionPageSample, Error> {
    if ![2, 3].contains(&children) || ![None, Some(1), Some(5)].contains(&limit) {
        return Err(query_validate_error());
    }
    let mut filters = vec![
        FieldRef::new("lane_a").eq(0_i32),
        FieldRef::new("lane_b").eq(0_i32),
    ];
    if children == 3 {
        filters.push(FieldRef::new("group_key").eq(0_i32));
    }
    if residual {
        filters.push(FieldRef::new("sort_key").eq(1_i32));
    }
    let mut request = DynamicQuery::new("PerfAuditStreamingRow")
        .filter(FilterExpr::and(filters))
        .order_by(if descending { desc("id") } else { asc("id") });
    request = if wide {
        request.select(["id", "payload"])
    } else {
        request.select(["id"])
    };
    if let Some(limit) = limit {
        request = request.limit(limit);
    }
    let start = ic_cdk::api::performance_counter(1);
    let page = icydb::db::with_request_execution(|| {
        db()?.execute_trusted_live_page(&request, continuation.as_deref())
    })?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    let ids = page
        .rows
        .iter()
        .map(|row| match row.first().map(OutputValue::as_public) {
            Some(PublicValue::Int64(value)) => {
                i32::try_from(*value).map_err(|_| query_validate_error())
            }
            _ => Err(query_validate_error()),
        })
        .collect::<Result<_, _>>()?;
    Ok(IntersectionPageSample {
        ids,
        continuation: page.continuation,
        work: page.work,
        instructions,
    })
}
