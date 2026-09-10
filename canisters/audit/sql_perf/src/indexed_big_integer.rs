//! Fixed indexed-scalar admission probes; never included in production actors.

use crate::{authored, db, query_validate_error};
use candid::CandidType;
use ic_cdk::update;
use icydb::{
    Error,
    db::{StructuralMutation, StructuralPatch, sql::SqlQueryResult},
    value::InputValue,
};

#[derive(CandidType)]
pub(crate) struct IndexedBigIntegerAttempt {
    outcome: Result<u32, Error>,
    instructions: u64,
}

/// Add two accepted indexes to the empty wide-scalar fixture.
#[update]
fn prepare_indexed_big_integer_fixture() -> Result<(), Error> {
    icydb::db::with_request_execution(|| {
        for (field, version) in [("signed", 1), ("unsigned", 2)] {
            let sql = format!(
                "CREATE INDEX perf_big_{field}_idx ON PerfAuditIndexedBigInteger ({field}) \
                 EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {}",
                version + 1,
            );
            let result = db()?.execute_admin_sql_ddl(&sql)?;
            let SqlQueryResult::Ddl {
                rows_scanned,
                index_keys_written,
                status,
                ..
            } = result
            else {
                return Err(query_validate_error());
            };
            if rows_scanned != 0 || index_keys_written != 0 || status != "published" {
                return Err(query_validate_error());
            }
        }
        Ok(())
    })
}

/// Measure successful and rejected engine work after typed fixture construction.
/// The oversized shape places its invalid value last to exercise batch atomicity.
#[update]
fn measure_indexed_big_integer_write(
    negative: bool,
    digits: u32,
    rows: u16,
) -> Result<IndexedBigIntegerAttempt, Error> {
    if ![20, 300, 4_092, 4_093, 4_094].contains(&digits) || ![1, 32].contains(&rows) {
        return Err(query_validate_error());
    }
    // Tag + length use three bytes; signed values also need the sign marker.
    // This selects a fixture, not engine admission: the shared encoder and
    // index-key builder remain responsible for accepting or rejecting it.
    let oversized = digits > if negative { 4_092 } else { 4_093 };
    let mut mutations = Vec::with_capacity(usize::from(rows));
    for id in 0..rows {
        let width = if oversized && id + 1 < rows {
            20
        } else {
            digits
        };
        let text = "9".repeat(width as usize);
        let signed = if negative {
            format!("-{text}")
        } else {
            "0".into()
        }
        .parse()
        .map_err(|_| query_validate_error())?;
        let unsigned = if negative { "0" } else { &text }
            .parse()
            .map_err(|_| query_validate_error())?;
        mutations.push(StructuralMutation::Insert {
            entity: "PerfAuditIndexedBigInteger".into(),
            patch: StructuralPatch::new()
                .field("id", authored(i32::from(id)))
                .field("signed", authored(InputValue::int_big(signed)))
                .field("unsigned", authored(InputValue::nat_big(unsigned))),
        });
    }
    let start = ic_cdk::api::performance_counter(1);
    let outcome = icydb::db::with_request_execution(|| {
        db()?
            .execute_trusted_structural_mutation_batch(mutations)
            .and_then(|results| u32::try_from(results.len()).map_err(|_| query_validate_error()))
    });
    Ok(IndexedBigIntegerAttempt {
        outcome,
        instructions: ic_cdk::api::performance_counter(1).saturating_sub(start),
    })
}
