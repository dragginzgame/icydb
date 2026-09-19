//! Test-only application batch qualification; no runtime admission policy lives here.

use crate::{enrollment_user_input, typed_fixture_invariant_error, typed_operation_fixture_error};
use candid::CandidType;
use ic_cdk::{api::performance_counter, update};
use icydb::{
    Error,
    db::with_request_execution,
    types::{Id, Ulid},
};
use icydb_testing_test_sql_fixtures::sql::SqlTestEnrollmentUser;

// Fixed, disjoint source/result identities let the host check every row without
// using the application transformation as its expected-value oracle.
fn source_id(position: u32) -> Id<SqlTestEnrollmentUser> {
    Id::from_key(Ulid::from_bytes(
        (u128::from(position) + 1_000).to_be_bytes(),
    ))
}

#[derive(CandidType)]
pub(crate) struct BatchWorkloadSample {
    result: Result<(), Error>,
    rejected_at: Option<u32>,
    read_instructions: u64,
    validation_instructions: u64,
    write_instructions: u64,
    total_instructions: u64,
}

// Setup is a separate message and never part of the measured interval.
#[update]
fn seed_batch_workload(count: u32, reject_last: bool) -> Result<(), icydb::Error> {
    with_request_execution(|| {
        if count == 0 || count > 128 {
            return Err(typed_fixture_invariant_error());
        }
        let session = icydb::db!()?;
        let mut batch = session.trusted_typed_write_batch();
        for position in 0..count {
            let name = if reject_last && position == count - 1 {
                "blocked".to_string()
            } else {
                format!("eligible-{position}")
            };
            batch
                .push(enrollment_user_input(source_id(position), &name))
                .map_err(typed_operation_fixture_error)?;
        }
        batch.execute().map_err(typed_operation_fixture_error)?;
        Ok(())
    })
}

// One invocation is one real request scope and one atomic output batch. The
// host, not this endpoint, owns chunking across separate committed requests.
#[update]
fn measure_batch_workload(start: u32, count: u32, individual_reads: bool) -> BatchWorkloadSample {
    let total_start = performance_counter(1);
    let mut sample = BatchWorkloadSample {
        result: Ok(()),
        rejected_at: None,
        read_instructions: 0,
        validation_instructions: 0,
        write_instructions: 0,
        total_instructions: 0,
    };
    sample.result = with_request_execution(|| {
        if count == 0 || count > 128 || start > 128 - count {
            return Err(typed_fixture_invariant_error());
        }
        let session = icydb::db!()?;
        let read_start = performance_counter(1);
        let ids = (start..start + count).map(source_id).collect::<Vec<_>>();
        let rows = if individual_reads {
            ids.iter()
                .map(|id| session.get::<SqlTestEnrollmentUser>(*id))
                .collect()
        } else {
            session.get_many::<SqlTestEnrollmentUser>(&ids)
        };
        sample.read_instructions = performance_counter(1).saturating_sub(read_start);
        let rows = rows.map_err(typed_operation_fixture_error)?;

        // Validate all rows before staging any output. A late domain rejection
        // is an ordinary application result, not database-budget exhaustion.
        let validation_start = performance_counter(1);
        let rejected = rows.iter().zip(&ids).position(|(row, id)| {
            row.as_ref()
                .is_none_or(|row| row.id != id.key() || !row.display_name.starts_with("eligible-"))
        });
        sample.validation_instructions = performance_counter(1).saturating_sub(validation_start);
        if let Some(position) = rejected {
            sample.rejected_at =
                Some(start + u32::try_from(position).map_err(|_| typed_fixture_invariant_error())?);
            return Ok(());
        }

        let write_start = performance_counter(1);
        let result = (|| {
            let mut batch = session.trusted_typed_write_batch();
            for (position, row) in (start..start + count).zip(rows) {
                let id = Id::from_key(Ulid::from_bytes(
                    (u128::from(position) + 2_000).to_be_bytes(),
                ));
                let row = row.ok_or_else(typed_fixture_invariant_error)?;
                let name = format!("processed:{}", row.display_name);
                batch
                    .push(enrollment_user_input(id, &name))
                    .map_err(typed_operation_fixture_error)?;
            }
            batch.execute().map_err(typed_operation_fixture_error)?;
            Ok(())
        })();
        sample.write_instructions = performance_counter(1).saturating_sub(write_start);
        result
    });
    sample.total_instructions = performance_counter(1).saturating_sub(total_start);
    sample
}
