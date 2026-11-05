//! Single-package host/runtime contract through a renamed IcyDB facade.

mod design;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod startup_admission;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod request_budget;

#[cfg(all(test, target_os = "linux"))]
mod memory_attribution;

use design::{FacadePlayer, FacadePlayerInsert, FacadeProfile};
use runtime_api::{
    self as icydb,
    db::{TypedWriteAdapter as _, WriteCell},
    types::{Id, Ulid},
};

// The final host grants namespaces; components request permanent keys.
fn icydb_storage_grants() -> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    #[cfg_attr(
        not(all(test, not(target_arch = "wasm32"))),
        expect(
            unused_mut,
            reason = "only native admission tests adjust the allocation grants"
        )
    )]
    let mut grants = vec![icydb::db::MemoryAuthority::new(
        "icydb_facade_only",
        "icydb.facade_only.",
    )?];
    #[cfg(all(test, not(target_arch = "wasm32")))]
    startup_admission::adjust_grants(&mut grants);
    icydb::db::AllocationGrants::new(grants)
}

canic_icydb::start!(icydb);

// Test-actor probe: measure database opening without executing a row query.
fn open_database() -> Result<u64, String> {
    let before = ic_platform::ic0::performance_counter(0);
    icydb::db::with_request_execution(|| db().map(|_| ())).map_err(|error| error.to_string())?;
    Ok(ic_platform::ic0::performance_counter(0) - before)
}

fn insert_profile(rank: u64) -> Result<(), String> {
    icydb::db::with_request_execution(|| {
        let database = db().map_err(|error| error.to_string())?;
        let binding = FacadePlayer::typed_binding(&database).map_err(|error| error.to_string())?;
        let write = FacadePlayerInsert {
            id: WriteCell::Value(Id::from_key(Ulid::MIN)),
            profile: WriteCell::Value(FacadeProfile {
                rank,
                label: "shared-source".to_string(),
            }),
        }
        .encode_write(&binding)
        .map_err(|error| error.to_string())?;
        database
            .execute_trusted_typed_write_row(write)
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
}

fn profile_rank() -> Result<Option<u64>, String> {
    icydb::db::with_request_execution(|| {
        db().map_err(|error| error.to_string())?
            .get::<FacadePlayer>(Id::from_key(Ulid::MIN))
            .map(|row| row.map(|row| row.profile.rank))
            .map_err(|error| error.to_string())
    })
}

#[cfg(test)]
mod tests;

icydb_testing_canic_adapter::export_wire!(update, "open_database", OpenDatabase, open_database, (), Result<u64, String>);
icydb_testing_canic_adapter::export_wire!(update, "insert_profile", InsertProfile, insert_profile, (rank: u64), Result<(), String>);
icydb_testing_canic_adapter::export_wire!(
    query,
    "profile_rank",
    ProfileRank,
    profile_rank,
    (),
    Result<Option<u64>, String>
);
