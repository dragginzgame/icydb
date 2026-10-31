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

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    #[allow(unused_mut)]
    let mut grants = vec![icydb::db::MemoryAuthority::new(
        "icydb.facade_only",
        "icydb.facade_only.",
    )?];
    #[allow(unused_mut)]
    let mut exclusions = vec![];
    #[cfg(all(test, not(target_arch = "wasm32")))]
    startup_admission::extend_pool(&mut grants, &mut exclusions);
    icydb::db::MemoryAllocationPool::new(grants, exclusions)
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
mod tests {
    use std::error::Error as _;

    use runtime_api::db::{TypedAdapterError, TypedEntityDescriptor};
    use runtime_api::model::{TypedInputValue, TypedNamedType, TypedOutputValue};
    use runtime_api::traits::EntitySource;

    use super::{FacadePlayer, FacadeProfile};

    fn assert_named_value<T: TypedInputValue + TypedNamedType + TypedOutputValue>() {}

    #[test]
    fn facade_owns_model_authoring_and_generated_runtime_paths() {
        assert_named_value::<FacadeProfile>();
        let _ = FacadePlayer::PROFILE;
    }

    #[test]
    fn generated_record_round_trips_inside_an_authored_blob() {
        let records = vec![FacadeProfile {
            rank: 7,
            label: "nested".to_string(),
        }];
        let blob = serde_json::to_vec(&records).unwrap();
        assert_eq!(
            serde_json::from_slice::<Vec<FacadeProfile>>(&blob).unwrap(),
            records
        );
    }

    #[test]
    fn single_package_native_driver_reads_and_writes() {
        crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
        assert_eq!(super::profile_rank().unwrap(), None);
        super::insert_profile(19).unwrap();
        assert_eq!(super::profile_rank().unwrap(), Some(19));
    }

    #[test]
    fn native_database_threads_are_isolated_and_driving_preserves_rows() {
        crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
        super::insert_profile(7).unwrap();

        // Every worker uses the same generated actor and primary key. Its fresh
        // runtime must neither inherit nor reset the caller's database.
        for rank in [19, 37] {
            std::thread::spawn(move || {
                crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
                assert_eq!(super::profile_rank().unwrap(), None);
                super::insert_profile(rank).unwrap();
                crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
                assert_eq!(super::profile_rank().unwrap(), Some(rank));
            })
            .join()
            .unwrap();
            assert_eq!(super::profile_rank().unwrap(), Some(7));
        }
    }

    #[test]
    fn generated_readiness_survives_schema_receipt_retention() {
        use crate::__icydb_generated::{
            __drive_native_database_for_tests, ICYDB_SCHEMA_ENTITY_STORES, ICYDB_SCHEMA_FRAGMENT,
        };

        __drive_native_database_for_tests().unwrap();
        super::insert_profile(7).unwrap();
        // More than two full receipt windows: control-only NoOps create no
        // journal debt whose wakeup could hide a lost readiness receipt.
        for ordinal in 0..130 {
            runtime_api::db::with_request_execution(|| {
                crate::db()
                    .unwrap()
                    .apply_generated_schema_fragment(
                        ICYDB_SCHEMA_FRAGMENT,
                        None,
                        &format!("retention-{ordinal}"),
                        ICYDB_SCHEMA_ENTITY_STORES,
                    )
                    .unwrap();
            });
            assert!(matches!(
                crate::startup_state().unwrap(),
                runtime_api::db::DatabaseStartupState::Ready,
            ));
        }
        __drive_native_database_for_tests().unwrap();
        assert_eq!(super::profile_rank().unwrap(), Some(7));
    }

    #[test]
    fn generated_schema_binding_mismatch_keeps_source_context() {
        crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
        runtime_api::db::with_request_execution(|| {
            let session = crate::db().unwrap();
            FacadePlayer::typed_binding(&session).unwrap();
            for (descriptor, entity, field) in [
                (
                    TypedEntityDescriptor::new("fixture::MissingEntity", &[], &[]),
                    "fixture::MissingEntity",
                    None,
                ),
                (
                    TypedEntityDescriptor::new(
                        FacadePlayer::ENTITY,
                        &["fixture::MissingField"],
                        &[],
                    ),
                    FacadePlayer::ENTITY,
                    Some("fixture::MissingField"),
                ),
            ] {
                let error = session.bind_typed_entity(&descriptor).unwrap_err();
                let TypedAdapterError::BindingUnavailable(context) = error
                    .source()
                    .and_then(|cause| cause.downcast_ref::<TypedAdapterError>())
                    .expect("typed binding failures retain their adapter source")
                else {
                    panic!("expected generated binding source context");
                };
                assert_eq!(context.entity_source(), entity);
                assert_eq!(context.field_source(), field);
            }
            // A rejected descriptor cannot replace or repair accepted authority.
            FacadePlayer::typed_binding(&session).unwrap();
        });
    }
}

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
