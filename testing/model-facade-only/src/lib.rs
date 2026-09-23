//! Single-package host/runtime contract through a renamed IcyDB facade.

mod design;

#[cfg(all(test, target_os = "linux"))]
mod memory_attribution;

use design::{FacadePlayer, FacadePlayerInsert, FacadeProfile};
use runtime_api::{
    self as icydb,
    db::{TypedWriteAdapter as _, WriteCell},
    types::{Id, Ulid},
};

icydb::ic_memory_range!(
    authority = "icydb.facade_only",
    start = 10,
    end = 99,
    mode = Allowed
);

icydb::start!();

// Test-actor probe: measure database opening without executing a row query.
#[ic_cdk::update]
fn open_database() -> Result<u64, String> {
    let before = ic_cdk::api::performance_counter(0);
    icydb::db::with_request_execution(|| db().map(|_| ())).map_err(|error| error.to_string())?;
    Ok(ic_cdk::api::performance_counter(0) - before)
}

#[ic_cdk::update]
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

#[ic_cdk::query]
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
    use runtime_api::db::{TypedAdapterError, TypedEntityDescriptor, TypedOperationError};
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
                let TypedOperationError::Adapter(TypedAdapterError::BindingUnavailable(context)) =
                    error
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
