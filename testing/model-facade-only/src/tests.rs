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
                TypedEntityDescriptor::new(FacadePlayer::ENTITY, &["fixture::MissingField"], &[]),
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
