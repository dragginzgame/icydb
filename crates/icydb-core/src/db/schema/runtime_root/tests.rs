use super::{AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaRuntimeStoreRoot};
use crate::db::{
    integrity::DatabaseIncarnationId,
    schema::{AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests},
};

fn store_root(
    store_path: &'static str,
    revision: AcceptedSchemaRevision,
) -> AcceptedSchemaRuntimeStoreRoot {
    let candidate = empty_accepted_schema_candidate_for_tests(store_path, revision);
    AcceptedSchemaRuntimeStoreRoot::new(store_path, Some(candidate.root()))
}

#[test]
fn runtime_root_identity_binds_every_store_root_and_database_incarnation() {
    let first = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
        DatabaseIncarnationId::for_tests(1),
        &[
            store_root("test::First", AcceptedSchemaRevision::INITIAL),
            store_root("test::Second", AcceptedSchemaRevision::INITIAL),
        ],
    )
    .expect("canonical accepted roots should admit");
    let changed_store = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
        DatabaseIncarnationId::for_tests(1),
        &[
            store_root("test::First", AcceptedSchemaRevision::INITIAL),
            store_root("test::Second", AcceptedSchemaRevision::new(2)),
        ],
    )
    .expect("changed accepted root should admit");
    let changed_incarnation = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
        DatabaseIncarnationId::for_tests(2),
        &[
            store_root("test::First", AcceptedSchemaRevision::INITIAL),
            store_root("test::Second", AcceptedSchemaRevision::INITIAL),
        ],
    )
    .expect("changed database incarnation should admit");

    assert_ne!(first, changed_store);
    assert_ne!(first, changed_incarnation);
}

#[test]
fn runtime_root_identity_rejects_noncanonical_store_order() {
    let stores = [
        AcceptedSchemaRuntimeStoreRoot::new("test::Second", None),
        AcceptedSchemaRuntimeStoreRoot::new("test::First", None),
    ];

    assert!(
        AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            DatabaseIncarnationId::for_tests(1),
            &stores,
        )
        .is_err(),
    );
}
