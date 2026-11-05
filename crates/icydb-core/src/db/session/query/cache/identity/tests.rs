use super::SchemaCacheIdentity;
use crate::db::{
    integrity::DatabaseIncarnationId,
    schema::{
        AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaRuntimeStoreRoot,
        SchemaVersion, empty_accepted_schema_candidate_for_tests,
    },
};

fn runtime_root(fill: u8) -> AcceptedSchemaRuntimeRootIdentity {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::CacheIdentity",
        AcceptedSchemaRevision::INITIAL,
    );
    AcceptedSchemaRuntimeRootIdentity::from_store_roots(
        DatabaseIncarnationId::for_tests(fill),
        &[AcceptedSchemaRuntimeStoreRoot::new(
            "test::CacheIdentity",
            Some(candidate.root()),
        )],
    )
    .expect("cache identity root should admit")
}

#[test]
fn schema_cache_identity_rejects_mixed_runtime_roots() {
    let first = SchemaCacheIdentity::new(
        runtime_root(1),
        AcceptedSchemaRevision::INITIAL,
        SchemaVersion::initial(),
        1,
        [7; 16],
    );
    let second = SchemaCacheIdentity::new(
        runtime_root(2),
        AcceptedSchemaRevision::INITIAL,
        SchemaVersion::initial(),
        1,
        [7; 16],
    );

    assert_ne!(first, second);
}
