mod recovery_reader;

use super::*;
use crate::{
    db::{
        data::DataStore,
        index::IndexStore,
        integrity::DatabaseIncarnationId,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldStorageDecode, LeafCodec,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot,
            SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_for_tests,
        },
    },
    traits::Path,
};
use std::cell::RefCell;

const STORE_PATH: &str = "runtime_entity_catalog::tests::Store";
const ENTITY_PATH: &str = "runtime_entity_catalog::tests::Entity";

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "runtime_entity_catalog::tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(1)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.runtime_entity_catalog.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(3)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.runtime_entity_catalog.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(2)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.runtime_entity_catalog.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("runtime entity test store should register");
        registry
    };
}

fn test_db() -> Db<TestCanister> {
    DATA_STORE.with_borrow_mut(|store| *store = DataStore::init_heap());
    INDEX_STORE.with_borrow_mut(|store| *store = IndexStore::init_heap());
    let entity_tag = EntityTag::new(91);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_PATH.to_string(),
        "Entity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );
    let candidate = accepted_schema_candidate_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        std::collections::BTreeMap::from([(entity_tag, snapshot)]),
    );
    SCHEMA_STORE.with(|store| {
        let mut store = store.borrow_mut();
        *store = SchemaStore::init_heap();
        store
            .publish_accepted_schema_candidate(
                DatabaseIncarnationId::for_tests(0x62),
                AcceptedSchemaRevision::NONE,
                &candidate,
            )
            .expect("accepted runtime candidate should publish");
    });

    Db::<TestCanister>::new(
        &STORE_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    )
}

#[test]
fn accepted_bundle_alone_supplies_runtime_entity_routing() {
    let db = test_db();
    let entity_tag = EntityTag::new(91);
    let by_tag =
        accepted_runtime_entity_for_tag(&db, entity_tag).expect("accepted tag should route");
    let by_path =
        accepted_runtime_entity_for_path(&db, ENTITY_PATH).expect("accepted path should route");
    let by_name = accepted_runtime_entity_for_name(&db, "Entity")
        .expect("accepted name lookup should succeed")
        .expect("accepted name should route");
    let enumerated =
        accepted_runtime_entities(&db).expect("accepted entity catalog should enumerate");

    assert_eq!(by_tag, by_path);
    assert_eq!(by_path, by_name);
    assert_eq!(by_tag.entity_path(), ENTITY_PATH);
    assert_eq!(by_tag.store_path(), STORE_PATH);
    assert_eq!(enumerated, vec![by_tag]);
    // Effective routing includes this heap-backed store; canonical replay
    // routing must continue to exclude it for both lookup identities.
    for result in [
        super::canonical_runtime_entity_for_tag(&db, entity_tag),
        super::canonical_runtime_entity_for_path(&db, ENTITY_PATH),
    ] {
        assert_eq!(
            result.unwrap_err().class(),
            crate::error::ErrorClass::Corruption
        );
    }
}
