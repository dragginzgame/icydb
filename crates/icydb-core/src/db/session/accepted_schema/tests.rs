//! Heap schema publications schedule reconciliation and replicated cache preparation.

use super::*;
use crate::{
    db::{
        DatabaseStartupState, DynamicMutation, DynamicQuery, DynamicStructuralPatch,
        DynamicWriteCell, GeneratedStartupDriverStep, RequestExecutionRoot, StoreRegistry,
        commit::install_startup_recovery_wakeup,
        data::DataStore,
        drive_generated_startup_recovery_page,
        index::IndexStore,
        observe_generated_startup_state,
        registry::{StoreAllocationIdentities, StoreRuntimeStorageCapabilities},
        schema::{
            SchemaMigrationCommand, SchemaMigrationPhase, apply_generated_schema, migrate_schema,
            schema_application_target,
        },
    },
    traits::Path,
    value::{InputValue, OutputValue},
};
use icydb_schema::{
    DeclaredEntityVersion, EntityFragment, EntityMigration, EntitySourceKey, EntityStoreAssignment,
    FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType, ScalarType, SchemaCapability,
    SchemaFragment, SchemaMigrationPlan, SchemaName, SchemaProposal, SchemaSubmissionKey,
};
use std::cell::Cell;

const STORE: &str = "runtime_wakeup::HeapStore";
const INITIAL: &str = "generated/heap-wakeup-initial";
const RENAMED: &str = "generated/heap-wakeup-renamed";

struct HeapCanister;

impl Path for HeapCanister {
    const PATH: &'static str = "runtime_wakeup::Canister";
}

impl CanisterKind for HeapCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(170)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.schema_wakeup.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(171)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.schema_wakeup.startup.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(172)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.schema_wakeup.integrity.v1";
}

thread_local! {
    static DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static STORES: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(STORE, &DATA, &INDEX, &SCHEMA,
            StoreAllocationIdentities::absent(), StoreRuntimeStorageCapabilities::heap()).unwrap();
        registry
    };
    static WAKEUPS: Cell<u32> = const { Cell::new(0) };
}

fn wakeup() {
    WAKEUPS.with(|count| count.set(count.get() + 1));
}

fn proposal(session: &DbSession<HeapCanister>, renamed: bool) -> SchemaProposal {
    let target = schema_application_target(&session.db).unwrap();
    let entity_name = if renamed { "CatalogItem" } else { "Item" };
    let entity = EntityFragment::try_new(
        SchemaName::try_new(entity_name).unwrap(),
        DeclaredEntityVersion::try_new(if renamed { 2 } else { 1 }).unwrap(),
        vec![FieldFragment::new(
            SchemaName::try_new("id").unwrap(),
            FieldType::Scalar(ScalarType::Nat64),
            false,
            FieldInsertPolicy::Required,
            None,
        )],
        vec![FieldSourceKey::try_new("id").unwrap()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let assignment =
        EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity());
    SchemaProposal::try_compose(
        if renamed {
            vec![SchemaCapability::VERSIONED_MIGRATIONS]
        } else {
            Vec::new()
        },
        target.database_identity(),
        SchemaSubmissionKey::try_new(if renamed { RENAMED } else { INITIAL }).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(vec![entity], Vec::new()).unwrap()],
        vec![assignment],
        Vec::new(),
        renamed.then(|| {
            SchemaMigrationPlan::try_new(vec![
                EntityMigration::try_new(
                    EntitySourceKey::try_new(entity_name).unwrap(),
                    DeclaredEntityVersion::try_new(1).unwrap(),
                    Some(EntitySourceKey::try_new("Item").unwrap()),
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap(),
            ])
            .unwrap()
        }),
    )
    .unwrap()
}

fn initialize() -> DbSession<HeapCanister> {
    install_startup_recovery_wakeup(wakeup);
    let session = DbSession::new(&STORES, &RequestExecutionRoot::__new_runtime_root());
    assert!(session.db.drive_startup_recovery_page().unwrap());
    apply_generated_schema(&session.db, &proposal(&session, false)).unwrap();
    assert_eq!(
        observe_generated_startup_state::<HeapCanister>(&STORES, INITIAL),
        Ok(DatabaseStartupState::Ready)
    );
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &STORES, INITIAL).unwrap(),
        GeneratedStartupDriverStep::Terminal
    );
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "Item".into(),
            patch: DynamicStructuralPatch::new(vec![(
                "id".into(),
                DynamicWriteCell::Value(InputValue::nat64(1)),
            )]),
        })
        .unwrap();
    WAKEUPS.with(|count| count.set(0));
    session
}

fn cached_root(session: &DbSession<HeapCanister>) -> Option<Rc<AcceptedSchemaRuntimeRoot>> {
    ACCEPTED_SCHEMA_RUNTIME_ROOTS
        .with(|roots| roots.borrow().get(&session.db.cache_scope_id()).cloned())
}

#[test]
fn heap_ddl_wakes_the_ready_driver_and_prepares_the_current_root() {
    let session = initialize();
    let before = cached_root(&session).unwrap();
    assert!(
        session
            .db
            .store_handle(STORE)
            .unwrap()
            .journal_tail_store()
            .is_none()
    );
    for (version, sql) in [
        (1, "ALTER TABLE Item ADD COLUMN note TEXT NULL"),
        (2, "CREATE INDEX extra_id ON Item (id)"),
        (3, "DROP INDEX extra_id ON Item"),
    ] {
        WAKEUPS.with(|count| count.set(0));
        session
            .execute_admin_sql_ddl(&format!(
                "{sql} EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {}",
                version + 1
            ))
            .unwrap();
        assert_eq!(WAKEUPS.with(Cell::get), 1);
        assert!(
            cached_root(&session).is_none(),
            "DDL should leave preparation to the scheduled driver"
        );
        assert_eq!(
            observe_generated_startup_state::<HeapCanister>(&STORES, INITIAL),
            Ok(DatabaseStartupState::Ready)
        );
        assert_eq!(
            drive_generated_startup_recovery_page(&session, &STORES, INITIAL).unwrap(),
            GeneratedStartupDriverStep::Terminal
        );
        let prepared = cached_root(&session).unwrap();
        assert_ne!(prepared.identity(), before.identity());
        let current = session
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
            .unwrap();
        assert!(
            Rc::ptr_eq(&prepared, &current),
            "the next read must reuse the driver's preparation"
        );
        let page = session
            .execute_trusted_live_page(&DynamicQuery::new("Item").select(["id"]).limit(1), None)
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::nat64(1)]]);
    }
}

#[test]
fn heap_metadata_migration_wakes_reconciliation_without_another_upgrade() {
    let session = initialize();
    let candidate = proposal(&session, true);
    let error = apply_generated_schema(&session.db, &candidate).unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::schema_migration(
            icydb_diagnostic_code::SchemaMigrationCode::MigrationInProgress
        )
        .diagnostic_code()
    );
    assert_eq!(WAKEUPS.with(Cell::get), 0);
    assert_eq!(
        observe_generated_startup_state::<HeapCanister>(&STORES, RENAMED),
        Ok(DatabaseStartupState::Recovering)
    );
    let status = migrate_schema(
        &session.db,
        &candidate,
        SchemaMigrationCommand::Advance {
            expected_database: candidate.target_database(),
            expected_head: candidate.expected_head().clone(),
            expected_plan: candidate.migration().unwrap().digest(),
            acknowledged_finding_page: None,
        },
    )
    .unwrap();
    assert_eq!(status.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(
        WAKEUPS.with(Cell::get),
        1,
        "completed heap migration must re-arm the stopped watchdog"
    );
    // Consume the scheduled work through the same Core steps as the generated
    // adapter: current proposal binding, generated reconciliation, then prepare.
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &STORES, RENAMED).unwrap(),
        GeneratedStartupDriverStep::ApplyGeneratedSchema
    );
    apply_generated_schema(&session.db, &proposal(&session, true)).unwrap();
    assert_eq!(
        observe_generated_startup_state::<HeapCanister>(&STORES, RENAMED),
        Ok(DatabaseStartupState::Ready)
    );
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &STORES, RENAMED).unwrap(),
        GeneratedStartupDriverStep::Terminal
    );
    let prepared = cached_root(&session).unwrap();
    assert!(Rc::ptr_eq(
        &prepared,
        &session
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
            .unwrap()
    ));
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("CatalogItem").select(["id"]).limit(1),
            None,
        )
        .unwrap();
    assert_eq!(page.rows, vec![vec![OutputValue::nat64(1)]]);
}
