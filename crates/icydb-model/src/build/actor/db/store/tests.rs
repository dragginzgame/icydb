use super::*;
use crate::node::StoreJournaledMemoryConfig;
use crate::node::{Canister, Def, Schema};
use std::sync::Arc;

fn compact_tokens(tokens: TokenStream) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn heap_store() -> Store {
    Store::new_heap(
        Def::new("demo::schema", "ScratchStore"),
        "demo::schema::DemoCanister",
        StoreHeapConfig::new(),
    )
}

fn journaled_store() -> Store {
    Store::new_journaled(
        Def::new("demo::schema", "JournaledStore"),
        "demo::schema::DemoCanister",
        StoreJournaledMemoryConfig::new("store_20"),
    )
}

fn actor_builder() -> ActorBuilder {
    ActorBuilder::new(
        Arc::new(Schema::new()),
        Canister::new(Def::new("test", "Canister"), "test", None),
        icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
            .expect("empty test fragment should admit"),
        None,
        quote::quote!(::icydb),
    )
}

#[test]
fn generated_bootstrap_uses_current_storage_grants() {
    let rendered = compact_tokens(generate_store_wiring(&actor_builder(), quote!()));
    assert!(rendered.contains("ensure_default_storage_runtime"));
    assert!(rendered.contains("::icydb::db::StorageConfig::default()"));
    assert!(rendered.contains("crate::icydb_storage_grants"));
}

#[test]
fn ordinary_admission_checks_startup_state() {
    let rendered = compact_tokens(schema_bootstrap_tokens(&actor_builder()));

    assert!(rendered.contains("fnadmit_ordinary_database_work("));
    assert!(rendered.contains("DatabaseStartupState::Ready"));
    assert!(rendered.contains("__startup_recovery_pending()"));
}

#[test]
fn native_startup_driver_advances_bounded_database_work() {
    let rendered = compact_tokens(schema_bootstrap_tokens(&actor_builder()));
    for required in [
        "pub(crate)fnstartup_driver_attempt(",
        "__drive_generated_startup_recovery_page",
        "GeneratedStartupDriverStep::Terminal=>::std::result::Result::Ok(true)",
        "__icydb_startup_driver_attempt_for_tests",
        "__record_generated_schema_startup_failure",
        "__drive_native_database_for_tests",
        "while!startup_driver_attempt()?{}",
    ] {
        assert!(rendered.contains(required), "missing token: {required}");
    }
}

#[test]
fn startup_observation_is_ungated_and_never_becomes_an_automatic_endpoint() {
    let rendered = compact_tokens(startup_observation_tokens(&quote!(::icydb)));

    assert!(rendered.contains("pubfnstartup_state()->::std::result::Result<"));
    assert!(rendered.contains("ensure_memory_bootstrap()"));
    assert!(rendered.contains("__observe_generated_startup_state::<"));
    for forbidden in [
        "core_db(",
        "db(",
        "with_request_execution",
        "set_timer",
        "ic_cdk::query",
        "ic_cdk::update",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "forbidden token: {forbidden}"
        );
    }
}

#[test]
fn heap_store_wiring_uses_heap_initializers_and_absent_allocation_identity() {
    let store = heap_store();
    let (journal_def, data_def, index_def, schema_def, store_init) = store_registry_entry_tokens(
        &quote!(::icydb),
        0,
        "demo::schema::ScratchStore",
        &store,
        "demo",
    );
    let rendered = quote! {
        #journal_def
        #data_def
        #index_def
        #schema_def
        #store_init
    }
    .to_string();

    assert!(rendered.contains("DataStore :: init_heap"));
    assert!(rendered.contains("IndexStore :: init_heap"));
    assert!(rendered.contains("SchemaStore :: init_heap"));
    assert!(rendered.contains("StoreAllocationIdentities :: absent"));
    assert!(rendered.contains("StoreRuntimeStorageCapabilities :: heap"));
    assert!(rendered.contains("__ICYDB_STORE_0_DATA"));
    assert!(rendered.contains("__ICYDB_STORE_0_INDEX"));
    assert!(rendered.contains("__ICYDB_STORE_0_SCHEMA"));
    assert_eq!(rendered.matches("open_memory").count(), 0);
    assert_eq!(
        rendered
            .matches("StoreAllocationIdentity :: from_committed_key")
            .count(),
        0
    );
    assert!(!rendered.contains("ensure_memory_bootstrap"));
}

#[test]
fn journaled_store_wiring_declares_journal_memory_and_registers_four_role_allocation() {
    let store = journaled_store();
    let (journal_def, data_def, index_def, schema_def, store_init) = store_registry_entry_tokens(
        &quote!(::icydb),
        0,
        "demo::schema::JournaledStore",
        &store,
        "demo",
    );
    let rendered = quote! {
        #journal_def
        #data_def
        #index_def
        #schema_def
        #store_init
    }
    .to_string();

    assert_eq!(rendered.matches("open_memory").count(), 4);
    assert_eq!(
        rendered
            .matches("ic-storage committed allocation unavailable:")
            .count(),
        4
    );
    assert_eq!(
        rendered
            .matches("StoreAllocationIdentity :: from_committed_key")
            .count(),
        4
    );
    assert!(rendered.contains("JournalTailStore :: init"));
    assert_eq!(rendered.matches("init_journaled").count(), 3);
    assert!(rendered.contains("register_journaled_store"));
    assert!(rendered.contains("StoreAllocationIdentities :: new_journaled"));
    assert!(rendered.contains("StoreRuntimeStorageCapabilities :: journaled"));
    for role in ["data", "index", "schema", "journal"] {
        assert!(rendered.contains(&format!("icydb.demo.store.store_20.{role}.v1")));
    }
    assert!(!rendered.contains("init_heap"));
}

#[test]
fn store_registry_wiring_preserves_startup_admission() {
    let mut store_inits = quote!();
    store_inits.extend(
        store_registry_entry_tokens(
            &quote!(::icydb),
            0,
            "demo::schema::ScratchStore",
            &heap_store(),
            "demo",
        )
        .4,
    );
    let registry = StoreRegistryTokens {
        memory_authority: "icydb_demo".to_owned(),
        stable_keys: Vec::new(),
        journal_defs: quote!(),
        data_defs: quote!(),
        index_defs: quote!(),
        schema_defs: quote!(),
        store_inits,
    };

    let rendered = compact_tokens(store_wiring_tokens(
        &quote!(::icydb),
        registry,
        quote!(),
        quote!(),
        CanisterMemoryWiring {
            commit: "icydb.demo.commit.control.v1",
            startup: "icydb.demo.startup.control.v1",
            integrity_progress: "icydb.demo.integrity.progress.v1",
        },
    ));

    assert!(rendered.contains("pubfnicydb_memory_requests()"));
    assert!(rendered.contains("\"icydb.demo.startup.control.v1\""));
    assert!(rendered.contains("\"icydb.demo.integrity.progress.v1\""));
    assert!(rendered.contains("Result<(),::icydb::db::DatabaseBootstrapError>"));
    assert!(
        rendered
            .contains("::icydb::__macro::ensure_default_storage_runtime(\"icydb_demo\",&requirements,::icydb::db::StorageConfig::default(),crate::icydb_storage_grants,)")
    );
    assert!(rendered.contains("ensure_memory_bootstrap()?"));
    assert!(rendered.contains("pubfnstartup_state()->::std::result::Result<"));
    assert!(rendered.contains("__observe_generated_startup_state::<__IcydbGeneratedCanister>"));
    assert!(rendered.contains("::std::cell::OnceCell"));
    assert!(rendered.contains("MEMORY_BOOTSTRAP.with("));
    assert!(rendered.contains("constfnnative_test_startup_recovery_wakeup(){}"));
    assert!(
        rendered
            .contains("__install_startup_recovery_wakeup(native_test_startup_recovery_wakeup,)")
    );
    assert!(rendered.contains(
        "__install_startup_recovery_wakeup(crate::__icydb_host::ensure_startup_watchdog_scheduled_immediately,)"
    ));
    assert!(rendered.contains(
        "CoreDbSession::<__IcydbGeneratedCanister>::__new_from_current_request(&STORE_REGISTRY,)"
    ));
    assert!(rendered.contains(
        "CoreDbSession::<__IcydbGeneratedCanister>::new(&STORE_REGISTRY,request_root.__core(),)"
    ));
    assert!(rendered.contains("pubfndb()->::std::result::Result<"));
    assert_eq!(
        rendered.matches("admit_ordinary_database_work()?").count(),
        2
    );
    let admission = rendered
        .find("admit_ordinary_database_work()?")
        .expect("ordinary db construction should contain startup admission");
    let session = rendered
        .find("DbSession::new(core_db()?)")
        .expect("ordinary db construction should create its session");
    assert!(
        admission < session,
        "admission must precede session construction"
    );
    assert!(
        rendered.contains(
            "pubfndb_with_request_root(request_root:&::icydb::db::RequestExecutionRoot,)"
        )
    );
    assert!(rendered.contains("request_root.__ensure_compatible_with_current()?"));
    assert!(!rendered.contains("panic!("));
}
