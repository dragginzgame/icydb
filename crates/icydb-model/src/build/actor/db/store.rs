//! Module: db::store
//! Responsibility: generated store memory declarations and session accessors.
//! Does not own: store runtime behavior, memory manager implementation, or schema authority.
//! Boundary: translates validated store metadata into actor-local storage wiring tokens.

use crate::build::actor::ActorBuilder;
use crate::node::{Store, StoreHeapConfig, StoreJournaledMemoryConfig, StoreStorage};
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

///
/// StoreRegistryTokens
///
/// Generated token bundle for all store-memory definitions and registration
/// statements emitted for one actor. It keeps store wiring helpers below the
/// argument limit while preserving the generated-code phase boundary.
///

struct StoreRegistryTokens {
    memory_authority: String,
    journal_defs: TokenStream,
    data_defs: TokenStream,
    index_defs: TokenStream,
    schema_defs: TokenStream,
    store_inits: TokenStream,
}

/// Validated canister-owned memory declarations emitted around store wiring.
struct CanisterMemoryWiring<'a> {
    memory_min: u8,
    memory_max: u8,
    commit_memory_id: u8,
    commit_stable_key: &'a str,
    integrity_progress_memory_id: u8,
    integrity_progress_stable_key: &'a str,
}

/// Emit generated stores, optional frontends, and session accessors.
pub(super) fn generate_store_wiring(
    builder: &ActorBuilder,
    frontend_surfaces: TokenStream,
) -> TokenStream {
    let canister = &builder.canister;
    let memory_namespace = canister.memory_namespace();
    let memory_authority = format!("icydb.{memory_namespace}");
    let store_registry = store_registry_tokens(builder, memory_namespace, &memory_authority);
    let memory_min = canister.memory_min();
    let memory_max = canister.memory_max();
    let commit_memory_id = canister.commit_memory_id();
    let commit_stable_key = canister.commit_stable_key();
    let integrity_progress_memory_id = canister.integrity_progress_memory_id();
    let integrity_progress_stable_key = canister.integrity_progress_stable_key();
    let schema_bootstrap = schema_bootstrap_tokens(builder);

    store_wiring_tokens(
        store_registry,
        frontend_surfaces,
        schema_bootstrap,
        CanisterMemoryWiring {
            memory_min,
            memory_max,
            commit_memory_id,
            commit_stable_key: &commit_stable_key,
            integrity_progress_memory_id,
            integrity_progress_stable_key: &integrity_progress_stable_key,
        },
    )
}

fn schema_bootstrap_tokens(builder: &ActorBuilder) -> TokenStream {
    let fragment_bytes = &builder.schema_fragment_bytes;
    let migration_capability = builder.schema_migration_plan_bytes.as_ref().map_or_else(
        TokenStream::new,
        |_| quote!(::icydb::__icydb_require_migration_capability!();),
    );
    let migration_bytes = builder
        .schema_migration_plan_bytes
        .as_ref()
        .map_or_else(|| quote!(None), |bytes| quote!(Some(&[#(#bytes),*])));
    let apply_generated_schema = if builder.schema_migration_plan_bytes.is_some() {
        quote! {
            session.ensure_generated_schema_fragment(
                ICYDB_SCHEMA_FRAGMENT,
                ICYDB_SCHEMA_MIGRATION_PLAN,
                ICYDB_SCHEMA_SUBMISSION_KEY,
                ICYDB_SCHEMA_ENTITY_STORES,
            )
        }
    } else {
        quote! {
            session
                .apply_generated_schema_fragment(
                    ICYDB_SCHEMA_FRAGMENT,
                    ICYDB_SCHEMA_MIGRATION_PLAN,
                    ICYDB_SCHEMA_SUBMISSION_KEY,
                    ICYDB_SCHEMA_ENTITY_STORES,
                )
                .map(|_receipt| ())
        }
    };
    let submission_key = &builder.schema_submission_key;
    let entity_stores = builder
        .get_entities()
        .into_iter()
        .map(|(_, entity)| (entity.name().to_owned(), entity.store().to_owned()))
        .collect::<Vec<_>>();
    let entity_names = entity_stores.iter().map(|(name, _)| name);
    let store_paths = entity_stores.iter().map(|(_, store)| store);
    let recovery = schema_application_recovery_tokens();

    quote! {
        #migration_capability
        pub(super) const ICYDB_SCHEMA_FRAGMENT: &[u8] = &[#(#fragment_bytes),*];
        pub(super) const ICYDB_SCHEMA_MIGRATION_PLAN: ::std::option::Option<&[u8]> = #migration_bytes;
        pub(super) const ICYDB_SCHEMA_SUBMISSION_KEY: &str = #submission_key;
        pub(super) const ICYDB_SCHEMA_ENTITY_STORES: &[(&str, &str)] = &[
            #((#entity_names, #store_paths)),*
        ];

        fn apply_generated_schema(
            session: &::icydb::db::DbSession<__IcydbGeneratedCanister>,
        ) -> ::std::result::Result<(), ::icydb::Error> {
            #apply_generated_schema
        }

        thread_local! {
            static SCHEMA_APPLICATION: ::std::cell::OnceCell<()> =
                    const { ::std::cell::OnceCell::new() };
        }

        #recovery

        fn ensure_schema_application(
            session: &::icydb::db::DbSession<__IcydbGeneratedCanister>,
        ) -> ::std::result::Result<(), ::icydb::Error> {
            SCHEMA_APPLICATION.with(|application| {
                if application.get().is_some() {
                    return Ok(());
                } else if !session.__continue_startup_recovery()? {
                    schedule_schema_application_recovery();
                    return Ok(());
                }
                apply_generated_schema(session)?;
                match application.set(()) {
                    Ok(()) | Err(()) => Ok(()),
                }
            })
        }
    }
}

fn schema_application_recovery_tokens() -> TokenStream {
    quote! {
        thread_local! {
            static SCHEMA_APPLICATION_RECOVERY_TIMER_SCHEDULED: ::std::cell::Cell<bool> =
                    const { ::std::cell::Cell::new(false) };
        }

        fn schedule_schema_application_recovery() {
            let should_schedule = SCHEMA_APPLICATION_RECOVERY_TIMER_SCHEDULED.with(|scheduled| {
                !scheduled.replace(true)
            });
            if !should_schedule {
                return;
            }
            ::icydb::__reexports::ic_cdk_timers::set_timer(
                ::std::time::Duration::ZERO,
                async {
                    SCHEMA_APPLICATION_RECOVERY_TIMER_SCHEDULED
                        .with(|scheduled| scheduled.set(false));
                    ::icydb::db::with_request_execution(|| {
                        let result = core_db()
                            .map(::icydb::db::DbSession::new)
                            .and_then(|session| ensure_schema_application(&session));
                        if let Err(error) = result {
                            ::icydb::__reexports::ic_cdk::println!(
                                "IcyDB startup recovery failed (E{})",
                                error.code().raw(),
                            );
                        }
                    });
                },
            );
        }
    }
}

fn store_registry_tokens(
    builder: &ActorBuilder,
    memory_namespace: &str,
    memory_authority: &str,
) -> StoreRegistryTokens {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut schema_defs = quote!();
    let mut journal_defs = quote!();
    let mut store_inits = quote!();

    for (store_ordinal, (store_path, store)) in builder.get_stores().into_iter().enumerate() {
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                store_ordinal,
                &store_path,
                &store,
                memory_namespace,
                memory_authority,
            );
        journal_defs.extend(journal_def);
        data_defs.extend(data_def);
        index_defs.extend(index_def);
        schema_defs.extend(schema_def);
        store_inits.extend(store_init);
    }

    StoreRegistryTokens {
        memory_authority: memory_authority.to_owned(),
        journal_defs,
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    }
}

/// Render one store registry entry into data/index/schema cells plus registration.
fn store_registry_entry_tokens(
    store_ordinal: usize,
    store_path: &str,
    store: &Store,
    memory_namespace: &str,
    memory_authority: &str,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    match store.storage() {
        StoreStorage::Heap(config) => {
            heap_store_registry_entry_tokens(store_ordinal, store_path, *config)
        }
        StoreStorage::Journaled(config) => journaled_store_registry_entry_tokens(
            store_ordinal,
            store_path,
            store,
            memory_namespace,
            memory_authority,
            *config,
        ),
    }
}

fn store_cell_ident(store_ordinal: usize, role: &str) -> Ident {
    format_ident!("__ICYDB_STORE_{store_ordinal}_{role}")
}

fn stable_store_cell_tokens(
    cell_ident: &Ident,
    store_ty: TokenStream,
    stable_key: &str,
    memory_id: u8,
    memory_authority: &str,
) -> TokenStream {
    quote! {
        thread_local! {
            static #cell_ident: ::std::cell::RefCell<
                #store_ty
            > = ::std::cell::RefCell::new(
                #store_ty::init(
                    ::icydb::__macro::ic_memory_key!(
                        authority = #memory_authority,
                        key = #stable_key,
                        ty = #store_ty,
                        id = #memory_id,
                    )
                    .expect(concat!(
                        "ic-memory committed allocation unavailable: ",
                        #stable_key
                    ))
                )
            );
        }
    }
}

fn journaled_store_cell_tokens(
    cell_ident: &Ident,
    store_ty: TokenStream,
    stable_key: &str,
    memory_id: u8,
    memory_authority: &str,
) -> TokenStream {
    quote! {
        thread_local! {
            static #cell_ident: ::std::cell::RefCell<
                #store_ty
            > = ::std::cell::RefCell::new(
                #store_ty::init_journaled(
                    ::icydb::__macro::ic_memory_key!(
                        authority = #memory_authority,
                        key = #stable_key,
                        ty = #store_ty,
                        id = #memory_id,
                    )
                    .expect(concat!(
                        "ic-memory committed allocation unavailable: ",
                        #stable_key
                    ))
                )
            );
        }
    }
}

/// Render one volatile heap store registry entry into data/index/schema cells plus registration.
fn heap_store_registry_entry_tokens(
    store_ordinal: usize,
    store_path: &str,
    _heap: StoreHeapConfig,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    let data_cell_ident = store_cell_ident(store_ordinal, "DATA");
    let index_cell_ident = store_cell_ident(store_ordinal, "INDEX");
    let schema_cell_ident = store_cell_ident(store_ordinal, "SCHEMA");

    let data_def = quote! {
        thread_local! {
            static #data_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::DataStore
            > = const { ::std::cell::RefCell::new(
                ::icydb::__macro::DataStore::init_heap()
            ) };
        }
    };
    let index_def = quote! {
        thread_local! {
            static #index_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::IndexStore
            > = const { ::std::cell::RefCell::new(
                ::icydb::__macro::IndexStore::init_heap()
            ) };
        }
    };
    let schema_def = quote! {
        thread_local! {
            static #schema_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::SchemaStore
            > = const { ::std::cell::RefCell::new(
                ::icydb::__macro::SchemaStore::init_heap()
            ) };
        }
    };
    let store_init = quote! {
        reg.register_store(
            #store_path,
            &#data_cell_ident,
            &#index_cell_ident,
            &#schema_cell_ident,
            ::icydb::__macro::StoreAllocationIdentities::absent(),
            ::icydb::__macro::StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("store registration should succeed");
    };

    (quote!(), data_def, index_def, schema_def, store_init)
}

/// Render one journaled cached-stable store registry entry into canonical
/// stable data/index/schema cells, a journal-tail declaration, and registration.
fn journaled_store_registry_entry_tokens(
    store_ordinal: usize,
    store_path: &str,
    store: &Store,
    memory_namespace: &str,
    memory_authority: &str,
    journaled: StoreJournaledMemoryConfig,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    let data_cell_ident = store_cell_ident(store_ordinal, "DATA");
    let index_cell_ident = store_cell_ident(store_ordinal, "INDEX");
    let schema_cell_ident = store_cell_ident(store_ordinal, "SCHEMA");
    let data_allocation = store.stable_data_allocation(memory_namespace);
    let index_allocation = store.stable_index_allocation(memory_namespace);
    let schema_allocation = store.stable_schema_allocation(memory_namespace);
    let journal_allocation = store.journal_allocation(memory_namespace);
    let data_memory_id = journaled.data_memory_id();
    let index_memory_id = journaled.index_memory_id();
    let schema_memory_id = journaled.schema_memory_id();
    let journal_memory_id = journaled.journal_memory_id();
    let data_stable_key = data_allocation.stable_key();
    let index_stable_key = index_allocation.stable_key();
    let schema_stable_key = schema_allocation.stable_key();
    let journal_stable_key = journal_allocation.stable_key();

    let journal_cell_ident = store_cell_ident(store_ordinal, "JOURNAL");
    let journal_def = stable_store_cell_tokens(
        &journal_cell_ident,
        quote!(::icydb::__macro::JournalTailStore),
        journal_stable_key,
        journal_memory_id,
        memory_authority,
    );
    let data_def = journaled_store_cell_tokens(
        &data_cell_ident,
        quote!(::icydb::__macro::DataStore),
        data_stable_key,
        data_memory_id,
        memory_authority,
    );
    let index_def = journaled_store_cell_tokens(
        &index_cell_ident,
        quote!(::icydb::__macro::IndexStore),
        index_stable_key,
        index_memory_id,
        memory_authority,
    );
    let schema_def = journaled_store_cell_tokens(
        &schema_cell_ident,
        quote!(::icydb::__macro::SchemaStore),
        schema_stable_key,
        schema_memory_id,
        memory_authority,
    );
    let store_init = quote! {
        reg.register_journaled_store(
            #store_path,
            &#data_cell_ident,
            &#index_cell_ident,
            &#schema_cell_ident,
            &#journal_cell_ident,
            ::icydb::__macro::StoreAllocationIdentities::new_journaled(
                ::icydb::__macro::StoreAllocationIdentity::new(
                    #data_memory_id,
                    #data_stable_key,
                ),
                ::icydb::__macro::StoreAllocationIdentity::new(
                    #index_memory_id,
                    #index_stable_key,
                ),
                ::icydb::__macro::StoreAllocationIdentity::new(
                    #schema_memory_id,
                    #schema_stable_key,
                ),
                ::icydb::__macro::StoreAllocationIdentity::new(
                    #journal_memory_id,
                    #journal_stable_key,
                ),
            ),
            ::icydb::__macro::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("store registration should succeed");
    };

    (journal_def, data_def, index_def, schema_def, store_init)
}

/// Assemble the outer canister store wiring around the generated registry.
#[expect(
    clippy::too_many_lines,
    reason = "one generated block keeps the store registry, memory declarations, and startup wiring visibly co-located"
)]
fn store_wiring_tokens(
    store_registry: StoreRegistryTokens,
    frontend_surfaces: TokenStream,
    schema_bootstrap: TokenStream,
    memory: CanisterMemoryWiring<'_>,
) -> TokenStream {
    let StoreRegistryTokens {
        memory_authority,
        journal_defs,
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    } = store_registry;
    let CanisterMemoryWiring {
        memory_min,
        memory_max,
        commit_memory_id,
        commit_stable_key,
        integrity_progress_memory_id,
        integrity_progress_stable_key,
    } = memory;
    let store_registry_init = if store_inits.is_empty() {
        quote! {
            ::icydb::__macro::StoreRegistry::new()
        }
    } else {
        quote! {
            {
                let mut reg =
                    ::icydb::__macro::StoreRegistry::new();
                #store_inits
                reg
            }
        }
    };

    quote! {
        #[doc(hidden)]
        pub struct __IcydbGeneratedCanister;

        impl ::icydb::__macro::Path for __IcydbGeneratedCanister {
            const PATH: &'static str = "__icydb_generated_canister";
        }

        impl ::icydb::__macro::CanisterKind for __IcydbGeneratedCanister {
            const COMMIT_MEMORY_ID: u8 = #commit_memory_id;
            const COMMIT_STABLE_KEY: &'static str = #commit_stable_key;
            const INTEGRITY_PROGRESS_MEMORY_ID: u8 = #integrity_progress_memory_id;
            const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
                #integrity_progress_stable_key;
        }

        ::icydb::__macro::ic_memory_range!(
            authority = #memory_authority,
            start = #memory_min,
            end = #memory_max,
        );

        ::icydb::__macro::ic_memory_declaration!(
            authority = #memory_authority,
            key = #commit_stable_key,
            label = "CommitMarker",
            id = #commit_memory_id,
        );

        ::icydb::__macro::ic_memory_declaration!(
            authority = #memory_authority,
            key = #integrity_progress_stable_key,
            label = "IntegrityProgress",
            id = #integrity_progress_memory_id,
        );

        #journal_defs
        thread_local! {
            static MEMORY_BOOTSTRAP:
                ::std::cell::OnceCell<
                    ::std::result::Result<
                        (),
                        ::icydb::db::DatabaseBootstrapError
                    >
                > =
                    const { ::std::cell::OnceCell::new() };
        }

        fn ensure_memory_bootstrap() ->
            ::std::result::Result<(), ::icydb::db::DatabaseBootstrapError>
        {
            MEMORY_BOOTSTRAP.with(|bootstrap| {
                bootstrap
                    .get_or_init(|| {
                        ::icydb::__macro::ensure_default_memory_manager(
                            #memory_authority,
                        )
                    })
                    .clone()
            })
        }

        #data_defs
        #index_defs
        #schema_defs
        #frontend_surfaces
        #schema_bootstrap
        thread_local! {
            static STORE_REGISTRY:
                ::icydb::__macro::StoreRegistry =
                #store_registry_init;
        }

        #[doc(hidden)]
        pub fn core_db() -> ::std::result::Result<
            ::icydb::__macro::CoreDbSession<__IcydbGeneratedCanister>,
            ::icydb::Error,
        > {
            ensure_memory_bootstrap()?;

            ::icydb::__macro::CoreDbSession::<__IcydbGeneratedCanister>::__new_from_current_request(
                &STORE_REGISTRY,
            )
            .ok_or_else(::icydb::db::__request_execution_scope_required)
        }

        #[doc(hidden)]
        pub fn core_db_with_request_root(
            request_root: &::icydb::db::RequestExecutionRoot,
        ) -> ::std::result::Result<
            ::icydb::__macro::CoreDbSession<__IcydbGeneratedCanister>,
            ::icydb::db::DatabaseBootstrapError,
        > {
            ensure_memory_bootstrap()?;

            Ok(::icydb::__macro::CoreDbSession::<__IcydbGeneratedCanister>::new(
                &STORE_REGISTRY,
                request_root.__core(),
            ))
        }

        pub fn db() -> ::std::result::Result<
            ::icydb::db::DbSession<__IcydbGeneratedCanister>,
            ::icydb::Error,
        > {
            let session = ::icydb::db::DbSession::new(core_db()?);
            ensure_schema_application(&session)?;
            Ok(session)
        }

        #[doc(hidden)]
        pub fn db_with_request_root(
            request_root: &::icydb::db::RequestExecutionRoot,
        ) -> ::std::result::Result<
            ::icydb::db::DbSession<__IcydbGeneratedCanister>,
            ::icydb::Error,
        > {
            request_root.__ensure_compatible_with_current()?;
            let session = ::icydb::db::DbSession::new(core_db_with_request_root(request_root)?);
            ensure_schema_application(&session)?;
            Ok(session)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            StoreJournaledMemoryConfig::new(20, 21, 22, 23),
        )
    }

    fn actor_builder() -> ActorBuilder {
        ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", 0, 1, 2, 3, None),
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
            None,
        )
    }

    #[test]
    fn schema_application_cache_matches_the_stable_memory_target_scope() {
        let rendered = compact_tokens(schema_bootstrap_tokens(&actor_builder()));

        assert!(rendered.contains("::std::cell::OnceCell"));
        assert!(rendered.contains("SCHEMA_APPLICATION.with("));
        assert!(rendered.contains("__continue_startup_recovery"));
        assert!(rendered.contains("elseif!session.__continue_startup_recovery()?"));
        assert!(rendered.contains("ic_cdk_timers::set_timer"));
    }

    #[test]
    fn heap_store_wiring_uses_heap_initializers_and_absent_allocation_identity() {
        let store = heap_store();
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                0,
                "demo::schema::ScratchStore",
                &store,
                "demo",
                "icydb.demo",
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
        assert_eq!(rendered.matches("ic_memory_key").count(), 0);
        assert_eq!(
            rendered.matches("StoreAllocationIdentity :: new").count(),
            0
        );
        assert!(!rendered.contains("ensure_memory_bootstrap"));
    }

    #[test]
    fn journaled_store_wiring_declares_journal_memory_and_registers_four_role_allocation() {
        let store = journaled_store();
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                0,
                "demo::schema::JournaledStore",
                &store,
                "demo",
                "icydb.demo",
            );
        let rendered = quote! {
            #journal_def
            #data_def
            #index_def
            #schema_def
            #store_init
        }
        .to_string();

        assert_eq!(rendered.matches("ic_memory_key").count(), 4);
        assert_eq!(
            rendered
                .matches("ic-memory committed allocation unavailable:")
                .count(),
            4
        );
        assert_eq!(rendered.matches("authority = \"icydb.demo\"").count(), 4);
        assert_eq!(
            rendered.matches("StoreAllocationIdentity :: new").count(),
            4
        );
        assert!(rendered.contains("JournalTailStore :: init"));
        assert_eq!(rendered.matches("init_journaled").count(), 3);
        assert!(rendered.contains("register_journaled_store"));
        assert!(rendered.contains("StoreAllocationIdentities :: new_journaled"));
        assert!(rendered.contains("StoreRuntimeStorageCapabilities :: journaled"));
        for expected in ["id = 20u8", "id = 21u8", "id = 22u8", "id = 23u8"] {
            assert!(
                rendered.contains(expected),
                "journaled store wiring should render {expected}: {rendered}"
            );
        }
        assert!(rendered.contains("icydb.demo.memory_20.data.v1"));
        assert!(rendered.contains("icydb.demo.memory_21.index.v1"));
        assert!(rendered.contains("icydb.demo.memory_22.schema.v1"));
        assert!(rendered.contains("icydb.demo.memory_23.journal.v1"));
        assert!(!rendered.contains("init_heap"));
    }

    #[test]
    fn store_registry_wiring_is_lint_clean() {
        let mut store_inits = quote!();
        store_inits.extend(
            store_registry_entry_tokens(
                0,
                "demo::schema::ScratchStore",
                &heap_store(),
                "demo",
                "icydb.demo",
            )
            .4,
        );
        let registry = StoreRegistryTokens {
            memory_authority: "icydb.demo".to_owned(),
            journal_defs: quote!(),
            data_defs: quote!(),
            index_defs: quote!(),
            schema_defs: quote!(),
            store_inits,
        };

        let rendered = compact_tokens(store_wiring_tokens(
            registry,
            quote!(),
            quote!(),
            CanisterMemoryWiring {
                memory_min: 10,
                memory_max: 19,
                commit_memory_id: 18,
                commit_stable_key: "icydb.demo.commit.v1",
                integrity_progress_memory_id: 17,
                integrity_progress_stable_key: "icydb.demo.integrity.progress.v1",
            },
        ));

        assert!(!rendered.contains("allow(unused_mut)"));
        assert!(!rendered.contains("expect(clippy::let_and_return"));
        assert_eq!(rendered.matches("authority=\"icydb.demo\"").count(), 3);
        assert!(rendered.contains("key=\"icydb.demo.integrity.progress.v1\""));
        assert!(rendered.contains("label=\"IntegrityProgress\""));
        assert!(rendered.contains("id=17u8"));
        assert!(rendered.contains("Result<(),::icydb::db::DatabaseBootstrapError>"));
        assert!(
            rendered.contains("::icydb::__macro::ensure_default_memory_manager(\"icydb.demo\",)")
        );
        assert!(!rendered.contains("bootstrap_default_memory_manager()"));
        assert!(!rendered.contains("fn bootstrap_memory_manager()"));
        assert!(rendered.contains("ensure_memory_bootstrap()?"));
        assert!(rendered.contains("::std::cell::OnceCell"));
        assert!(rendered.contains("MEMORY_BOOTSTRAP.with("));
        assert!(rendered.contains(
            "CoreDbSession::<__IcydbGeneratedCanister>::__new_from_current_request(&STORE_REGISTRY,)"
        ));
        assert!(rendered.contains(
            "CoreDbSession::<__IcydbGeneratedCanister>::new(&STORE_REGISTRY,request_root.__core(),)"
        ));
        assert!(rendered.contains("pubfndb()->::std::result::Result<"));
        assert!(rendered.contains(
            "pubfndb_with_request_root(request_root:&::icydb::db::RequestExecutionRoot,)"
        ));
        assert!(rendered.contains("request_root.__ensure_compatible_with_current()?"));
        assert!(!rendered.contains("must_use"));
        assert_eq!(
            rendered
                .matches("::icydb::db::DatabaseBootstrapError")
                .count(),
            3,
        );
        assert!(!rendered.contains("Result<(),::std::string::String>"));
        assert!(!rendered.contains("panic!("));
    }
}
