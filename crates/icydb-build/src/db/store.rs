//! Module: db::store
//! Responsibility: generated store memory declarations and session accessors.
//! Does not own: store runtime behavior, memory manager implementation, or schema authority.
//! Boundary: translates validated store metadata into actor-local storage wiring tokens.

use crate::ActorBuilder;
use icydb_schema::node::{Store, StoreHeapConfig, StoreJournaledMemoryConfig, StoreStorage};
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

/// Emit the generated store definitions, runtime hooks, and session accessors.
pub(super) fn generate_store_wiring(
    builder: &ActorBuilder,
    canister_path: &syn::Path,
    entity_runtime_hooks: TokenStream,
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

    store_wiring_tokens(
        canister_path,
        store_registry,
        entity_runtime_hooks,
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

    for (store_path, store) in builder.get_stores() {
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(&store_path, &store, memory_namespace, memory_authority);
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
        StoreStorage::Heap(config) => heap_store_registry_entry_tokens(store_path, store, *config),
        StoreStorage::Journaled(config) => journaled_store_registry_entry_tokens(
            store_path,
            store,
            memory_namespace,
            memory_authority,
            *config,
        ),
    }
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
                )
            );
        }
    }
}

/// Render one volatile heap store registry entry into data/index/schema cells plus registration.
fn heap_store_registry_entry_tokens(
    store_path: &str,
    store: &Store,
    _heap: StoreHeapConfig,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let schema_cell_ident = format_ident!("{}_SCHEMA", store.ident());

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
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let schema_cell_ident = format_ident!("{}_SCHEMA", store.ident());
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

    let journal_cell_ident = format_ident!("{}_JOURNAL", store.ident());
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
fn store_wiring_tokens(
    canister_path: &syn::Path,
    store_registry: StoreRegistryTokens,
    entity_runtime_hooks: TokenStream,
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
        fn ensure_memory_bootstrap() ->
            ::std::result::Result<(), ::icydb::db::DatabaseBootstrapError>
        {
            static MEMORY_BOOTSTRAP:
                ::std::sync::OnceLock<
                    ::std::result::Result<(), ::icydb::db::DatabaseBootstrapError>
                > =
                    ::std::sync::OnceLock::new();

            MEMORY_BOOTSTRAP.get_or_init(|| {
                ::icydb::__macro::bootstrap_default_memory_manager()
                    .map(|_allocations| ())
                    .map_err(::icydb::db::DatabaseBootstrapError::from)
            }).clone()
        }

        #data_defs
        #index_defs
        #schema_defs
        #entity_runtime_hooks
        thread_local! {
            static STORE_REGISTRY:
                ::icydb::__macro::StoreRegistry =
                #store_registry_init;
        }

        #[doc(hidden)]
        pub fn core_db() -> ::std::result::Result<
            ::icydb::__macro::CoreDbSession<#canister_path>,
            ::icydb::db::DatabaseBootstrapError,
        > {
            ensure_memory_bootstrap()?;

            Ok(::icydb::__macro::CoreDbSession::<#canister_path>::new_with_hooks(
                &STORE_REGISTRY,
                ENTITY_RUNTIME_HOOKS
            ))
        }

        pub fn db() -> ::std::result::Result<
            ::icydb::db::DbSession<#canister_path>,
            ::icydb::db::DatabaseBootstrapError,
        > {
            Ok(::icydb::db::DbSession::new(core_db()?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_schema::node::Def;

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
            "SCRATCH_STORE",
            "scratch",
            "demo::schema::DemoCanister",
            StoreHeapConfig::new(),
        )
    }

    fn journaled_store() -> Store {
        Store::new_journaled(
            Def::new("demo::schema", "JournaledStore"),
            "JOURNALED_STORE",
            "journaled",
            "demo::schema::DemoCanister",
            StoreJournaledMemoryConfig::new(20, 21, 22, 23),
        )
    }

    #[test]
    fn heap_store_wiring_uses_heap_initializers_and_absent_allocation_identity() {
        let store = heap_store();
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens("demo::schema::ScratchStore", &store, "demo", "icydb.demo");
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
        assert!(rendered.contains("icydb.demo.journaled.data.v1"));
        assert!(rendered.contains("icydb.demo.journaled.index.v1"));
        assert!(rendered.contains("icydb.demo.journaled.schema.v1"));
        assert!(rendered.contains("icydb.demo.journaled.journal.v1"));
        assert!(!rendered.contains("init_heap"));
    }

    #[test]
    fn store_registry_wiring_is_lint_clean() {
        let canister_path: syn::Path = syn::parse_quote!(demo::schema::DemoCanister);
        let mut store_inits = quote!();
        store_inits.extend(
            store_registry_entry_tokens(
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
            &canister_path,
            registry,
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
        assert!(rendered.contains("map_err(::icydb::db::DatabaseBootstrapError::from)"));
        assert!(rendered.contains("ensure_memory_bootstrap()?"));
        assert!(rendered.contains("pubfndb()->::std::result::Result<"));
        assert!(!rendered.contains("must_use"));
        assert!(
            rendered
                .matches("::icydb::db::DatabaseBootstrapError")
                .count()
                >= 4
        );
        assert!(!rendered.contains("Result<(),::std::string::String>"));
        assert!(!rendered.contains("panic!("));
    }
}
