use crate::ActorBuilder;
use icydb_schema::node::{Store, StoreHeapConfig, StoreStableMemoryConfig, StoreStorage};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

///
/// StoreRegistryTokens
///
/// Generated token bundle for all store-memory definitions and registration
/// statements emitted for one actor. It keeps store wiring helpers below the
/// argument limit while preserving the generated-code phase boundary.
///
struct StoreRegistryTokens {
    data_defs: TokenStream,
    index_defs: TokenStream,
    schema_defs: TokenStream,
    store_inits: TokenStream,
}

/// Emit the generated store definitions, runtime hooks, and session accessors.
pub(super) fn generate_store_wiring(
    builder: &ActorBuilder,
    canister_path: &syn::Path,
    entity_runtime_hooks: TokenStream,
) -> TokenStream {
    let canister = &builder.canister;
    let store_registry = store_registry_tokens(builder, canister.memory_namespace());
    let memory_min = canister.memory_min();
    let memory_max = canister.memory_max();
    let commit_memory_id = canister.commit_memory_id();
    let commit_stable_key = canister.commit_stable_key();

    store_wiring_tokens(
        canister_path,
        store_registry,
        entity_runtime_hooks,
        memory_min,
        memory_max,
        commit_memory_id,
        &commit_stable_key,
    )
}

fn store_registry_tokens(builder: &ActorBuilder, memory_namespace: &str) -> StoreRegistryTokens {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut schema_defs = quote!();
    let mut store_inits = quote!();

    for (store_path, store) in builder.get_stores() {
        let (data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(&store_path, &store, memory_namespace);
        data_defs.extend(data_def);
        index_defs.extend(index_def);
        schema_defs.extend(schema_def);
        store_inits.extend(store_init);
    }

    StoreRegistryTokens {
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
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    match store.storage() {
        StoreStorage::Stable(config) => {
            stable_store_registry_entry_tokens(store_path, store, memory_namespace, *config)
        }
        StoreStorage::Heap(config) => heap_store_registry_entry_tokens(store_path, store, *config),
    }
}

/// Render one stable store registry entry into data/index/schema cells plus registration.
fn stable_store_registry_entry_tokens(
    store_path: &str,
    store: &Store,
    memory_namespace: &str,
    stable: StoreStableMemoryConfig,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let schema_cell_ident = format_ident!("{}_SCHEMA", store.ident());
    let data_allocation = store.stable_data_allocation(memory_namespace);
    let index_allocation = store.stable_index_allocation(memory_namespace);
    let schema_allocation = store.stable_schema_allocation(memory_namespace);
    let data_memory_id = stable.data_memory_id();
    let index_memory_id = stable.index_memory_id();
    let schema_memory_id = stable.schema_memory_id();
    let data_stable_key = data_allocation.stable_key();
    let index_stable_key = index_allocation.stable_key();
    let schema_stable_key = schema_allocation.stable_key();

    let data_def = quote! {
        thread_local! {
            static #data_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::DataStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::DataStore::init(
                    {
                        ensure_memory_bootstrap();
                        ::icydb::__macro::ic_memory_key!(
                            key = #data_stable_key,
                            ty = ::icydb::__macro::DataStore,
                            id = #data_memory_id,
                        )
                    }
                )
            );
        }
    };
    let index_def = quote! {
        thread_local! {
            static #index_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::IndexStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::IndexStore::init(
                    {
                        ensure_memory_bootstrap();
                        ::icydb::__macro::ic_memory_key!(
                            key = #index_stable_key,
                            ty = ::icydb::__macro::IndexStore,
                            id = #index_memory_id,
                        )
                    }
                )
            );
        }
    };
    let schema_def = quote! {
        thread_local! {
            static #schema_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::SchemaStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::SchemaStore::init(
                    {
                        ensure_memory_bootstrap();
                        ::icydb::__macro::ic_memory_key!(
                            key = #schema_stable_key,
                            ty = ::icydb::__macro::SchemaStore,
                            id = #schema_memory_id,
                        )
                    }
                )
            );
        }
    };
    let store_init = quote! {
        reg.register_store(
            #store_path,
            &#data_cell_ident,
            &#index_cell_ident,
            &#schema_cell_ident,
            ::icydb::__macro::StoreAllocationIdentities::new(
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
            ),
            ::icydb::__macro::StoreRuntimeStorageCapabilities::stable(),
        )
        .expect("store registration should succeed");
    };

    (data_def, index_def, schema_def, store_init)
}

/// Render one volatile heap store registry entry into data/index/schema cells plus registration.
fn heap_store_registry_entry_tokens(
    store_path: &str,
    store: &Store,
    _heap: StoreHeapConfig,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let schema_cell_ident = format_ident!("{}_SCHEMA", store.ident());

    let data_def = quote! {
        thread_local! {
            static #data_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::DataStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::DataStore::init_heap()
            );
        }
    };
    let index_def = quote! {
        thread_local! {
            static #index_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::IndexStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::IndexStore::init_heap()
            );
        }
    };
    let schema_def = quote! {
        thread_local! {
            static #schema_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::SchemaStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::SchemaStore::init_heap()
            );
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

    (data_def, index_def, schema_def, store_init)
}

/// Assemble the outer canister store wiring around the generated registry.
fn store_wiring_tokens(
    canister_path: &syn::Path,
    store_registry: StoreRegistryTokens,
    entity_runtime_hooks: TokenStream,
    memory_min: u8,
    memory_max: u8,
    commit_memory_id: u8,
    commit_stable_key: &str,
) -> TokenStream {
    let StoreRegistryTokens {
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    } = store_registry;

    quote! {
        ::icydb::__macro::ic_memory_range!(
            start = #memory_min,
            end = #memory_max,
        );

        ::icydb::__macro::ic_memory_declaration!(
            key = #commit_stable_key,
            label = "CommitMarker",
            id = #commit_memory_id,
        );

        fn ensure_memory_bootstrap() {
            static MEMORY_BOOTSTRAP:
                ::std::sync::OnceLock<::std::result::Result<(), ::std::string::String>> =
                    ::std::sync::OnceLock::new();

            let result = MEMORY_BOOTSTRAP.get_or_init(|| {
                ::icydb::__macro::bootstrap_default_memory_manager()
                    .map_err(|err| err.to_string())?;

                Ok(())
            });

            if let Err(err) = result {
                panic!("generated canister memory bootstrap should succeed: {err}");
            }
        }

        #data_defs
        #index_defs
        #schema_defs
        #entity_runtime_hooks
        thread_local! {
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static STORE_REGISTRY:
                ::icydb::__macro::StoreRegistry =
            {
                ensure_memory_bootstrap();
                let mut reg =
                    ::icydb::__macro::StoreRegistry::new();
                #store_inits
                reg
            };
        }

        #[doc(hidden)]
        #[must_use]
        pub fn core_db() -> ::icydb::__macro::CoreDbSession<#canister_path> {
            ensure_memory_bootstrap();

            ::icydb::__macro::CoreDbSession::<#canister_path>::new_with_hooks(
                &STORE_REGISTRY,
                ENTITY_RUNTIME_HOOKS
            )
        }

        #[must_use]
        pub fn db() -> ::icydb::db::DbSession<#canister_path> {
            ::icydb::db::DbSession::new(core_db())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use icydb_schema::node::Def;

    fn stable_store() -> Store {
        Store::new_stable(
            Def::new("demo::schema", "DemoStore"),
            "DEMO_STORE",
            "demo",
            "demo::schema::DemoCanister",
            StoreStableMemoryConfig::new(10, 11, 12),
        )
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

    #[test]
    fn stable_store_wiring_uses_ic_memory_key_for_each_store_role() {
        let store = stable_store();
        let (data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens("demo::schema::DemoStore", &store, "demo");
        let rendered = quote! {
            #data_def
            #index_def
            #schema_def
            #store_init
        }
        .to_string();

        assert_eq!(rendered.matches("ic_memory_key").count(), 3);
        assert_eq!(
            rendered.matches("StoreAllocationIdentity :: new").count(),
            3
        );
        assert!(rendered.contains("StoreRuntimeStorageCapabilities :: stable"));
        for expected in ["id = 10u8", "id = 11u8", "id = 12u8"] {
            assert!(
                rendered.contains(expected),
                "stable store wiring should render {expected}: {rendered}"
            );
        }
        assert!(rendered.contains("icydb.demo.demo.data.v1"));
        assert!(rendered.contains("icydb.demo.demo.index.v1"));
        assert!(rendered.contains("icydb.demo.demo.schema.v1"));
        assert!(!rendered.contains("heap"));
    }

    #[test]
    fn heap_store_wiring_uses_heap_initializers_and_absent_allocation_identity() {
        let store = heap_store();
        let (data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens("demo::schema::ScratchStore", &store, "demo");
        let rendered = quote! {
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
}
