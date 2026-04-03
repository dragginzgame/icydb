use crate::ActorBuilder;
use icydb_schema::node::Store;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_str;

#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();
    tokens.extend(stores(builder));
    tokens.extend(sql_dispatch(builder));
    tokens
}

fn stores(builder: &ActorBuilder) -> TokenStream {
    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def().path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", canister.def().path()));
    let (data_defs, index_defs, store_inits) = store_registry_tokens(builder);
    let entity_runtime_hooks = entity_runtime_hooks(builder, &canister_path);
    let memory_min = canister.memory_min();
    let memory_max = canister.memory_max();

    store_wiring_tokens(
        &canister_path,
        data_defs,
        index_defs,
        store_inits,
        entity_runtime_hooks,
        memory_min,
        memory_max,
    )
}

fn store_registry_tokens(builder: &ActorBuilder) -> (TokenStream, TokenStream, TokenStream) {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut store_inits = quote!();

    for (store_path, store) in builder.get_stores() {
        let (data_def, index_def, store_init) = store_registry_entry_tokens(&store_path, &store);
        data_defs.extend(data_def);
        index_defs.extend(index_def);
        store_inits.extend(store_init);
    }

    (data_defs, index_defs, store_inits)
}

fn store_registry_entry_tokens(
    store_path: &str,
    store: &Store,
) -> (TokenStream, TokenStream, TokenStream) {
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let data_memory_id = store.data_memory_id();
    let index_memory_id = store.index_memory_id();

    let data_def = quote! {
        ::icydb::__reexports::canic_memory::eager_static! {
            static #data_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::DataStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::DataStore::init(
                    ::icydb::__reexports::canic_memory::ic_memory!(
                        ::icydb::__macro::DataStore,
                        #data_memory_id
                    )
                )
            );
        }
    };
    let index_def = quote! {
        ::icydb::__reexports::canic_memory::eager_static! {
            static #index_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::IndexStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::IndexStore::init(
                    ::icydb::__reexports::canic_memory::ic_memory!(
                        ::icydb::__macro::IndexStore,
                        #index_memory_id
                    )
                )
            );
        }
    };
    let store_init = quote! {
        reg.register_store(#store_path, &#data_cell_ident, &#index_cell_ident)
            .expect("store registration should succeed");
    };

    (data_def, index_def, store_init)
}

fn store_wiring_tokens(
    canister_path: &syn::Path,
    data_defs: TokenStream,
    index_defs: TokenStream,
    store_inits: TokenStream,
    entity_runtime_hooks: TokenStream,
    memory_min: u8,
    memory_max: u8,
) -> TokenStream {
    quote! {
        #data_defs
        #index_defs
        #entity_runtime_hooks
        thread_local! {
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static STORE_REGISTRY:
                ::icydb::__macro::StoreRegistry =
            {
                let mut reg =
                    ::icydb::__macro::StoreRegistry::new();
                #store_inits
                reg
            };
        }

        // reserve the ic memory range
        ::icydb::__reexports::canic_memory::eager_init!({
            ::icydb::__reexports::canic_memory::ic_memory_range!(
                #memory_min,
                #memory_max
            );
        });

        fn ensure_memory_bootstrap() {
            use ::icydb::__reexports::canic_memory::runtime::{
                init_eager_tls, registry::MemoryRegistryRuntime, run_registered_eager_init,
            };

            if MemoryRegistryRuntime::is_initialized() {
                return;
            }

            init_eager_tls();
            run_registered_eager_init();
            MemoryRegistryRuntime::init(None)
                .expect("generated canister memory registry init should succeed");
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

fn entity_runtime_hooks(builder: &ActorBuilder, canister_path: &syn::Path) -> TokenStream {
    let mut hook_inits = quote!();
    let entities = builder.get_entities();

    for (entity_path, _) in entities {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        hook_inits.extend(quote! {
            ::icydb::__macro::EntityRuntimeHooks::<#canister_path>::for_entity::<#entity_ty>(),
        });
    }

    quote! {
        static ENTITY_RUNTIME_HOOKS: &[
            ::icydb::__macro::EntityRuntimeHooks<#canister_path>
        ] = &[
            #hook_inits
        ];
    }
}

fn sql_dispatch(builder: &ActorBuilder) -> TokenStream {
    let entities = builder.get_entities();
    let mut descriptor_entries = quote!();

    for (entity_path, _entity) in entities {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        let descriptor = sql_descriptor_entry_tokens(&entity_ty);
        descriptor_entries.extend(descriptor);
    }

    sql_dispatch_module_tokens(descriptor_entries)
}

fn sql_dispatch_module_tokens(descriptor_entries: TokenStream) -> TokenStream {
    let imports = sql_dispatch_import_tokens();
    let query_surface = sql_dispatch_query_surface_tokens();

    quote! {
        #[cfg(feature = "sql")]
        pub mod sql_dispatch {
            #imports

            static SQL_ENTITY_AUTHORITIES: &[::icydb::db::EntityAuthority] = &[
                #descriptor_entries
            ];

            #query_surface
        }
    }
}

fn sql_dispatch_import_tokens() -> TokenStream {
    quote! {
        use super::db;

        use ::icydb::{
            Error,
            __macro::execute_generated_sql_query,
            db::sql::SqlQueryResult,
        };
    }
}

fn sql_dispatch_query_surface_tokens() -> TokenStream {
    let api = sql_dispatch_query_api_tokens();
    let support = sql_dispatch_query_support_tokens();

    quote! {
        #api
        #support
    }
}

fn sql_dispatch_query_api_tokens() -> TokenStream {
    quote! {
        #[must_use]
        pub fn entities() -> Vec<String> {
            generated_sql_entities()
        }

        #[doc(hidden)]
        #[must_use]
        pub const fn authorities() -> &'static [::icydb::db::EntityAuthority] {
            SQL_ENTITY_AUTHORITIES
        }

        pub fn query(sql: &str) -> Result<SqlQueryResult, Error> {
            execute_generated_sql_query(&db(), sql, SQL_ENTITY_AUTHORITIES)
        }
    }
}

fn sql_dispatch_query_support_tokens() -> TokenStream {
    quote! {
        fn generated_sql_entities() -> Vec<String> {
            let mut entities = Vec::with_capacity(SQL_ENTITY_AUTHORITIES.len());

            for authority in SQL_ENTITY_AUTHORITIES {
                entities.push(authority.model().name().to_string());
            }

            entities
        }
    }
}

fn sql_descriptor_entry_tokens(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        ::icydb::db::EntityAuthority::new(
            <#entity_ty as ::icydb::traits::EntitySchema>::MODEL,
            <#entity_ty as ::icydb::traits::EntityKind>::ENTITY_TAG,
            <<#entity_ty as ::icydb::traits::EntityPlacement>::Store as ::icydb::traits::Path>::PATH,
        ),
    }
}
