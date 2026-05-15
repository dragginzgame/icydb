use crate::ActorBuilder;
use icydb_schema::node::Store;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_str;

/// Render the generated store/session wiring for one canister actor.
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();
    tokens.extend(stores(builder));
    tokens
}

/// Emit the generated store definitions, runtime hooks, and session accessors.
fn stores(builder: &ActorBuilder) -> TokenStream {
    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def().path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", canister.def().path()));
    let store_registry = store_registry_tokens(builder);
    let entity_runtime_hooks = entity_runtime_hooks(builder, &canister_path);
    let memory_min = canister.memory_min();
    let memory_max = canister.memory_max();

    store_wiring_tokens(
        &canister_path,
        store_registry,
        entity_runtime_hooks,
        memory_min,
        memory_max,
    )
}

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

///
/// AdminSqlTokens
///
/// Generated token bundle for the opted-in admin SQL helper surface. These
/// helpers stay non-exported; the public controller-gated methods are owned by
/// the runtime macro.
///

struct AdminSqlTokens {
    reset_statements: TokenStream,
    query_arms: TokenStream,
    ddl_arms: TokenStream,
    show_entities_dispatch: TokenStream,
}

fn store_registry_tokens(builder: &ActorBuilder) -> StoreRegistryTokens {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut schema_defs = quote!();
    let mut store_inits = quote!();

    for (store_path, store) in builder.get_stores() {
        let (data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(&store_path, &store);
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
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let data_cell_ident = format_ident!("{}_DATA", store.ident());
    let index_cell_ident = format_ident!("{}_INDEX", store.ident());
    let schema_cell_ident = format_ident!("{}_SCHEMA", store.ident());
    let data_memory_id = store.data_memory_id();
    let index_memory_id = store.index_memory_id();
    let schema_memory_id = store.schema_memory_id();

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
    let schema_def = quote! {
        ::icydb::__reexports::canic_memory::eager_static! {
            static #schema_cell_ident: ::std::cell::RefCell<
                ::icydb::__macro::SchemaStore
            > = ::std::cell::RefCell::new(
                ::icydb::__macro::SchemaStore::init(
                    ::icydb::__reexports::canic_memory::ic_memory!(
                        ::icydb::__macro::SchemaStore,
                        #schema_memory_id
                    )
                )
            );
        }
    };
    let store_init = quote! {
        reg.register_store(#store_path, &#data_cell_ident, &#index_cell_ident, &#schema_cell_ident)
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
) -> TokenStream {
    let StoreRegistryTokens {
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    } = store_registry;

    quote! {
        ::icydb::__reexports::canic_memory::eager_init!({
            ::icydb::__reexports::canic_memory::ic_memory_range!(#memory_min, #memory_max);
        });

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
                let mut reg =
                    ::icydb::__macro::StoreRegistry::new();
                #store_inits
                reg
            };
        }

        fn ensure_memory_bootstrap() {
            use ::icydb::__reexports::canic_memory::api::MemoryApi;

            MemoryApi::bootstrap_pending()
                .expect("generated canister memory registry pending flush should succeed");
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

/// Emit the entity runtime hook table for all entities bound to this canister.
fn entity_runtime_hooks(builder: &ActorBuilder, canister_path: &syn::Path) -> TokenStream {
    let mut hook_inits = quote!();
    let mut admin_sql = AdminSqlTokens::empty();
    let entities = builder.get_entities();

    for (entity_path, _) in entities {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        hook_inits.extend(quote! {
            ::icydb::__macro::EntityRuntimeHooks::<#canister_path>::for_entity::<#entity_ty>(),
        });
        admin_sql.push_entity(&entity_ty);
    }

    quote! {
        static ENTITY_RUNTIME_HOOKS: &[
            ::icydb::__macro::EntityRuntimeHooks<#canister_path>
        ] = &[
            #hook_inits
        ];

        #admin_sql
    }
}

impl AdminSqlTokens {
    fn empty() -> Self {
        Self {
            reset_statements: quote!(),
            query_arms: quote!(),
            ddl_arms: quote!(),
            show_entities_dispatch: empty_admin_sql_query_dispatch(),
        }
    }

    fn push_entity(&mut self, entity_ty: &syn::Path) {
        if self.show_entities_dispatch.is_empty() {
            self.show_entities_dispatch = show_entities_dispatch_for(entity_ty);
        }
        self.reset_statements
            .extend(admin_sql_reset_statement(entity_ty));
        self.query_arms
            .extend(admin_sql_query_dispatch_arm(entity_ty));
        self.ddl_arms.extend(admin_sql_ddl_dispatch_arm(entity_ty));
    }
}

impl quote::ToTokens for AdminSqlTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let reset_statements = &self.reset_statements;
        let query_arms = &self.query_arms;
        let ddl_arms = &self.ddl_arms;
        let show_entities_dispatch = &self.show_entities_dispatch;

        tokens.extend(quote! {
            #[cfg(all(feature = "sql", feature = "diagnostics"))]
            #[allow(dead_code)]
            fn icydb_admin_sql_query_dispatch(
                sql: &str,
            ) -> Result<
                (
                    ::icydb::db::sql::SqlQueryResult,
                    ::icydb::db::SqlQueryExecutionAttribution,
                ),
                ::icydb::Error,
            > {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #query_arms
                    None => {
                        #show_entities_dispatch
                    }
                    Some(entity) => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        format!(
                            "admin SQL query target entity '{entity}' is not available on this canister"
                        ),
                    )),
                }
            }

            #[cfg(feature = "sql")]
            #[allow(dead_code)]
            fn icydb_admin_sql_reset_all_tables() -> Result<(), ::icydb::Error> {
                #reset_statements

                Ok(())
            }

            #[cfg(feature = "sql")]
            #[allow(dead_code)]
            fn icydb_admin_sql_ddl_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #ddl_arms
                    None => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        "admin SQL DDL requires one target entity",
                    )),
                    Some(entity) => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        format!(
                            "admin SQL DDL target entity '{entity}' is not available on this canister"
                        ),
                    )),
                }
            }
        });
    }
}

fn admin_sql_reset_statement(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        db().delete::<#entity_ty>().execute()?;
    }
}

fn admin_sql_query_dispatch_arm(entity_ty: &syn::Path) -> TokenStream {
    quote! {
            Some(entity)
                if ::icydb::__macro::identifiers_tail_match(
                    entity,
                    <#entity_ty as ::icydb::traits::Path>::PATH
                ) || ::icydb::__macro::identifiers_tail_match(
                    entity,
                    <#entity_ty as ::icydb::traits::EntitySchema>::NAME
                ) =>
            {
                return db()
                    .execute_sql_query_with_attribution::<#entity_ty>(sql)
                    .map_err(::icydb::Error::from);
            }
    }
}

fn admin_sql_ddl_dispatch_arm(entity_ty: &syn::Path) -> TokenStream {
    quote! {
            Some(entity)
                if ::icydb::__macro::identifiers_tail_match(
                    entity,
                    <#entity_ty as ::icydb::traits::Path>::PATH
                ) || ::icydb::__macro::identifiers_tail_match(
                    entity,
                    <#entity_ty as ::icydb::traits::EntitySchema>::NAME
                ) =>
            {
                return db()
                    .execute_sql_ddl::<#entity_ty>(sql)
                    .map_err(::icydb::Error::from);
            }
    }
}

fn empty_admin_sql_query_dispatch() -> TokenStream {
    quote! {
        return Err(::icydb::Error::new(
            ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
            ::icydb::ErrorOrigin::Interface,
            "admin SQL query requires at least one canister entity",
        ));
    }
}

fn show_entities_dispatch_for(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        return db()
            .execute_sql_query_with_attribution::<#entity_ty>(sql)
            .map_err(::icydb::Error::from);
    }
}
