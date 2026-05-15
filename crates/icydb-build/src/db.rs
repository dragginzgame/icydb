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
    readonly_enabled: bool,
    ddl_enabled: bool,
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
    let mut admin_sql = builder.options.sql_enabled().then(|| {
        AdminSqlTokens::empty(
            builder.options.sql_readonly_enabled(),
            builder.options.sql_ddl_enabled(),
        )
    });
    let entities = builder.get_entities();

    for (entity_path, _) in entities {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        hook_inits.extend(quote! {
            ::icydb::__macro::EntityRuntimeHooks::<#canister_path>::for_entity::<#entity_ty>(),
        });
        if let Some(admin_sql) = admin_sql.as_mut() {
            admin_sql.push_entity(&entity_ty);
        }
    }
    let admin_sql = admin_sql.map_or_else(TokenStream::new, |admin_sql| quote!(#admin_sql));

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
    fn empty(readonly_enabled: bool, ddl_enabled: bool) -> Self {
        Self {
            readonly_enabled,
            ddl_enabled,
            reset_statements: quote!(),
            query_arms: quote!(),
            ddl_arms: quote!(),
            show_entities_dispatch: quote!(),
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
        let controller_guard = admin_sql_controller_guard();
        let perf_result = self.readonly_enabled.then(admin_sql_perf_result);
        let readonly_dispatch = self
            .readonly_enabled
            .then(|| self.readonly_dispatch_tokens());
        let ddl_dispatch = self.ddl_enabled.then(|| self.ddl_dispatch_tokens());
        let endpoints = admin_sql_endpoint_exports(self.readonly_enabled, self.ddl_enabled);
        let reset_helper = self.ddl_enabled.then(|| self.reset_helper_tokens());

        tokens.extend(quote! {
            #controller_guard
            #perf_result
            #readonly_dispatch
            #reset_helper
            #ddl_dispatch
            #endpoints
        });
    }
}

impl AdminSqlTokens {
    fn readonly_dispatch_tokens(&self) -> TokenStream {
        let query_arms = &self.query_arms;
        let show_entities_dispatch = if self.show_entities_dispatch.is_empty() {
            empty_admin_sql_query_dispatch()
        } else {
            self.show_entities_dispatch.clone()
        };

        quote! {
            #[cfg(feature = "sql")]
            #[allow(dead_code)]
            fn icydb_admin_sql_query_dispatch(
                sql: &str,
            ) -> Result<
                (
                    ::icydb::db::sql::SqlQueryResult,
                    ::icydb::db::AdminSqlQueryAttribution,
                ),
                ::icydb::Error,
            > {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #query_arms
                    None => #show_entities_dispatch,
                    Some(entity) => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        format!(
                            "admin SQL query target entity '{entity}' is not available on this canister"
                        ),
                    )),
                }
            }
        }
    }

    fn reset_helper_tokens(&self) -> TokenStream {
        let reset_statements = &self.reset_statements;

        quote! {
            #[cfg(feature = "sql")]
            #[allow(dead_code)]
            #[allow(
                clippy::unnecessary_wraps,
                reason = "zero-entity canisters still need the macro-owned reset helper to share the fallible reset signature"
            )]
            #[allow(
                clippy::missing_const_for_fn,
                reason = "the same generated reset helper is non-const when a canister owns entities"
            )]
            fn icydb_admin_sql_reset_all_tables() -> Result<(), ::icydb::Error> {
                #reset_statements

                Ok(())
            }
        }
    }

    fn ddl_dispatch_tokens(&self) -> TokenStream {
        let ddl_arms = &self.ddl_arms;

        quote! {

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
        }
    }
}

fn admin_sql_controller_guard() -> TokenStream {
    quote! {
        #[cfg(feature = "sql")]
        fn icydb_admin_sql_require_controller(action: &str) -> Result<(), ::icydb::Error> {
            let caller = ::icydb::__reexports::canic_cdk::api::msg_caller();
            if !::icydb::__reexports::canic_cdk::api::is_controller(&caller) {
                return Err(::icydb::Error::new(
                    ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                    ::icydb::ErrorOrigin::Interface,
                    format!("admin SQL {action} requires a controller caller"),
                ));
            }

            Ok(())
        }
    }
}

fn admin_sql_perf_result() -> TokenStream {
    quote! {
        #[cfg(feature = "sql")]
        #[derive(::icydb::__reexports::candid::CandidType, Clone, Debug, Eq, PartialEq)]
        struct IcydbAdminSqlQueryPerfResult {
            result: ::icydb::db::sql::SqlQueryResult,
            instructions: u64,
            planner_instructions: u64,
            store_instructions: u64,
            executor_instructions: u64,
            pure_covering_decode_instructions: u64,
            pure_covering_row_assembly_instructions: u64,
            decode_instructions: u64,
            compiler_instructions: u64,
        }

        #[cfg(feature = "sql")]
        impl IcydbAdminSqlQueryPerfResult {
            fn from_attribution(
                result: ::icydb::db::sql::SqlQueryResult,
                attribution: ::icydb::db::AdminSqlQueryAttribution,
            ) -> Self {
                Self {
                    result,
                    instructions: attribution.total_local_instructions,
                    planner_instructions: attribution.execution.planner_local_instructions,
                    store_instructions: attribution.execution.store_local_instructions,
                    executor_instructions: attribution.execution.executor_local_instructions,
                    pure_covering_decode_instructions: attribution
                        .pure_covering
                        .map_or(0, |pure_covering| pure_covering.decode_local_instructions),
                    pure_covering_row_assembly_instructions: attribution
                        .pure_covering
                        .map_or(0, |pure_covering| {
                            pure_covering.row_assembly_local_instructions
                        }),
                    decode_instructions: attribution.response_decode_local_instructions,
                    compiler_instructions: attribution.compile_local_instructions,
                }
            }
        }
    }
}

fn admin_sql_endpoint_exports(readonly_enabled: bool, ddl_enabled: bool) -> TokenStream {
    let query_endpoint = readonly_enabled.then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::query]
        fn icydb_admin_sql_query(
            sql: String,
        ) -> Result<IcydbAdminSqlQueryPerfResult, ::icydb::Error> {
            icydb_admin_sql_require_controller("query")?;

            let (result, attribution) = icydb_admin_sql_query_dispatch(sql.as_str())?;

            Ok(IcydbAdminSqlQueryPerfResult::from_attribution(
                result,
                attribution,
            ))
        }
        }
    });

    let ddl_endpoints = ddl_enabled.then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn ddl(sql: String) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            icydb_admin_sql_require_controller("DDL")?;

            icydb_admin_sql_ddl_dispatch(sql.as_str())
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn fixtures_reset() -> Result<(), ::icydb::Error> {
            icydb_admin_sql_require_controller("lifecycle reset")?;

            icydb_admin_sql_reset_all_tables()
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::canic_cdk::update]
        fn fixtures_load_default() -> Result<(), ::icydb::Error> {
            icydb_admin_sql_require_controller("lifecycle load_default")?;
            let hook: fn() -> Result<(), ::icydb::Error> = crate::icydb_admin_sql_load_default;

            icydb_admin_sql_reset_all_tables()?;
            hook()
        }
        }
    });

    quote! {
        #query_endpoint
        #ddl_endpoints
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
                db().execute_admin_sql_query_with_attribution::<#entity_ty>(sql)
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
                db().execute_sql_ddl::<#entity_ty>(sql)
            }
    }
}

fn empty_admin_sql_query_dispatch() -> TokenStream {
    quote! {
        Err(::icydb::Error::new(
            ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
            ::icydb::ErrorOrigin::Interface,
            "admin SQL query requires at least one canister entity",
        ))
    }
}

fn show_entities_dispatch_for(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        db().execute_admin_sql_query_with_attribution::<#entity_ty>(sql)
    }
}
