use proc_macro2::TokenStream;
use quote::quote;

///
/// SqlSurfaceTokens
///
/// Generated token bundle for the opted-in IcyDB SQL helper surface. These
/// helpers stay non-exported; the public controller-gated methods are owned by
/// the runtime macro.
///
pub(super) struct SqlSurfaceTokens {
    readonly_enabled: bool,
    ddl_enabled: bool,
    fixtures_enabled: bool,
    reset_statements: TokenStream,
    query_arms: TokenStream,
    ddl_arms: TokenStream,
    show_entities_dispatch: TokenStream,
}

impl SqlSurfaceTokens {
    pub(super) fn empty(readonly_enabled: bool, ddl_enabled: bool, fixtures_enabled: bool) -> Self {
        Self {
            readonly_enabled,
            ddl_enabled,
            fixtures_enabled,
            reset_statements: quote!(),
            query_arms: quote!(),
            ddl_arms: quote!(),
            show_entities_dispatch: quote!(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_ty: &syn::Path) {
        if self.show_entities_dispatch.is_empty() {
            self.show_entities_dispatch = show_entities_dispatch_for(entity_ty);
        }
        self.reset_statements
            .extend(sql_surface_reset_statement(entity_ty));
        self.query_arms
            .extend(sql_surface_query_dispatch_arm(entity_ty));
        self.ddl_arms
            .extend(sql_surface_ddl_dispatch_arm(entity_ty));
    }

    fn readonly_dispatch_tokens(&self) -> TokenStream {
        let query_arms = &self.query_arms;
        let show_entities_dispatch = if self.show_entities_dispatch.is_empty() {
            empty_sql_surface_query_dispatch()
        } else {
            self.show_entities_dispatch.clone()
        };

        quote! {
            #[cfg(feature = "sql")]
            #[allow(dead_code)]
            fn __icydb_query_dispatch(
                sql: &str,
            ) -> Result<
                (
                    ::icydb::db::sql::SqlQueryResult,
                    ::icydb::db::SqlQueryPerfAttribution,
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
                            "IcyDB SQL query target entity '{entity}' is not available on this canister"
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
            fn icydb_sql_surface_reset_all_tables() -> Result<(), ::icydb::Error> {
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
            fn icydb_sql_surface_ddl_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #ddl_arms
                    None => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        "IcyDB SQL DDL requires one target entity",
                    )),
                    Some(entity) => Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        format!(
                            "IcyDB SQL DDL target entity '{entity}' is not available on this canister"
                        ),
                    )),
                }
            }
        }
    }
}

impl quote::ToTokens for SqlSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let controller_guard = sql_surface_controller_guard();
        let perf_result = self.readonly_enabled.then(sql_surface_perf_result);
        let readonly_dispatch = self
            .readonly_enabled
            .then(|| self.readonly_dispatch_tokens());
        let ddl_dispatch = self.ddl_enabled.then(|| self.ddl_dispatch_tokens());
        let endpoints = sql_surface_endpoint_exports(
            self.readonly_enabled,
            self.ddl_enabled,
            self.fixtures_enabled,
        );
        let reset_helper = self.fixtures_enabled.then(|| self.reset_helper_tokens());

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

fn sql_surface_controller_guard() -> TokenStream {
    quote! {
        #[cfg(feature = "sql")]
        fn icydb_sql_surface_require_controller(action: &str) -> Result<(), ::icydb::Error> {
            let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
            if !::icydb::__reexports::ic_cdk::api::is_controller(&caller) {
                return Err(::icydb::Error::new(
                    ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                    ::icydb::ErrorOrigin::Interface,
                    format!("IcyDB SQL {action} requires a controller caller"),
                ));
            }

            Ok(())
        }
    }
}

fn sql_surface_perf_result() -> TokenStream {
    quote! {
        #[cfg(feature = "sql")]
        #[derive(::icydb::__reexports::candid::CandidType, Clone, Debug, Eq, PartialEq)]
        struct IcydbSqlQueryPerfResult {
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
        impl IcydbSqlQueryPerfResult {
            fn from_attribution(
                result: ::icydb::db::sql::SqlQueryResult,
                attribution: ::icydb::db::SqlQueryPerfAttribution,
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

fn sql_surface_endpoint_exports(
    readonly_enabled: bool,
    ddl_enabled: bool,
    fixtures_enabled: bool,
) -> TokenStream {
    let query_endpoint = readonly_enabled.then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::query]
        fn __icydb_query(
            sql: String,
        ) -> Result<IcydbSqlQueryPerfResult, ::icydb::Error> {
            icydb_sql_surface_require_controller("query")?;

            let (result, attribution) = __icydb_query_dispatch(sql.as_str())?;

            Ok(IcydbSqlQueryPerfResult::from_attribution(
                result,
                attribution,
            ))
        }
        }
    });

    let ddl_endpoint = ddl_enabled.then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::update]
        fn __icydb_ddl(sql: String) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            icydb_sql_surface_require_controller("DDL")?;

            icydb_sql_surface_ddl_dispatch(sql.as_str())
        }
        }
    });

    let fixture_endpoints = fixtures_enabled.then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::update]
        fn __icydb_fixtures_reset() -> Result<(), ::icydb::Error> {
            icydb_sql_surface_require_controller("lifecycle reset")?;

            icydb_sql_surface_reset_all_tables()
        }

        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::update]
        fn __icydb_fixtures_load() -> Result<(), ::icydb::Error> {
            icydb_sql_surface_require_controller("lifecycle load")?;
            let hook: fn() -> Result<(), ::icydb::Error> = crate::icydb_fixtures_load;

            icydb_sql_surface_reset_all_tables()?;
            hook()
        }
        }
    });

    quote! {
        #query_endpoint
        #ddl_endpoint
        #fixture_endpoints
    }
}

fn sql_surface_reset_statement(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        db().delete::<#entity_ty>().execute()?;
    }
}

fn sql_surface_query_dispatch_arm(entity_ty: &syn::Path) -> TokenStream {
    let entity_matches = sql_surface_entity_match_guard(entity_ty);

    quote! {
            Some(entity) if #entity_matches =>
            {
                db().execute_sql_query_with_perf_attribution::<#entity_ty>(sql)
            }
    }
}

fn sql_surface_ddl_dispatch_arm(entity_ty: &syn::Path) -> TokenStream {
    let entity_matches = sql_surface_entity_match_guard(entity_ty);

    quote! {
            Some(entity) if #entity_matches =>
            {
                db().execute_sql_ddl::<#entity_ty>(sql)
            }
    }
}

fn sql_surface_entity_match_guard(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        ::icydb::__macro::identifiers_tail_match(
            entity,
            <#entity_ty as ::icydb::traits::Path>::PATH
        ) || ::icydb::__macro::identifiers_tail_match(
            entity,
            <#entity_ty as ::icydb::traits::EntitySchema>::NAME
        )
    }
}

fn empty_sql_surface_query_dispatch() -> TokenStream {
    quote! {
        Err(::icydb::Error::new(
            ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
            ::icydb::ErrorOrigin::Interface,
            "IcyDB SQL query requires at least one canister entity",
        ))
    }
}

fn show_entities_dispatch_for(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        db().execute_sql_query_with_perf_attribution::<#entity_ty>(sql)
    }
}
