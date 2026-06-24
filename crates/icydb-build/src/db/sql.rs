//! Module: db::sql
//! Responsibility: generated SQL helper surface tokens for one canister actor.
//! Does not own: SQL parsing, query execution, or controller authorization policy.
//! Boundary: emits opt-in dispatch helpers and endpoint exports from codegen metadata.

use crate::{BuildSqlSurfaceFlags, BuildSqlUpdatePolicy};

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
    surfaces: BuildSqlSurfaceFlags,
    update_policy: Option<BuildSqlUpdatePolicy>,
    reset_statements: TokenStream,
    query_arms: TokenStream,
    ddl_arms: TokenStream,
    update_arms: TokenStream,
    show_entities_dispatch: TokenStream,
}

impl SqlSurfaceTokens {
    pub(super) fn empty(
        surfaces: BuildSqlSurfaceFlags,
        update_policy: Option<BuildSqlUpdatePolicy>,
    ) -> Self {
        Self {
            surfaces,
            update_policy,
            reset_statements: quote!(),
            query_arms: quote!(),
            ddl_arms: quote!(),
            update_arms: quote!(),
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
        if let Some(update_policy) = self.update_policy {
            self.update_arms
                .extend(sql_surface_update_dispatch_arm(entity_ty, update_policy));
        }
    }

    fn readonly_dispatch_tokens(&self) -> TokenStream {
        let query_arms = &self.query_arms;
        let introspection_guard = if self.surfaces.introspection_enabled() {
            quote!()
        } else {
            quote! {
                if dispatch.requires_introspection() {
                    return Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlIntrospectionDisabled,
                        ::icydb::ErrorOrigin::Interface,
                    ));
                }
            }
        };
        let show_entities_dispatch = if self.show_entities_dispatch.is_empty() {
            empty_sql_surface_query_dispatch()
        } else {
            self.show_entities_dispatch.clone()
        };

        quote! {
            #[cfg(feature = "sql")]
            fn __icydb_query_dispatch(
                sql: &str,
            ) -> Result<
                (
                    ::icydb::db::sql::SqlQueryResult,
                    ::icydb::db::SqlQueryPerfAttribution,
                ),
                ::icydb::Error,
            > {
                let dispatch = ::icydb::__macro::sql_statement_dispatch(sql)?;
                #introspection_guard

                match dispatch.entity_name() {
                    #query_arms
                    None => #show_entities_dispatch,
                    Some(_entity) => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryEntityNotConfigured,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                }
            }
        }
    }

    fn reset_helper_tokens(&self) -> TokenStream {
        let reset_statements = &self.reset_statements;

        quote! {
            #[cfg(feature = "sql")]
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
            fn icydb_sql_surface_ddl_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #ddl_arms
                    None => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlDdlTargetRequired,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                    Some(_entity) => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlDdlEntityNotConfigured,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                }
            }
        }
    }

    fn update_dispatch_tokens(&self) -> TokenStream {
        let update_arms = &self.update_arms;

        quote! {

            #[cfg(feature = "sql")]
            fn icydb_sql_surface_update_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    #update_arms
                    None => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                    Some(_entity) => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryEntityNotConfigured,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                }
            }
        }
    }
}

impl quote::ToTokens for SqlSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let controller_guard = sql_surface_controller_guard();
        let readonly_enabled = self.surfaces.readonly_enabled();
        let ddl_enabled = self.surfaces.ddl_enabled();
        let fixtures_enabled = self.surfaces.fixtures_enabled();
        let perf_result = readonly_enabled.then(sql_surface_perf_result);
        let readonly_dispatch = self
            .surfaces
            .readonly_enabled()
            .then(|| self.readonly_dispatch_tokens());
        let ddl_dispatch = ddl_enabled.then(|| self.ddl_dispatch_tokens());
        let update_dispatch = self
            .update_policy
            .is_some()
            .then(|| self.update_dispatch_tokens());
        let endpoints = sql_surface_endpoint_exports(self.surfaces, self.update_policy);
        let reset_helper = fixtures_enabled.then(|| self.reset_helper_tokens());

        tokens.extend(quote! {
            #controller_guard
            #perf_result
            #readonly_dispatch
            #reset_helper
            #ddl_dispatch
            #update_dispatch
            #endpoints
        });
    }
}

fn sql_surface_controller_guard() -> TokenStream {
    quote! {
        #[cfg(feature = "sql")]
        fn icydb_sql_surface_require_controller(_action: &str) -> Result<(), ::icydb::Error> {
            let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
            if !::icydb::__reexports::ic_cdk::api::is_controller(&caller) {
                return Err(::icydb::Error::from_runtime_boundary(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SqlSurfaceControllerRequired,
                    ::icydb::ErrorOrigin::Interface,
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
    surfaces: BuildSqlSurfaceFlags,
    update_policy: Option<BuildSqlUpdatePolicy>,
) -> TokenStream {
    let query_endpoint = surfaces.readonly_enabled().then(|| {
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

    let ddl_endpoint = surfaces.ddl_enabled().then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::update]
        fn __icydb_ddl(sql: String) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            icydb_sql_surface_require_controller("DDL")?;

            icydb_sql_surface_ddl_dispatch(sql.as_str())
        }
        }
    });

    let fixture_endpoints = surfaces.fixtures_enabled().then(|| {
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

    let update_endpoint = update_policy.is_some().then(|| {
        quote! {
        #[cfg(feature = "sql")]
        #[::icydb::__reexports::ic_cdk::update]
        fn __icydb_update(sql: String) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
            icydb_sql_surface_require_controller("SQL update")?;

            icydb_sql_surface_update_dispatch(sql.as_str())
        }
        }
    });

    quote! {
        #query_endpoint
        #ddl_endpoint
        #fixture_endpoints
        #update_endpoint
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

fn sql_surface_update_dispatch_arm(
    entity_ty: &syn::Path,
    policy: BuildSqlUpdatePolicy,
) -> TokenStream {
    let entity_matches = sql_surface_entity_match_guard(entity_ty);
    let executor = match policy {
        BuildSqlUpdatePolicy::PublicPrimaryKeyOnly => {
            quote! { execute_sql_public_primary_key_update }
        }
        BuildSqlUpdatePolicy::PublicBoundedDeterministic => {
            quote! { execute_sql_public_bounded_update }
        }
    };

    quote! {
            Some(entity) if #entity_matches =>
            {
                db().#executor::<#entity_ty>(sql)
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
        Err(::icydb::Error::from_runtime_boundary(
            ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
            ::icydb::ErrorOrigin::Interface,
        ))
    }
}

fn show_entities_dispatch_for(entity_ty: &syn::Path) -> TokenStream {
    quote! {
        db().execute_sql_query_with_perf_attribution::<#entity_ty>(sql)
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::{BuildSqlSurfaceFlags, BuildSqlUpdatePolicy};

    use super::SqlSurfaceTokens;

    fn compact_tokens(tokens: proc_macro2::TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    fn all_sql_surface_flags() -> BuildSqlSurfaceFlags {
        BuildSqlSurfaceFlags::default()
            .with_readonly_enabled(true)
            .with_ddl_enabled(true)
            .with_fixtures_enabled(true)
            .with_introspection_enabled(true)
    }

    fn sql_surface_flags_without_introspection() -> BuildSqlSurfaceFlags {
        BuildSqlSurfaceFlags::default()
            .with_readonly_enabled(true)
            .with_ddl_enabled(true)
            .with_fixtures_enabled(true)
    }

    #[test]
    fn generated_sql_surface_exports_only_query_ddl_and_fixture_endpoints() {
        let surface = compact_tokens(super::sql_surface_endpoint_exports(
            all_sql_surface_flags(),
            None,
        ));

        assert!(surface.contains("fn__icydb_query("));
        assert!(surface.contains("fn__icydb_ddl("));
        assert!(surface.contains("fn__icydb_fixtures_reset("));
        assert!(surface.contains("fn__icydb_fixtures_load("));
        assert!(
            !surface.contains("__icydb_update"),
            "generated SQL glue must not grow a row-mutation endpoint without an explicit policy gate",
        );
    }

    #[test]
    fn generated_sql_surface_never_calls_broad_session_update_executor() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SqlSurfaceTokens::empty(all_sql_surface_flags(), None);

        surface_tokens.push_entity(&entity_ty);

        let surface = compact_tokens(quote!(#surface_tokens));
        assert!(surface.contains("execute_sql_query_with_perf_attribution"));
        assert!(surface.contains("execute_sql_ddl"));
        assert!(
            !surface.contains("execute_sql_update"),
            "generated SQL glue must not route to broad session SQL UPDATE",
        );
        assert!(
            !surface.contains("execute_sql_public_primary_key_update")
                && !surface.contains("execute_sql_public_bounded_update"),
            "generated SQL glue must select an explicit generated endpoint policy before consuming public UPDATE helpers",
        );
    }

    #[test]
    fn generated_readonly_sql_surface_has_no_implicit_page_or_count_endpoint() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SqlSurfaceTokens::empty(all_sql_surface_flags(), None);

        surface_tokens.push_entity(&entity_ty);

        let endpoint = compact_tokens(super::sql_surface_endpoint_exports(
            all_sql_surface_flags(),
            None,
        ));
        let surface = compact_tokens(quote!(#surface_tokens));

        assert!(endpoint.contains("fn__icydb_query("));
        assert!(!endpoint.contains("fn__icydb_list("));
        assert!(!endpoint.contains("fn__icydb_page("));
        assert!(!endpoint.contains("fn__icydb_count("));
        assert!(surface.contains("execute_sql_query_with_perf_attribution"));
        assert!(!surface.contains("execute_sql_count"));
        assert!(!surface.contains("execute_fluent_count"));
        assert!(!surface.contains("execute_count"));
    }

    #[test]
    fn generated_sql_surface_does_not_emit_dead_code_suppressions() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SqlSurfaceTokens::empty(
            all_sql_surface_flags(),
            Some(BuildSqlUpdatePolicy::PublicPrimaryKeyOnly),
        );

        surface_tokens.push_entity(&entity_ty);

        let surface = compact_tokens(quote!(#surface_tokens));

        assert!(!surface.contains("allow(dead_code)"));
    }

    #[test]
    fn generated_sql_query_surface_can_reject_introspection() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens =
            SqlSurfaceTokens::empty(sql_surface_flags_without_introspection(), None);

        surface_tokens.push_entity(&entity_ty);

        let surface = compact_tokens(quote!(#surface_tokens));
        assert!(surface.contains("sql_statement_dispatch"));
        assert!(surface.contains("requires_introspection"));
        assert!(surface.contains("SqlIntrospectionDisabled"));
        assert!(surface.contains("execute_sql_query_with_perf_attribution"));
    }

    #[test]
    fn generated_sql_update_surface_requires_explicit_primary_key_policy() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SqlSurfaceTokens::empty(
            all_sql_surface_flags(),
            Some(BuildSqlUpdatePolicy::PublicPrimaryKeyOnly),
        );

        surface_tokens.push_entity(&entity_ty);

        let endpoint = compact_tokens(super::sql_surface_endpoint_exports(
            all_sql_surface_flags(),
            Some(BuildSqlUpdatePolicy::PublicPrimaryKeyOnly),
        ));
        let surface = compact_tokens(quote!(#surface_tokens));
        assert!(endpoint.contains("fn__icydb_update("));
        assert!(surface.contains("icydb_sql_surface_update_dispatch"));
        assert!(surface.contains("execute_sql_public_primary_key_update"));
        assert!(
            !surface.contains("execute_sql_update"),
            "generated SQL update glue must not call broad session SQL UPDATE",
        );
        assert!(
            !surface.contains("execute_sql_public_bounded_update"),
            "first generated SQL update policy must not expose bounded multi-row UPDATE",
        );
    }

    #[test]
    fn generated_sql_update_surface_can_select_bounded_policy_without_broad_update() {
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SqlSurfaceTokens::empty(
            all_sql_surface_flags(),
            Some(BuildSqlUpdatePolicy::PublicBoundedDeterministic),
        );

        surface_tokens.push_entity(&entity_ty);

        let surface = compact_tokens(quote!(#surface_tokens));
        assert!(surface.contains("icydb_sql_surface_update_dispatch"));
        assert!(surface.contains("execute_sql_public_bounded_update"));
        assert!(
            !surface.contains("execute_sql_update"),
            "generated SQL update glue must not call broad session SQL UPDATE",
        );
        assert!(
            !surface.contains("execute_sql_public_primary_key_update"),
            "bounded generated SQL update policy must not silently select the primary-key-only helper",
        );
    }
}
