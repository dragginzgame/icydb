//! Module: db::sql
//! Responsibility: generated private SQL capability tokens for one canister actor.
//! Does not own: public endpoint declarations, authorization, or SQL execution semantics.
//! Boundary: emits capability-gated private dispatch and handler items only.

use proc_macro2::TokenStream;
use quote::quote;

/// Private SQL token bundle for one generated actor.
pub(super) struct SqlSurfaceTokens {
    has_entities: bool,
    reset_statements: TokenStream,
}

impl SqlSurfaceTokens {
    pub(super) fn empty() -> Self {
        Self {
            has_entities: false,
            reset_statements: TokenStream::new(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_name: &str) {
        self.has_entities = true;
        self.reset_statements
            .extend(sql_surface_reset_statement(entity_name));
    }

    fn readonly_dispatch_tokens(&self) -> TokenStream {
        let entity_dispatch = if self.has_entities {
            quote! { db()?.execute_trusted_sql_query_with_perf_attribution(sql) }
        } else {
            empty_sql_surface_query_dispatch()
        };
        let show_entities_dispatch = if self.has_entities {
            quote! { db()?.execute_trusted_sql_query_with_perf_attribution(sql) }
        } else {
            empty_sql_surface_query_dispatch()
        };

        quote! {
            fn __icydb_query_dispatch<const INTROSPECTION: bool>(
                sql: &str,
            ) -> Result<
                (
                    ::icydb::db::sql::SqlQueryResult,
                    ::icydb::db::SqlQueryPerfAttribution,
                ),
                ::icydb::Error,
            > {
                let dispatch = ::icydb::__macro::sql_statement_dispatch(sql)?;
                if !INTROSPECTION && dispatch.requires_introspection() {
                    return Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlIntrospectionDisabled,
                        ::icydb::ErrorOrigin::Interface,
                    ));
                }

                match dispatch.entity_name() {
                    None => #show_entities_dispatch,
                    Some(_entity) => #entity_dispatch,
                }
            }
        }
    }

    fn reset_helper_tokens(&self) -> TokenStream {
        let reset_statements = &self.reset_statements;

        if reset_statements.is_empty() {
            quote! {
                const fn __icydb_sql_surface_reset_all_tables(
                ) {}
            }
        } else {
            quote! {
                fn __icydb_sql_surface_reset_all_tables(
                ) -> Result<(), ::icydb::Error> {
                    #reset_statements

                    Ok(())
                }
            }
        }
    }

    fn ddl_dispatch_tokens(&self) -> TokenStream {
        let entity_dispatch = if self.has_entities {
            quote! { db()?.execute_admin_sql_ddl(sql) }
        } else {
            quote! {
                Err(::icydb::Error::from_runtime_boundary(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SqlDdlEntityNotConfigured,
                    ::icydb::ErrorOrigin::Interface,
                ))
            }
        };

        quote! {
            fn __icydb_sql_surface_ddl_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    None => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlDdlTargetRequired,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                    Some(_entity) => #entity_dispatch,
                }
            }
        }
    }

    fn update_dispatch_tokens(&self) -> TokenStream {
        let primary_key_dispatch = if self.has_entities {
            quote! { db()?.execute_sql_public_primary_key_update(sql) }
        } else {
            empty_sql_surface_query_dispatch()
        };
        let bounded_dispatch = if self.has_entities {
            quote! { db()?.execute_sql_public_bounded_update(sql) }
        } else {
            empty_sql_surface_query_dispatch()
        };

        quote! {
            fn __icydb_sql_surface_update_primary_key_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    None => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                    Some(_entity) => #primary_key_dispatch,
                }
            }

            fn __icydb_sql_surface_update_bounded_dispatch(
                sql: &str,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                match ::icydb::__macro::sql_statement_entity_name(sql)?.as_deref() {
                    None => Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
                        ::icydb::ErrorOrigin::Interface,
                    )),
                    Some(_entity) => #bounded_dispatch,
                }
            }
        }
    }

    fn handler_tokens(&self) -> TokenStream {
        let reset_result = if self.reset_statements.is_empty() {
            quote! {
                __icydb_sql_surface_reset_all_tables();
                Ok(())
            }
        } else {
            quote! { __icydb_sql_surface_reset_all_tables() }
        };

        quote! {
            pub(crate) fn __icydb_endpoint_handler_sql_query<const INTROSPECTION: bool>(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryPerfResult, ::icydb::Error> {
                let (result, attribution) =
                    __icydb_query_dispatch::<INTROSPECTION>(sql.as_str())?;

                Ok(::icydb::db::sql::SqlQueryPerfResult::from_attribution(
                    result,
                    attribution,
                ))
            }

            pub(crate) fn __icydb_endpoint_handler_sql_ddl(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                __icydb_sql_surface_ddl_dispatch(sql.as_str())
            }

            #[allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]
            pub(crate) fn __icydb_endpoint_handler_fixtures_reset(
            ) -> Result<(), ::icydb::Error> {
                #reset_result
            }

            pub(crate) fn __icydb_endpoint_handler_fixtures_load(
                handler: fn() -> Result<(), ::icydb::Error>,
            ) -> Result<(), ::icydb::Error> {
                __icydb_endpoint_handler_fixtures_reset()?;
                handler()
            }

            pub(crate) fn __icydb_endpoint_handler_sql_update_primary_key(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                __icydb_sql_surface_update_primary_key_dispatch(sql.as_str())
            }

            pub(crate) fn __icydb_endpoint_handler_sql_update_bounded(
                sql: String,
            ) -> Result<::icydb::db::sql::SqlQueryResult, ::icydb::Error> {
                __icydb_sql_surface_update_bounded_dispatch(sql.as_str())
            }

            #[allow(clippy::result_large_err)]
            pub(crate) fn __icydb_endpoint_handler_sql_integrity(
                sql: String,
            ) -> Result<::icydb::db::IntegrityCheckResult, ::icydb::db::SqlIntegrityError> {
                let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
                let owner =
                    ::icydb::db::IntegrityJobOwner::new(caller.to_text()).map_err(|error| {
                        ::icydb::db::SqlIntegrityError::Integrity(
                            ::icydb::db::IntegrityCheckError::Job(error),
                        )
                    })?;
                let session = db().map_err(|error| {
                    ::icydb::db::SqlIntegrityError::Integrity(
                        ::icydb::db::IntegrityCheckError::Database(error),
                    )
                })?;

                session.execute_admin_integrity_sql(sql.as_str(), owner)
            }
        }
    }
}

impl quote::ToTokens for SqlSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let readonly_dispatch = self.readonly_dispatch_tokens();
        let reset_helper = self.reset_helper_tokens();
        let ddl_dispatch = self.ddl_dispatch_tokens();
        let update_dispatch = self.update_dispatch_tokens();
        let handlers = self.handler_tokens();

        tokens.extend(quote! {
            ::icydb::__icydb_with_sql_items! {
                #readonly_dispatch
                #reset_helper
                #ddl_dispatch
                #update_dispatch
                #handlers
            }
        });
    }
}

fn sql_surface_reset_statement(entity_name: &str) -> TokenStream {
    let delete_sql = format!("DELETE FROM {entity_name}");
    quote! {
        let _ = db()?.execute_trusted_sql_mutation(#delete_sql)?;
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

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::SqlSurfaceTokens;

    fn compact_tokens(tokens: proc_macro2::TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    #[test]
    fn generated_sql_capability_contains_every_private_handler_and_no_export() {
        let mut surface = SqlSurfaceTokens::empty();
        surface.push_entity("Character");
        let surface = compact_tokens(quote!(#surface));

        for handler in [
            "sql_query",
            "sql_ddl",
            "fixtures_reset",
            "fixtures_load",
            "sql_update_primary_key",
            "sql_update_bounded",
            "sql_integrity",
        ] {
            assert!(surface.contains(format!("__icydb_endpoint_handler_{handler}").as_str()));
        }
        for forbidden in [
            "ic_cdk::query",
            "ic_cdk::update",
            "export_name",
            "no_mangle",
        ] {
            assert!(!surface.contains(forbidden));
        }
    }

    #[test]
    fn generated_sql_capability_keeps_both_sealed_update_admissions() {
        let mut surface = SqlSurfaceTokens::empty();
        surface.push_entity("Character");
        let surface = compact_tokens(quote!(#surface));

        assert!(surface.contains("execute_sql_public_primary_key_update(sql)"));
        assert!(surface.contains("execute_sql_public_bounded_update(sql)"));
        assert!(surface.contains("__icydb_sql_surface_update_primary_key_dispatch"));
        assert!(surface.contains("__icydb_sql_surface_update_bounded_dispatch"));
    }

    #[test]
    fn generated_sql_capability_uses_trusted_query_and_admin_ddl() {
        let mut surface = SqlSurfaceTokens::empty();
        surface.push_entity("Character");
        let surface = compact_tokens(quote!(#surface));

        assert!(surface.contains("execute_trusted_sql_query_with_perf_attribution(sql)"));
        assert!(surface.contains("execute_admin_sql_ddl(sql)"));
    }

    #[test]
    fn generated_sql_query_capability_keeps_introspection_gate() {
        let mut surface = SqlSurfaceTokens::empty();
        surface.push_entity("Character");
        let surface = compact_tokens(quote!(#surface));

        assert!(surface.contains("if!INTROSPECTION&&dispatch.requires_introspection()"));
        assert!(surface.contains("SqlIntrospectionDisabled"));
    }

    #[test]
    fn generated_fixture_load_reuses_reset_then_calls_handler_once() {
        let surface = SqlSurfaceTokens::empty();
        let handler = compact_tokens(surface.handler_tokens());
        let reset = handler
            .find("__icydb_endpoint_handler_fixtures_reset()?")
            .expect("fixture load should propagate reset failure");
        let load = handler
            .find("handler()")
            .expect("fixture load should invoke the checked handler");

        assert!(reset < load);
        assert_eq!(handler.matches("handler()").count(), 1);
    }

    #[test]
    fn empty_and_entity_sql_capabilities_keep_exact_dispatch_boundaries() {
        let empty_surface = SqlSurfaceTokens::empty();
        let empty = compact_tokens(quote!(#empty_surface));
        let mut entity = SqlSurfaceTokens::empty();
        entity.push_entity("Character");
        let entity = compact_tokens(quote!(#entity));

        assert!(empty.contains("SqlQueryNoConfiguredEntities"));
        assert!(!empty.contains("execute_trusted_sql_query_with_perf_attribution"));
        assert!(entity.contains("execute_trusted_sql_query_with_perf_attribution"));
        assert!(entity.contains("DELETEFROMCharacter"));
    }
}
