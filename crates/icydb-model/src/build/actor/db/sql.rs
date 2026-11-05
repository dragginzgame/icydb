//! Module: db::sql
//! Responsibility: generated private SQL capability tokens for one canister actor.
//! Does not own: public endpoint declarations, authorization, or SQL execution semantics.
//! Boundary: emits capability-gated private dispatch and handler items only.

use proc_macro2::TokenStream;
use quote::quote;

/// Private SQL token bundle for one generated actor.
pub(super) struct SqlSurfaceTokens {
    icydb: TokenStream,
    has_entities: bool,
    reset_statements: TokenStream,
}

impl SqlSurfaceTokens {
    pub(super) fn empty(icydb: TokenStream) -> Self {
        Self {
            icydb,
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
        let icydb = &self.icydb;
        let entity_dispatch = if self.has_entities {
            quote! {
                db()?.execute_trusted_sql_query_dispatch(&dispatch, &[])
            }
        } else {
            empty_sql_surface_query_dispatch(&self.icydb)
        };

        quote! {
            fn __icydb_query_dispatch<const INTROSPECTION: bool>(
                sql: &str,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                let dispatch = #icydb::__macro::sql_statement_dispatch(sql)?;
                if !INTROSPECTION && dispatch.requires_introspection() {
                    return ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
                        #icydb::diagnostic::RuntimeBoundaryCode::SqlIntrospectionDisabled,
                        #icydb::ErrorOrigin::Interface,
                    ));
                }

                #entity_dispatch
            }
        }
    }

    fn reset_helper_tokens(&self) -> TokenStream {
        let icydb = &self.icydb;
        let reset_statements = &self.reset_statements;

        if reset_statements.is_empty() {
            quote! {
                const fn __icydb_sql_surface_reset_all_tables(
                ) {}
            }
        } else {
            quote! {
                fn __icydb_sql_surface_reset_all_tables(
                ) -> ::std::result::Result<(), #icydb::Error> {
                    #reset_statements

                    ::std::result::Result::Ok(())
                }
            }
        }
    }

    fn ddl_dispatch_tokens(&self) -> TokenStream {
        let icydb = &self.icydb;
        let entity_dispatch = if self.has_entities {
            quote! { db()?.execute_admin_sql_ddl_dispatch(&dispatch) }
        } else {
            quote! {
                ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
                    #icydb::diagnostic::RuntimeBoundaryCode::SqlDdlEntityNotConfigured,
                    #icydb::ErrorOrigin::Interface,
                ))
            }
        };

        quote! {
            fn __icydb_sql_surface_ddl_dispatch(
                sql: &str,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                let dispatch = #icydb::__macro::sql_statement_dispatch(sql)?;
                match dispatch.entity_name() {
                    ::std::option::Option::None => ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
                        #icydb::diagnostic::RuntimeBoundaryCode::SqlDdlTargetRequired,
                        #icydb::ErrorOrigin::Interface,
                    )),
                    ::std::option::Option::Some(_entity) => #entity_dispatch,
                }
            }
        }
    }

    fn update_dispatch_tokens(&self) -> TokenStream {
        let icydb = &self.icydb;
        let primary_key_dispatch = if self.has_entities {
            quote! { db()?.execute_sql_public_primary_key_update(&dispatch) }
        } else {
            empty_sql_surface_query_dispatch(&self.icydb)
        };
        let bounded_dispatch = if self.has_entities {
            quote! { db()?.execute_sql_public_bounded_update(&dispatch) }
        } else {
            empty_sql_surface_query_dispatch(&self.icydb)
        };

        quote! {
            fn __icydb_sql_surface_update_primary_key_dispatch(
                sql: &str,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                let dispatch = #icydb::__macro::sql_statement_dispatch(sql)?;
                match dispatch.entity_name() {
                    ::std::option::Option::None => ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
                        #icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
                        #icydb::ErrorOrigin::Interface,
                    )),
                    ::std::option::Option::Some(_entity) => #primary_key_dispatch,
                }
            }

            fn __icydb_sql_surface_update_bounded_dispatch(
                sql: &str,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                let dispatch = #icydb::__macro::sql_statement_dispatch(sql)?;
                match dispatch.entity_name() {
                    ::std::option::Option::None => ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
                        #icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
                        #icydb::ErrorOrigin::Interface,
                    )),
                    ::std::option::Option::Some(_entity) => #bounded_dispatch,
                }
            }
        }
    }

    fn handler_tokens(&self) -> TokenStream {
        let icydb = &self.icydb;
        let reset_result = if self.reset_statements.is_empty() {
            quote! {
                __icydb_sql_surface_reset_all_tables();
                ::std::result::Result::Ok(())
            }
        } else {
            quote! { __icydb_sql_surface_reset_all_tables() }
        };

        // Only the empty actor's reset is infallible and const-capable. Keep
        // expectations attached to that emitted shape, not populated actors.
        let reset_lints = if self.reset_statements.is_empty() {
            quote! {
                #[expect(
                    clippy::missing_const_for_fn,
                    clippy::unnecessary_wraps,
                    reason = "the empty actor retains the uniform fallible reset handler contract"
                )]
            }
        } else {
            TokenStream::new()
        };

        quote! {
            #[expect(
                clippy::needless_pass_by_value,
                reason = "generated handlers retain the owned SQL endpoint input contract"
            )]
            pub(crate) fn __icydb_endpoint_handler_sql_query<const INTROSPECTION: bool>(
                sql: ::std::string::String,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                __icydb_query_dispatch::<INTROSPECTION>(sql.as_str())?
                    .into_deliverable_query_reply()
            }

            #[expect(
                clippy::needless_pass_by_value,
                reason = "generated handlers retain the owned SQL endpoint input contract"
            )]
            pub(crate) fn __icydb_endpoint_handler_sql_ddl(
                sql: ::std::string::String,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                __icydb_sql_surface_ddl_dispatch(sql.as_str())
            }

            #reset_lints
            pub(crate) fn __icydb_endpoint_handler_fixtures_reset(
            ) -> ::std::result::Result<(), #icydb::Error> {
                #reset_result
            }

            pub(crate) fn __icydb_endpoint_handler_fixtures_load(
                handler: fn() -> ::std::result::Result<(), #icydb::Error>,
            ) -> ::std::result::Result<(), #icydb::Error> {
                __icydb_endpoint_handler_fixtures_reset()?;
                handler()
            }

            #[expect(
                clippy::needless_pass_by_value,
                reason = "generated handlers retain the owned SQL endpoint input contract"
            )]
            pub(crate) fn __icydb_endpoint_handler_sql_update_primary_key(
                sql: ::std::string::String,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                __icydb_sql_surface_update_primary_key_dispatch(sql.as_str())
            }

            #[expect(
                clippy::needless_pass_by_value,
                reason = "generated handlers retain the owned SQL endpoint input contract"
            )]
            pub(crate) fn __icydb_endpoint_handler_sql_update_bounded(
                sql: ::std::string::String,
            ) -> ::std::result::Result<#icydb::db::sql::SqlQueryResult, #icydb::Error> {
                __icydb_sql_surface_update_bounded_dispatch(sql.as_str())
            }

            #[expect(
                clippy::needless_pass_by_value,
                reason = "generated integrity handlers preserve the owned SQL endpoint input contract"
            )]
            pub(crate) fn __icydb_endpoint_handler_sql_integrity(
                sql: ::std::string::String,
                owner: #icydb::db::IntegrityJobOwner,
            ) -> ::std::result::Result<#icydb::db::IntegrityCheckResult, #icydb::db::SqlIntegrityError> {
                let session = db().map_err(|error| {
                    #icydb::db::SqlIntegrityError::Integrity(
                        #icydb::db::IntegrityCheckError::Database(error),
                    )
                })?;

                session.execute_admin_integrity_sql(sql.as_str(), owner)
            }
        }
    }
}

impl quote::ToTokens for SqlSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let icydb = &self.icydb;
        let readonly_dispatch = self.readonly_dispatch_tokens();
        let reset_helper = self.reset_helper_tokens();
        let ddl_dispatch = self.ddl_dispatch_tokens();
        let update_dispatch = self.update_dispatch_tokens();
        let handlers = self.handler_tokens();

        tokens.extend(quote! {
            #icydb::__icydb_with_sql_items! {
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

fn empty_sql_surface_query_dispatch(icydb: &TokenStream) -> TokenStream {
    quote! {
        ::std::result::Result::Err(#icydb::Error::from_runtime_boundary(
            #icydb::diagnostic::RuntimeBoundaryCode::SqlQueryNoConfiguredEntities,
            #icydb::ErrorOrigin::Interface,
        ))
    }
}

#[cfg(test)]
mod tests;
