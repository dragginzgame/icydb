use crate::ActorBuilder;
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
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut store_inits = quote!();
    let stores = builder.get_stores();
    // -------------------------
    // Store registry
    // -------------------------

    for (store_path, store) in &stores {
        let data_cell_ident = format_ident!("{}_DATA", store.ident());
        let index_cell_ident = format_ident!("{}_INDEX", store.ident());
        let data_memory_id = store.data_memory_id();
        let index_memory_id = store.index_memory_id();
        let store_path_lit = store_path.as_str();

        data_defs.extend(quote! {
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
        });

        index_defs.extend(quote! {
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
        });

        store_inits.extend(quote! {
            reg.register_store(#store_path_lit, &#data_cell_ident, &#index_cell_ident)
                .expect("store registration should succeed");
        });
    }

    // -------------------------
    // Canister + DB wiring
    // -------------------------

    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def().path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", canister.def().path()));

    let entity_runtime_hooks = entity_runtime_hooks(builder, &canister_path);
    let memory_min = canister.memory_min();
    let memory_max = canister.memory_max();

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

        /// Global accessor (fat handle) for this canister’s DB.
        /// This is the **only** API applications should use.
        #[must_use]
        pub const fn db() -> ::icydb::db::DbSession<#canister_path> {
            ::icydb::db::DbSession::new(
                ::icydb::__macro::CoreDbSession::<#canister_path>::new_with_hooks(
                    &STORE_REGISTRY,
                    ENTITY_RUNTIME_HOOKS
                )
            )
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

#[expect(clippy::too_many_lines)]
fn sql_dispatch(builder: &ActorBuilder) -> TokenStream {
    let entities = builder.get_entities();

    let mut variants = quote!();
    let mut all_routes = quote!();
    let mut entity_name_match = quote!();
    let mut primary_key_field_match = quote!();
    let mut projection_match = quote!();
    let mut explain_match = quote!();

    for (entity_path, entity) in entities {
        let variant_ident = format_ident!("{}", entity.def().ident());
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));

        variants.extend(quote! {
            #variant_ident,
        });
        all_routes.extend(quote! {
            SqlEntityRoute::#variant_ident,
        });
        entity_name_match.extend(quote! {
            Self::#variant_ident => {
                <#entity_ty as ::icydb::traits::EntityIdentity>::ENTITY_NAME
            }
        });
        primary_key_field_match.extend(quote! {
            Self::#variant_ident => {
                <#entity_ty as ::icydb::traits::EntitySchema>::MODEL
                    .primary_key()
                    .name()
            }
        });
        projection_match.extend(quote! {
            Self::#variant_ident => {
                let columns = db().sql_projection_columns::<#entity_ty>(sql)?;
                let projection = db().execute_sql_projection::<#entity_ty>(sql)?;
                Ok(::icydb::db::sql::projection_rows_from_response::<#entity_ty>(
                    columns, projection,
                ))
            }
        });
        explain_match.extend(quote! {
            Self::#variant_ident => db().explain_sql::<#entity_ty>(sql),
        });
    }

    quote! {
        ///
        /// SQL Runtime Dispatch
        ///
        /// Auto-generated helpers that map runtime SQL entity identifiers
        /// to concrete entity types for this canister.
        ///
        pub mod sql_dispatch {
            use super::db;

            use ::icydb::{
                Error,
                db::sql::SqlProjectionRows,
                error::{ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind},
            };

            ///
            /// SqlEntityRoute
            ///
            /// One generated runtime route that resolves to a concrete schema entity type.
            ///
            #[derive(Clone, Copy, Debug, Eq, PartialEq)]
            pub enum SqlEntityRoute {
                #variants
            }

            static SQL_ENTITY_ROUTES: &[SqlEntityRoute] = &[#all_routes];

            impl SqlEntityRoute {
                /// Resolve one runtime entity route from a parsed SQL statement.
                pub fn from_statement_route(statement: &::icydb::db::SqlStatementRoute) -> Result<Self, Error> {
                    let sql_entity = statement.entity();
                    Self::from_entity_name(sql_entity)
                        .ok_or_else(|| unsupported_sql_entity_error(sql_entity))
                }

                /// Resolve one runtime entity route from one SQL entity identifier.
                #[must_use]
                pub fn from_entity_name(entity_name: &str) -> Option<Self> {
                    for route in SQL_ENTITY_ROUTES {
                        if ::icydb::db::identifiers_tail_match(
                            entity_name,
                            route.entity_name(),
                        ) {
                            return Some(*route);
                        }
                    }

                    None
                }

                /// Return this route's canonical entity name.
                #[must_use]
                pub const fn entity_name(self) -> &'static str {
                    match self {
                        #entity_name_match
                    }
                }

                /// Return this route's primary-key field name.
                #[must_use]
                pub const fn primary_key_field(self) -> &'static str {
                    match self {
                        #primary_key_field_match
                    }
                }

                /// Execute one SQL projection query for this concrete route.
                pub fn execute_projection_rows(self, sql: &str) -> Result<SqlProjectionRows, Error> {
                    match self {
                        #projection_match
                    }
                }

                /// Execute one SQL explain query for this concrete route.
                pub fn execute_explain(self, sql: &str) -> Result<String, Error> {
                    match self {
                        #explain_match
                    }
                }
            }

            /// Return one list of all supported SQL entity names.
            #[must_use]
            pub fn entities() -> Vec<String> {
                SQL_ENTITY_ROUTES
                    .iter()
                    .map(|route| route.entity_name().to_string())
                    .collect()
            }

            /// Execute one reduced SQL statement and render shell-friendly output lines.
            pub fn query(sql: &str) -> Result<Vec<String>, Error> {
                let sql_trimmed = ::icydb::db::sql::normalize_sql_input(sql)?;
                let statement = db().sql_statement_route(sql_trimmed)?;
                let route = SqlEntityRoute::from_statement_route(&statement)?;
                if statement.is_explain() {
                    let explain_text = route
                        .execute_explain(sql_trimmed)
                        .map_err(|err| explain_surface_error(sql_trimmed, route, err))?;
                    return Ok(::icydb::db::sql::render_explain_lines(explain_text.as_str()));
                }

                let (entity, rows) = projection_rows_for_statement(sql_trimmed, &statement)?;

                Ok(::icydb::db::sql::render_projection_lines(entity.as_str(), &rows))
            }

            /// Execute one reduced SQL projection statement and return structured rows.
            pub fn query_rows(sql: &str) -> Result<::icydb::db::sql::SqlQueryRowsOutput, Error> {
                let sql_trimmed = ::icydb::db::sql::normalize_sql_input(sql)?;
                let statement = db().sql_statement_route(sql_trimmed)?;
                if statement.is_explain() {
                    return Err(unsupported_query_rows_explain_error());
                }

                let (entity, rows) = projection_rows_for_statement(sql_trimmed, &statement)?;

                Ok(::icydb::db::sql::SqlQueryRowsOutput::from_projection(
                    entity, rows,
                ))
            }

            /// Resolve SQL entity and execute one projection query.
            pub fn projection_rows(sql: &str) -> Result<(String, SqlProjectionRows), Error> {
                let statement = db().sql_statement_route(sql)?;
                projection_rows_for_statement(sql, &statement)
            }

            /// Resolve SQL entity and execute one explain query.
            pub fn explain(sql: &str) -> Result<String, Error> {
                let statement = db().sql_statement_route(sql)?;
                let route = SqlEntityRoute::from_statement_route(&statement)?;
                route.execute_explain(sql)
            }

            fn projection_rows_for_statement(
                sql: &str,
                statement: &::icydb::db::SqlStatementRoute,
            ) -> Result<(String, SqlProjectionRows), Error> {
                let route = SqlEntityRoute::from_statement_route(statement)?;
                let rows = route.execute_projection_rows(sql)?;

                Ok((route.entity_name().to_string(), rows))
            }

            fn unsupported_sql_entity_error(entity_name: &str) -> Error {
                let supported = entities().join(", ");

                Error::new(
                    ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                    ErrorOrigin::Query,
                    format!(
                        "query endpoint does not support entity '{entity_name}'; supported: {supported}"
                    ),
                )
            }

            fn unsupported_query_rows_explain_error() -> Error {
                Error::new(
                    ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                    ErrorOrigin::Query,
                    "query_rows supports projection SQL only; use query for EXPLAIN output",
                )
            }

            fn explain_surface_error(sql: &str, route: SqlEntityRoute, err: Error) -> Error {
                if !matches!(
                    err.kind(),
                    ErrorKind::Query(QueryErrorKind::UnorderedPagination)
                ) {
                    return err;
                }

                let target_sql = ::icydb::db::sql::explain_target_sql(sql);
                let suggestion = explain_order_hint_sql(target_sql, route.primary_key_field());
                let message = format!(
                    "Cannot EXPLAIN this SQL statement.\n\nReason:\nThe wrapped query uses LIMIT or OFFSET without ORDER BY, so it is non-deterministic and not executable under IcyDB's ordering contract.\n\nSQL:\n{target_sql}\n\nHow to fix:\nAdd an explicit ORDER BY that produces a stable total order, for example:\n{suggestion}",
                );

                Error::new(
                    ErrorKind::Query(QueryErrorKind::UnorderedPagination),
                    err.origin(),
                    message,
                )
            }

            fn explain_order_hint_sql(target_sql: &str, order_field: &str) -> String {
                let trimmed = target_sql.trim().trim_end_matches(';').trim_end();
                let upper = trimmed.to_ascii_uppercase();

                if let Some(index) = upper.find(" LIMIT ") {
                    return format!(
                        "EXPLAIN {} ORDER BY {order_field} ASC{}",
                        &trimmed[..index],
                        &trimmed[index..]
                    );
                } else if let Some(index) = upper.find(" OFFSET ") {
                    return format!(
                        "EXPLAIN {} ORDER BY {order_field} ASC{}",
                        &trimmed[..index],
                        &trimmed[index..]
                    );
                }

                format!("EXPLAIN {trimmed} ORDER BY {order_field} ASC")
            }
        }
    }
}
