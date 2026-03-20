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

    let mut lane_callback_defs = quote!();
    let mut descriptor_entries = quote!();

    for (entity_id, (entity_path, _entity)) in entities.into_iter().enumerate() {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));

        let query_fn_ident = format_ident!("__sql_query_{entity_id}");
        let explain_fn_ident = format_ident!("__sql_explain_{entity_id}");
        let lane_table_ident = format_ident!("SQL_LANE_TABLE_{entity_id}");

        lane_callback_defs.extend(quote! {
            fn #query_fn_ident(
                lowered: &::icydb::__macro::LoweredSqlCommand,
            ) -> Result<SqlQueryResult, Error> {
                db().execute_lowered_sql_dispatch_query::<#entity_ty>(lowered)
            }
        });

        lane_callback_defs.extend(quote! {
            fn #explain_fn_ident(
                lowered: &::icydb::__macro::LoweredSqlCommand,
            ) -> Result<SqlQueryResult, Error> {
                db().explain_lowered_sql_dispatch::<#entity_ty>(lowered)
            }
        });

        lane_callback_defs.extend(quote! {
            static #lane_table_ident: SqlLaneTable = SqlLaneTable {
                query: #query_fn_ident,
                explain: #explain_fn_ident,
            };
        });

        descriptor_entries.extend(quote! {
            SqlEntityDescriptor {
                entity_id: #entity_id,
                name: <#entity_ty as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
                schema: <#entity_ty as ::icydb::traits::EntitySchema>::MODEL,
                routes: &#lane_table_ident,
            },
        });
    }

    quote! {
        #[cfg(feature = "sql")]
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
                db::sql::SqlQueryResult,
                error::{ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind},
            };

            ///
            /// SqlLaneTable
            ///
            /// Static function table for one SQL entity route.
            ///
            #[derive(Clone, Copy, Debug)]
            pub struct SqlLaneTable {
                pub query: fn(&::icydb::__macro::LoweredSqlCommand) -> Result<SqlQueryResult, Error>,
                pub explain: fn(&::icydb::__macro::LoweredSqlCommand) -> Result<SqlQueryResult, Error>,
            }

            ///
            /// SqlEntityDescriptor
            ///
            /// Immutable runtime SQL descriptor for one concrete entity route.
            ///
            #[derive(Clone, Copy, Debug)]
            pub struct SqlEntityDescriptor {
                pub entity_id: usize,
                pub name: &'static str,
                pub schema: &'static ::icydb::model::entity::EntityModel,
                pub routes: &'static SqlLaneTable,
            }

            ///
            /// SqlEntityRoute
            ///
            /// One generated runtime route that resolves to one immutable entity descriptor.
            ///
            #[derive(Clone, Copy, Debug)]
            pub struct SqlEntityRoute {
                descriptor: &'static SqlEntityDescriptor,
            }

            #lane_callback_defs

            static SQL_ENTITY_DESCRIPTORS: &[SqlEntityDescriptor] = &[
                #descriptor_entries
            ];

            impl SqlEntityRoute {
                /// Resolve one runtime entity route from a parsed SQL statement.
                pub fn from_statement_route(statement: &::icydb::db::SqlStatementRoute) -> Result<Self, Error> {
                    if statement.is_show_entities() {
                        return Err(unsupported_entity_route_statement_error());
                    }

                    let sql_entity = statement.entity();
                    Self::from_entity_name(sql_entity)
                        .ok_or_else(|| unsupported_sql_entity_error(sql_entity))
                }

                /// Resolve one runtime entity route from one SQL entity identifier.
                #[must_use]
                pub fn from_entity_name(entity_name: &str) -> Option<Self> {
                    for descriptor in SQL_ENTITY_DESCRIPTORS {
                        if ::icydb::db::identifiers_tail_match(
                            entity_name,
                            descriptor.name,
                        ) {
                            return Some(Self { descriptor });
                        }
                    }

                    None
                }

                /// Return this route's canonical entity name.
                #[must_use]
                pub const fn entity_name(self) -> &'static str {
                    self.descriptor.name
                }

                /// Return this route's primary-key field name.
                #[must_use]
                pub const fn primary_key_field(self) -> &'static str {
                    self.descriptor.schema.primary_key().name()
                }

                /// Execute one SQL statement for this concrete route.
                pub fn execute_query(
                    self,
                    lowered: &::icydb::__macro::LoweredSqlCommand,
                ) -> Result<SqlQueryResult, Error> {
                    (self.descriptor.routes.query)(lowered)
                }

                /// Execute one already-lowered SQL explain statement for this concrete route.
                pub fn execute_explain(
                    self,
                    lowered: &::icydb::__macro::LoweredSqlCommand,
                ) -> Result<SqlQueryResult, Error> {
                    (self.descriptor.routes.explain)(lowered)
                }

                /// Describe this route's schema using descriptor-owned model authority.
                #[must_use]
                pub fn describe_descriptor_model(self) -> ::icydb::db::EntitySchemaDescription {
                    db().describe_entity_model(self.descriptor.schema)
                }

                /// Return one stable index listing for this route's schema model.
                #[must_use]
                pub fn descriptor_index_names(self) -> Vec<String> {
                    db().show_indexes_for_model(self.descriptor.schema)
                }

                /// Return one stable column listing for this route's schema model.
                #[must_use]
                pub fn descriptor_column_descriptions(self) -> Vec<::icydb::db::EntityFieldDescription> {
                    db().show_columns_for_model(self.descriptor.schema)
                }
            }

            /// Return one list of all supported SQL entity names.
            #[must_use]
            pub fn entities() -> Vec<String> {
                SQL_ENTITY_DESCRIPTORS
                    .iter()
                    .map(|descriptor| descriptor.name.to_string())
                    .collect()
            }

            /// Execute one reduced SQL statement and return one typed SQL surface payload.
            pub fn query(sql: &str) -> Result<SqlQueryResult, Error> {
                let sql_trimmed = ::icydb::db::sql::normalize_sql_input(sql)?;
                let parsed = db().parse_sql_statement(sql_trimmed)?;
                let statement = parsed.route().clone();
                match statement {
                    statement @ (::icydb::db::SqlStatementRoute::Query { .. }
                    | ::icydb::db::SqlStatementRoute::Explain { .. }) => {
                        query_lane_result_for_statement(sql_trimmed, &parsed, &statement)
                    }
                    statement @ ::icydb::db::SqlStatementRoute::Describe { .. } => {
                        describe_result_for_statement(&statement)
                    }
                    statement @ ::icydb::db::SqlStatementRoute::ShowIndexes { .. } => {
                        show_indexes_result_for_statement(&statement)
                    }
                    statement @ ::icydb::db::SqlStatementRoute::ShowColumns { .. } => {
                        show_columns_result_for_statement(&statement)
                    }
                    ::icydb::db::SqlStatementRoute::ShowEntities => {
                        Ok(show_entities_result_for_statement())
                    }
                }
            }

            fn query_lane_result_for_statement(
                sql: &str,
                parsed: &::icydb::db::SqlParsedStatement,
                statement: &::icydb::db::SqlStatementRoute,
            ) -> Result<SqlQueryResult, Error> {
                let route = SqlEntityRoute::from_statement_route(statement)?;
                let prepared = db().prepare_sql_dispatch_parsed(parsed, route.entity_name())?;
                let lowered =
                    db().lower_sql_dispatch_query_lane_prepared(&prepared, route.primary_key_field())?;
                let result = if statement.is_explain() {
                    route.execute_explain(&lowered)
                } else {
                    route.execute_query(&lowered)
                };

                if matches!(statement, ::icydb::db::SqlStatementRoute::Explain { .. }) {
                    return result.map_err(|err| explain_surface_error(sql, route, err));
                }

                result
            }

            fn describe_result_for_statement(
                statement: &::icydb::db::SqlStatementRoute,
            ) -> Result<SqlQueryResult, Error> {
                let route = SqlEntityRoute::from_statement_route(statement)?;
                let description = route.describe_descriptor_model();

                Ok(SqlQueryResult::Describe(description))
            }

            fn show_indexes_result_for_statement(
                statement: &::icydb::db::SqlStatementRoute,
            ) -> Result<SqlQueryResult, Error> {
                let route = SqlEntityRoute::from_statement_route(statement)?;
                let indexes = route.descriptor_index_names();

                Ok(SqlQueryResult::ShowIndexes {
                    entity: route.entity_name().to_string(),
                    indexes,
                })
            }

            fn show_columns_result_for_statement(
                statement: &::icydb::db::SqlStatementRoute,
            ) -> Result<SqlQueryResult, Error> {
                let route = SqlEntityRoute::from_statement_route(statement)?;
                let columns = route.descriptor_column_descriptions();

                Ok(SqlQueryResult::ShowColumns {
                    entity: route.entity_name().to_string(),
                    columns,
                })
            }

            fn show_entities_result_for_statement() -> SqlQueryResult {
                let entities = db().show_entities();

                SqlQueryResult::ShowEntities { entities }
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

            fn unsupported_entity_route_statement_error() -> Error {
                Error::new(
                    ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                    ErrorOrigin::Query,
                    "entity route resolution requires one entity-scoped SQL statement",
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
