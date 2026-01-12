use crate::ActorBuilder;
use icydb_schema::types::StoreType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_str;

///
/// Entry point for codegen.
/// Expands into:
/// - thread-local Data/Index store definitions
/// - registry wiring
/// - a global `db()` accessor
///
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();

    // Generate store definitions + registries
    tokens.extend(stores(builder));

    tokens
}

///
/// Generate thread-local DataStores and IndexStores for this canister,
/// along with their registries and the `db()` accessor.
///
fn stores(builder: &ActorBuilder) -> TokenStream {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut data_inits = quote!();
    let mut index_inits = quote!();

    for (store_path, store) in builder.get_stores() {
        let cell_ident = format_ident!("{}", &store.ident);
        let memory_id = store.memory_id;
        let store_path_lit = store_path;

        if matches!(store.ty, StoreType::Index) {
            // Index store
            index_defs.extend(quote! {
                ::icydb::__reexports::canic_memory::eager_static! {
                    static #cell_ident: ::std::cell::RefCell<::icydb::__internal::core::db::store::IndexStore> =
                        ::std::cell::RefCell::new(::icydb::__internal::core::db::store::IndexStore::init(
                            ::icydb::__reexports::canic_memory::ic_memory!(::icydb::__internal::core::db::store::IndexStore, #memory_id)
                        ));
                }
            });

            index_inits.extend(quote! {
                reg.register(#store_path_lit, &#cell_ident);
            });
        } else {
            // Data store
            data_defs.extend(quote! {
                ::icydb::__reexports::canic_memory::eager_static! {
                    static #cell_ident: ::std::cell::RefCell<::icydb::__internal::core::db::store::DataStore> =
                        ::std::cell::RefCell::new(::icydb::__internal::core::db::store::DataStore::init(
                            ::icydb::__reexports::canic_memory::ic_memory!(::icydb::__internal::core::db::store::DataStore, #memory_id)
                        ));
                }
            });

            data_inits.extend(quote! {
                reg.register(#store_path_lit, &#cell_ident);
            });
        }
    }

    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def.path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", builder.canister.def.path()));
    let memory_min = canister.memory_min;
    let memory_max = canister.memory_max;

    quote! {
        #data_defs
        #index_defs

        thread_local! {
            // registries
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static DATA_REGISTRY: ::icydb::__internal::core::db::store::DataStoreRegistry = {
                let mut reg = ::icydb::__internal::core::db::store::DataStoreRegistry::new();
                #data_inits
                reg
            };

            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static INDEX_REGISTRY: ::icydb::__internal::core::db::store::IndexStoreRegistry = {
                let mut reg = ::icydb::__internal::core::db::store::IndexStoreRegistry::new();
                #index_inits
                reg
            };
        }

        static DB: ::icydb::__internal::core::db::Db<#canister_path> =
            ::icydb::__internal::core::db::Db::<#canister_path>::new(&DATA_REGISTRY, &INDEX_REGISTRY);

        // reserve the ic memory range
        ::icydb::__reexports::canic_memory::eager_init!({
            ::icydb::__reexports::canic_memory::ic_memory_range!(#memory_min, #memory_max);
        });

        /// Global accessor (fat handle) for this canister’s DB.
        /// This is the **only** API applications should use.
        #[must_use]
        pub const fn db() -> ::icydb::db::DbSession<#canister_path> {
            ::icydb::__internal::db_session(DB)
        }

        /// Internal raw DB access for tests and invariant-breaking tooling.
        /// Not part of the public API.
        #[doc(hidden)]
        pub(crate) const fn db_core() -> ::icydb::__internal::core::db::DbSession<#canister_path> {
            ::icydb::__internal::core::db::DbSession::new(DB)
        }

        /// Internal raw DB handle for tests and invariant-breaking tooling.
        /// Not part of the public API.
        #[doc(hidden)]
        #[expect(dead_code)]
        pub(crate) const fn db_raw() -> ::icydb::__internal::core::db::Db<#canister_path> {
            DB
        }
    }
}
