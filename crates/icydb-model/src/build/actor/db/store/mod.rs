//! Module: db::store
//! Responsibility: generated store memory declarations and session accessors.
//! Does not own: store runtime behavior, memory manager implementation, or schema authority.
//! Boundary: translates validated store metadata into actor-local storage wiring tokens.

use crate::build::actor::ActorBuilder;
use crate::node::{Store, StoreHeapConfig, StoreStorage};
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

///
/// StoreRegistryTokens
///
/// Generated token bundle for all store-memory definitions and registration
/// statements emitted for one actor. It keeps store wiring helpers below the
/// argument limit while preserving the generated-code phase boundary.
///

struct StoreRegistryTokens {
    memory_authority: String,
    journal_defs: TokenStream,
    stable_keys: Vec<String>,
    data_defs: TokenStream,
    index_defs: TokenStream,
    schema_defs: TokenStream,
    store_inits: TokenStream,
}

/// Validated canister-owned memory declarations emitted around store wiring.
struct CanisterMemoryWiring<'a> {
    commit: &'a str,
    startup: &'a str,
    integrity_progress: &'a str,
}

/// Emit generated stores, optional frontends, and session accessors.
pub(super) fn generate_store_wiring(
    builder: &ActorBuilder,
    frontend_surfaces: &TokenStream,
) -> TokenStream {
    let icydb = &builder.icydb;
    let canister = &builder.canister;
    let memory_namespace = canister.memory_namespace();
    let memory_authority = format!("icydb_{memory_namespace}");
    let store_registry = store_registry_tokens(builder, memory_namespace, &memory_authority);
    let commit_stable_key = canister.commit_stable_key();
    let startup_stable_key = canister.startup_stable_key();
    let integrity_progress_stable_key = canister.integrity_progress_stable_key();
    let schema_bootstrap = schema_bootstrap_tokens(builder);

    store_wiring_tokens(
        icydb,
        store_registry,
        frontend_surfaces,
        &schema_bootstrap,
        &CanisterMemoryWiring {
            commit: &commit_stable_key,
            startup: &startup_stable_key,
            integrity_progress: &integrity_progress_stable_key,
        },
    )
}

fn schema_bootstrap_tokens(builder: &ActorBuilder) -> TokenStream {
    let icydb = &builder.icydb;
    let fragment_bytes = &builder.schema_fragment_bytes;
    let migration_capability = builder.schema_migration_plan_bytes.as_ref().map_or_else(
        TokenStream::new,
        |_| quote!(#icydb::__icydb_require_migration_capability!();),
    );
    let migration_bytes = builder.schema_migration_plan_bytes.as_ref().map_or_else(
        || quote!(::std::option::Option::None),
        |bytes| quote!(::std::option::Option::Some(&[#(#bytes),*])),
    );
    let apply_generated_schema = if builder.schema_migration_plan_bytes.is_some() {
        quote! {
            session.ensure_generated_schema_fragment(
                ICYDB_SCHEMA_FRAGMENT,
                ICYDB_SCHEMA_MIGRATION_PLAN,
                ICYDB_SCHEMA_SUBMISSION_KEY,
                ICYDB_SCHEMA_ENTITY_STORES,
            )
        }
    } else {
        quote! {
            session
                .apply_generated_schema_fragment(
                    ICYDB_SCHEMA_FRAGMENT,
                    ICYDB_SCHEMA_MIGRATION_PLAN,
                    ICYDB_SCHEMA_SUBMISSION_KEY,
                    ICYDB_SCHEMA_ENTITY_STORES,
                )
                .map(|_receipt| ())
        }
    };
    let submission_key = &builder.schema_submission_key;
    let entity_stores = builder
        .get_entities()
        .into_iter()
        .map(|(_, entity)| (entity.name().to_owned(), entity.store().to_owned()))
        .collect::<Vec<_>>();
    let entity_names = entity_stores.iter().map(|(name, _)| name);
    let store_paths = entity_stores.iter().map(|(_, store)| store);
    let startup_driver = startup_driver_tokens(icydb);

    quote! {
        #migration_capability
        pub(super) const ICYDB_SCHEMA_FRAGMENT: &[u8] = &[#(#fragment_bytes),*];
        pub(super) const ICYDB_SCHEMA_MIGRATION_PLAN: ::std::option::Option<&[u8]> = #migration_bytes;
        pub(super) const ICYDB_SCHEMA_SUBMISSION_KEY: &str = #submission_key;
        pub(super) const ICYDB_SCHEMA_ENTITY_STORES: &[(&str, &str)] = &[
            #((#entity_names, #store_paths)),*
        ];

        fn apply_generated_schema(
            session: &#icydb::db::DbSession<__IcydbGeneratedCanister>,
        ) -> ::std::result::Result<(), #icydb::Error> {
            #apply_generated_schema
        }

        #startup_driver

        fn admit_ordinary_database_work(
        ) -> ::std::result::Result<(), #icydb::Error> {
            match startup_state() {
                ::std::result::Result::Ok(#icydb::db::DatabaseStartupState::Ready) => ::std::result::Result::Ok(()),
                ::std::result::Result::Ok(#icydb::db::DatabaseStartupState::Recovering) => {
                    ::std::result::Result::Err(#icydb::db::__startup_recovery_pending())
                }
                ::std::result::Result::Err(failure) => ::std::result::Result::Err(failure.error().clone()),
            }
        }
    }
}

fn startup_driver_tokens(icydb: &TokenStream) -> TokenStream {
    quote! {
        pub(crate) fn startup_driver_attempt() -> ::std::result::Result<bool, #icydb::Error> {
            let session = #icydb::db::DbSession::new(core_db()?);
            match session.__drive_generated_startup_recovery_page(
                &STORE_REGISTRY,
                ICYDB_SCHEMA_SUBMISSION_KEY,
            )? {
                #icydb::db::GeneratedStartupDriverStep::Terminal => ::std::result::Result::Ok(true),
                #icydb::db::GeneratedStartupDriverStep::Recovering => ::std::result::Result::Ok(false),
                #icydb::db::GeneratedStartupDriverStep::ApplyGeneratedSchema => {
                    complete_generated_schema_handoff(&session)
                }
            }
        }

        /// Invoke the canonical generated startup driver from a local audit endpoint.
        #[cfg(feature = "test-admin-api")]
        #[doc(hidden)]
        pub(super) fn __icydb_startup_driver_attempt_for_tests(
        ) -> ::std::result::Result<bool, #icydb::Error> {
            startup_driver_attempt()
        }

        /// Drive startup and online convergence for a native libtest database.
        ///
        /// Native tests do not execute IC lifecycle hooks or timer callbacks.
        /// Call before the first database access and periodically between
        /// simulated messages in long-running write tests. This boundary drains
        /// the same bounded production driver, then checks ordinary admission.
        /// It does not reset the database or bypass convergence backpressure.
        #[cfg(all(test, not(target_arch = "wasm32")))]
        pub(crate) fn __drive_native_database_for_tests(
        ) -> ::std::result::Result<(), #icydb::Error> {
            #icydb::db::with_request_execution(|| {
                while !startup_driver_attempt()? {
                    // Keep production paging and request limits on both fresh
                    // startup and later convergence of accumulated writes.
                }
                admit_ordinary_database_work()
            })
        }

        fn complete_generated_schema_handoff(
            session: &#icydb::db::DbSession<__IcydbGeneratedCanister>,
        ) -> ::std::result::Result<bool, #icydb::Error> {
            if let ::std::result::Result::Err(error) = apply_generated_schema(session) {
                // Explicit controller commands own migration progress. Stop
                // without a receipt in that case; otherwise record failures
                // through the existing startup classifier and binding.
                if error.code() == #icydb::ErrorCode::SCHEMA_MIGRATION_IN_PROGRESS
                    || #icydb::db::__record_generated_schema_startup_failure::<
                    __IcydbGeneratedCanister,
                >(
                    &STORE_REGISTRY,
                    ICYDB_SCHEMA_SUBMISSION_KEY,
                    &error,
                )? {
                    return ::std::result::Result::Ok(true);
                }
                // Retryable failures use the adapter's logged cadence, not
                // the immediate successor reserved for successful progress.
                return ::std::result::Result::Err(error);
            }
            match startup_state() {
                ::std::result::Result::Ok(#icydb::db::DatabaseStartupState::Ready) => {
                    let _ = #icydb::db::__clear_generated_startup_failure::<
                        __IcydbGeneratedCanister,
                    >()?;
                    ::std::result::Result::Ok(false)
                }
                ::std::result::Result::Ok(#icydb::db::DatabaseStartupState::Recovering) => ::std::result::Result::Ok(false),
                ::std::result::Result::Err(_) => ::std::result::Result::Ok(true),
            }
        }

    }
}

fn store_registry_tokens(
    builder: &ActorBuilder,
    memory_namespace: &str,
    memory_authority: &str,
) -> StoreRegistryTokens {
    let icydb = &builder.icydb;
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut schema_defs = quote!();
    let mut journal_defs = quote!();
    let mut store_inits = quote!();
    let mut stable_keys = Vec::new();

    for (store_ordinal, (store_path, store)) in builder.get_stores().into_iter().enumerate() {
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                icydb,
                store_ordinal,
                &store_path,
                &store,
                memory_namespace,
            );
        if matches!(store.storage(), StoreStorage::Journaled(_)) {
            stable_keys.extend([
                store
                    .stable_data_allocation(memory_namespace)
                    .stable_key()
                    .to_owned(),
                store
                    .stable_index_allocation(memory_namespace)
                    .stable_key()
                    .to_owned(),
                store
                    .stable_schema_allocation(memory_namespace)
                    .stable_key()
                    .to_owned(),
                store
                    .journal_allocation(memory_namespace)
                    .stable_key()
                    .to_owned(),
            ]);
        }
        journal_defs.extend(journal_def);
        data_defs.extend(data_def);
        index_defs.extend(index_def);
        schema_defs.extend(schema_def);
        store_inits.extend(store_init);
    }

    StoreRegistryTokens {
        memory_authority: memory_authority.to_owned(),
        stable_keys,
        journal_defs,
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    }
}

/// Render one store registry entry into data/index/schema cells plus registration.
fn store_registry_entry_tokens(
    icydb: &TokenStream,
    store_ordinal: usize,
    store_path: &str,
    store: &Store,
    memory_namespace: &str,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    match store.storage() {
        StoreStorage::Heap(config) => {
            heap_store_registry_entry_tokens(icydb, store_ordinal, store_path, *config)
        }
        StoreStorage::Journaled(_) => journaled_store_registry_entry_tokens(
            icydb,
            store_ordinal,
            store_path,
            store,
            memory_namespace,
        ),
    }
}

fn store_cell_ident(store_ordinal: usize, role: &str) -> Ident {
    format_ident!("__ICYDB_STORE_{store_ordinal}_{role}")
}

fn store_cell_tokens(
    icydb: &TokenStream,
    cell_ident: &Ident,
    store_ty: &TokenStream,
    stable_key: &str,
    initializer: &Ident,
) -> TokenStream {
    quote! {
        ::std::thread_local! {
            static #cell_ident: ::std::cell::RefCell<
                #store_ty
            > = ::std::cell::RefCell::new(
                #store_ty::#initializer(
                    #icydb::__macro::open_memory(#stable_key)
                    .expect(::std::concat!(
                        "ic-storage committed allocation unavailable: ",
                        #stable_key
                    ))
                ).expect("ic-storage collection initialization failed")
            );
        }
    }
}

// Heap roles share const initialization but never acquire stable-memory handles.
fn heap_store_cell_tokens(cell: &Ident, store_type: &TokenStream) -> TokenStream {
    quote! {
        ::std::thread_local! {
            static #cell: ::std::cell::RefCell<#store_type> = const {
                ::std::cell::RefCell::new(#store_type::init_heap())
            };
        }
    }
}

/// Render one volatile heap store registry entry into data/index/schema cells plus registration.
fn heap_store_registry_entry_tokens(
    icydb: &TokenStream,
    store_ordinal: usize,
    store_path: &str,
    _heap: StoreHeapConfig,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    let data_cell_ident = store_cell_ident(store_ordinal, "DATA");
    let index_cell_ident = store_cell_ident(store_ordinal, "INDEX");
    let schema_cell_ident = store_cell_ident(store_ordinal, "SCHEMA");

    let data_def = heap_store_cell_tokens(&data_cell_ident, &quote!(#icydb::__macro::DataStore));
    let index_def = heap_store_cell_tokens(&index_cell_ident, &quote!(#icydb::__macro::IndexStore));
    let schema_def =
        heap_store_cell_tokens(&schema_cell_ident, &quote!(#icydb::__macro::SchemaStore));
    let store_init = quote! {
        reg.register_store(
            #store_path,
            &#data_cell_ident,
            &#index_cell_ident,
            &#schema_cell_ident,
            #icydb::__macro::StoreAllocationIdentities::absent(),
            #icydb::__macro::StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("store registration should succeed");
    };

    (quote!(), data_def, index_def, schema_def, store_init)
}

/// Render one journaled cached-stable store registry entry into canonical
/// stable data/index/schema cells, a journal-tail declaration, and registration.
fn journaled_store_registry_entry_tokens(
    icydb: &TokenStream,
    store_ordinal: usize,
    store_path: &str,
    store: &Store,
    memory_namespace: &str,
) -> (
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
    TokenStream,
) {
    let data_cell_ident = store_cell_ident(store_ordinal, "DATA");
    let index_cell_ident = store_cell_ident(store_ordinal, "INDEX");
    let schema_cell_ident = store_cell_ident(store_ordinal, "SCHEMA");
    let data_allocation = store.stable_data_allocation(memory_namespace);
    let index_allocation = store.stable_index_allocation(memory_namespace);
    let schema_allocation = store.stable_schema_allocation(memory_namespace);
    let journal_allocation = store.journal_allocation(memory_namespace);
    let data_stable_key = data_allocation.stable_key();
    let index_stable_key = index_allocation.stable_key();
    let schema_stable_key = schema_allocation.stable_key();
    let journal_stable_key = journal_allocation.stable_key();

    let journal_cell_ident = store_cell_ident(store_ordinal, "JOURNAL");
    let journal_def = store_cell_tokens(
        icydb,
        &journal_cell_ident,
        &quote!(#icydb::__macro::JournalTailStore),
        journal_stable_key,
        &format_ident!("init"),
    );
    let data_def = store_cell_tokens(
        icydb,
        &data_cell_ident,
        &quote!(#icydb::__macro::DataStore),
        data_stable_key,
        &format_ident!("init_journaled"),
    );
    let index_def = store_cell_tokens(
        icydb,
        &index_cell_ident,
        &quote!(#icydb::__macro::IndexStore),
        index_stable_key,
        &format_ident!("init_journaled"),
    );
    let schema_def = store_cell_tokens(
        icydb,
        &schema_cell_ident,
        &quote!(#icydb::__macro::SchemaStore),
        schema_stable_key,
        &format_ident!("init_journaled"),
    );
    let store_init = quote! {
        reg.register_journaled_store(
            #store_path,
            &#data_cell_ident,
            &#index_cell_ident,
            &#schema_cell_ident,
            &#journal_cell_ident,
            #icydb::__macro::StoreAllocationIdentities::new_journaled(
                #icydb::__macro::StoreAllocationIdentity::from_committed_key(
                    #data_stable_key,
                ).expect("validated store allocation must be committed"),
                #icydb::__macro::StoreAllocationIdentity::from_committed_key(
                    #index_stable_key,
                ).expect("validated store allocation must be committed"),
                #icydb::__macro::StoreAllocationIdentity::from_committed_key(
                    #schema_stable_key,
                ).expect("validated store allocation must be committed"),
                #icydb::__macro::StoreAllocationIdentity::from_committed_key(
                    #journal_stable_key,
                ).expect("validated store allocation must be committed"),
            ),
            #icydb::__macro::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("store registration should succeed");
    };

    (journal_def, data_def, index_def, schema_def, store_init)
}

/// Assemble the outer canister store wiring around the generated registry.
fn store_wiring_tokens(
    icydb: &TokenStream,
    store_registry: StoreRegistryTokens,
    frontend_surfaces: &TokenStream,
    schema_bootstrap: &TokenStream,
    memory: &CanisterMemoryWiring<'_>,
) -> TokenStream {
    let StoreRegistryTokens {
        memory_authority,
        stable_keys,
        journal_defs,
        data_defs,
        index_defs,
        schema_defs,
        store_inits,
    } = store_registry;
    let CanisterMemoryWiring {
        commit: commit_stable_key,
        startup: startup_stable_key,
        integrity_progress: integrity_progress_stable_key,
    } = memory;
    let store_registry_init = if store_inits.is_empty() {
        quote! {
            #icydb::__macro::StoreRegistry::new()
        }
    } else {
        quote! {
            {
                let mut reg =
                    #icydb::__macro::StoreRegistry::new();
                #store_inits
                reg
            }
        }
    };
    let startup_observation = startup_observation_tokens(icydb);

    quote! {
        #[doc(hidden)]
        pub struct __IcydbGeneratedCanister;

        impl #icydb::__macro::Path for __IcydbGeneratedCanister {
            const PATH: &'static str = "__icydb_generated_canister";
        }

        impl #icydb::__macro::CanisterKind for __IcydbGeneratedCanister {
            const COMMIT_STABLE_KEY: &'static str = #commit_stable_key;
            const STARTUP_STABLE_KEY: &'static str = #startup_stable_key;
            const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
                #integrity_progress_stable_key;

            fn generated_schema_application_record_key() -> ::std::result::Result<
                ::std::option::Option<[u8; 32]>,
                #icydb::__macro::InternalError,
            > {
                #icydb::__macro::generated_schema_application_record_key(
                    ICYDB_SCHEMA_SUBMISSION_KEY,
                ).map(::std::option::Option::Some)
            }
        }

        /// Complete database memory requirements for host composition, without opening stores.
        pub fn icydb_memory_requests() -> ::std::result::Result<
            ::std::vec::Vec<#icydb::db::MemoryRequest>,
            #icydb::db::AdmissionError,
        > {
            [#commit_stable_key, #integrity_progress_stable_key, #startup_stable_key,
                #(#stable_keys),*]
                .into_iter()
                .map(|key| #icydb::db::MemoryRequest::new(
                    #memory_authority, key
                ))
                .collect()
        }

        #journal_defs
        ::std::thread_local! {
            static MEMORY_BOOTSTRAP:
                ::std::cell::OnceCell<
                    ::std::result::Result<
                        (),
                        #icydb::db::DatabaseBootstrapError
                    >
                > =
                    const { ::std::cell::OnceCell::new() };
        }

        #[cfg(all(test, not(target_arch = "wasm32")))]
        const fn native_test_startup_recovery_wakeup() {}

        fn ensure_memory_bootstrap() ->
            ::std::result::Result<(), #icydb::db::DatabaseBootstrapError>
        {
            #[cfg(all(test, not(target_arch = "wasm32")))]
            #icydb::db::__install_startup_recovery_wakeup(
                native_test_startup_recovery_wakeup,
            );
            #[cfg(not(all(test, not(target_arch = "wasm32"))))]
            #icydb::db::__install_startup_recovery_wakeup(
                crate::__icydb_host::ensure_startup_watchdog_scheduled_immediately,
            );
            MEMORY_BOOTSTRAP.with(|bootstrap| {
                bootstrap
                    .get_or_init(|| {
                        // Construct and validate once at the existing bootstrap owner.
                        let requirements = icydb_memory_requests()
                            .and_then(#icydb::db::SealedDeclarationSnapshot::new)
                            .map_err(#icydb::db::DatabaseBootstrapError::from)?;
                        #icydb::__macro::ensure_default_storage_runtime(
                            #memory_authority,
                            &requirements,
                             #icydb::db::StorageConfig::default(),
                            crate::icydb_storage_grants,
                        )
                    })
                    .clone()
            })
        }

        #data_defs
        #index_defs
        #schema_defs
        #frontend_surfaces
        #schema_bootstrap
        ::std::thread_local! {
            static STORE_REGISTRY:
                #icydb::__macro::StoreRegistry =
                #store_registry_init;
        }

        #[doc(hidden)]
        pub fn core_db() -> ::std::result::Result<
            #icydb::__macro::CoreDbSession<__IcydbGeneratedCanister>,
            #icydb::Error,
        > {
            ensure_memory_bootstrap()?;

            #icydb::__macro::CoreDbSession::<__IcydbGeneratedCanister>::__new_from_current_request(
                &STORE_REGISTRY,
            )
            .ok_or_else(#icydb::db::__request_execution_scope_required)
        }

        #startup_observation

        #[doc(hidden)]
        pub fn core_db_with_request_root(
            request_root: &#icydb::db::RequestExecutionRoot,
        ) -> ::std::result::Result<
            #icydb::__macro::CoreDbSession<__IcydbGeneratedCanister>,
            #icydb::db::DatabaseBootstrapError,
        > {
            ensure_memory_bootstrap()?;

            ::std::result::Result::Ok(#icydb::__macro::CoreDbSession::<__IcydbGeneratedCanister>::new(
                &STORE_REGISTRY,
                request_root.__core(),
            ))
        }

        pub fn db() -> ::std::result::Result<
            #icydb::db::DbSession<__IcydbGeneratedCanister>,
            #icydb::Error,
        > {
            admit_ordinary_database_work()?;
            let session = #icydb::db::DbSession::new(core_db()?);
            ::std::result::Result::Ok(session)
        }

        #[doc(hidden)]
        pub fn db_with_request_root(
            request_root: &#icydb::db::RequestExecutionRoot,
        ) -> ::std::result::Result<
            #icydb::db::DbSession<__IcydbGeneratedCanister>,
            #icydb::Error,
        > {
            request_root.__ensure_compatible_with_current()?;
            admit_ordinary_database_work()?;
            let session = #icydb::db::DbSession::new(core_db_with_request_root(request_root)?);
            ::std::result::Result::Ok(session)
        }
    }
}

fn startup_observation_tokens(icydb: &TokenStream) -> TokenStream {
    quote! {
        pub fn startup_state() -> ::std::result::Result<
            #icydb::db::DatabaseStartupState,
            #icydb::db::StartupFailure,
        > {
            ensure_memory_bootstrap()
                .map_err(#icydb::db::__startup_bootstrap_failure)?;
            #icydb::db::__observe_generated_startup_state::<__IcydbGeneratedCanister>(
                &STORE_REGISTRY,
                ICYDB_SCHEMA_SUBMISSION_KEY,
            )
        }
    }
}

#[cfg(test)]
mod tests;
