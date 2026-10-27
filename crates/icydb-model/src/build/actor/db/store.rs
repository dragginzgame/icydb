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
    bucket_size_pages: u16,
    commit_stable_key: &'a str,
    startup_stable_key: &'a str,
    integrity_progress_stable_key: &'a str,
}

/// Emit generated stores, optional frontends, and session accessors.
pub(super) fn generate_store_wiring(
    builder: &ActorBuilder,
    frontend_surfaces: TokenStream,
) -> TokenStream {
    let icydb = &builder.icydb;
    let canister = &builder.canister;
    let memory_namespace = canister.memory_namespace();
    let memory_authority = format!("icydb.{memory_namespace}");
    let store_registry = store_registry_tokens(builder, memory_namespace, &memory_authority);
    let commit_stable_key = canister.commit_stable_key();
    let startup_stable_key = canister.startup_stable_key();
    let integrity_progress_stable_key = canister.integrity_progress_stable_key();
    let schema_bootstrap = schema_bootstrap_tokens(builder);

    store_wiring_tokens(
        icydb,
        store_registry,
        frontend_surfaces,
        schema_bootstrap,
        CanisterMemoryWiring {
            bucket_size_pages: canister.memory_profile().bucket_size_pages(),
            commit_stable_key: &commit_stable_key,
            startup_stable_key: &startup_stable_key,
            integrity_progress_stable_key: &integrity_progress_stable_key,
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
    let migration_bytes = builder
        .schema_migration_plan_bytes
        .as_ref()
        .map_or_else(|| quote!(None), |bytes| quote!(Some(&[#(#bytes),*])));
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
                Ok(#icydb::db::DatabaseStartupState::Ready) => Ok(()),
                Ok(#icydb::db::DatabaseStartupState::Recovering) => {
                    Err(#icydb::db::__startup_recovery_pending())
                }
                Err(failure) => Err(failure.error().clone()),
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
                #icydb::db::GeneratedStartupDriverStep::Terminal => Ok(true),
                #icydb::db::GeneratedStartupDriverStep::Recovering => Ok(false),
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
            if let Err(error) = apply_generated_schema(session) {
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
                    return Ok(true);
                }
                // Retryable failures use the adapter's logged cadence, not
                // the immediate successor reserved for successful progress.
                return Err(error);
            }
            match startup_state() {
                Ok(#icydb::db::DatabaseStartupState::Ready) => {
                    let _ = #icydb::db::__clear_generated_startup_failure::<
                        __IcydbGeneratedCanister,
                    >()?;
                    Ok(false)
                }
                Ok(#icydb::db::DatabaseStartupState::Recovering) => Ok(false),
                Err(_) => Ok(true),
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
    store_ty: TokenStream,
    stable_key: &str,
    initializer: Ident,
) -> TokenStream {
    quote! {
        thread_local! {
            static #cell_ident: ::std::cell::RefCell<
                #store_ty
            > = ::std::cell::RefCell::new(
                #store_ty::#initializer(
                    #icydb::__macro::open_memory(#stable_key)
                    .expect(concat!(
                        "ic-memory committed allocation unavailable: ",
                        #stable_key
                    ))
                )
            );
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

    let data_def = quote! {
        thread_local! {
            static #data_cell_ident: ::std::cell::RefCell<
                #icydb::__macro::DataStore
            > = const { ::std::cell::RefCell::new(
                #icydb::__macro::DataStore::init_heap()
            ) };
        }
    };
    let index_def = quote! {
        thread_local! {
            static #index_cell_ident: ::std::cell::RefCell<
                #icydb::__macro::IndexStore
            > = const { ::std::cell::RefCell::new(
                #icydb::__macro::IndexStore::init_heap()
            ) };
        }
    };
    let schema_def = quote! {
        thread_local! {
            static #schema_cell_ident: ::std::cell::RefCell<
                #icydb::__macro::SchemaStore
            > = const { ::std::cell::RefCell::new(
                #icydb::__macro::SchemaStore::init_heap()
            ) };
        }
    };
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
        quote!(#icydb::__macro::JournalTailStore),
        journal_stable_key,
        format_ident!("init"),
    );
    let data_def = store_cell_tokens(
        icydb,
        &data_cell_ident,
        quote!(#icydb::__macro::DataStore),
        data_stable_key,
        format_ident!("init_journaled"),
    );
    let index_def = store_cell_tokens(
        icydb,
        &index_cell_ident,
        quote!(#icydb::__macro::IndexStore),
        index_stable_key,
        format_ident!("init_journaled"),
    );
    let schema_def = store_cell_tokens(
        icydb,
        &schema_cell_ident,
        quote!(#icydb::__macro::SchemaStore),
        schema_stable_key,
        format_ident!("init_journaled"),
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
#[expect(
    clippy::too_many_lines,
    reason = "one generated block keeps the store registry, memory declarations, and startup wiring visibly co-located"
)]
fn store_wiring_tokens(
    icydb: &TokenStream,
    store_registry: StoreRegistryTokens,
    frontend_surfaces: TokenStream,
    schema_bootstrap: TokenStream,
    memory: CanisterMemoryWiring<'_>,
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
        bucket_size_pages,
        commit_stable_key,
        startup_stable_key,
        integrity_progress_stable_key,
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
        }

        /// Complete database memory requirements for host composition, without opening stores.
        pub fn icydb_memory_requests() -> ::std::result::Result<
            ::std::vec::Vec<#icydb::db::MemoryRequest>,
            #icydb::db::MemoryDeclarationError,
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
        thread_local! {
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
                        #icydb::__macro::ensure_default_memory_manager(
                            #memory_authority,
                            &requirements,
                            #bucket_size_pages,
                            crate::icydb_memory_pool,
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
        thread_local! {
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

            Ok(#icydb::__macro::CoreDbSession::<__IcydbGeneratedCanister>::new(
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
            Ok(session)
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
            Ok(session)
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
mod tests {
    use super::*;
    use crate::node::StoreJournaledMemoryConfig;
    use crate::node::{Canister, CanisterMemoryProfile, Def, Schema};
    use std::sync::Arc;

    fn compact_tokens(tokens: TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    fn heap_store() -> Store {
        Store::new_heap(
            Def::new("demo::schema", "ScratchStore"),
            "demo::schema::DemoCanister",
            StoreHeapConfig::new(),
        )
    }

    fn journaled_store() -> Store {
        Store::new_journaled(
            Def::new("demo::schema", "JournaledStore"),
            "demo::schema::DemoCanister",
            StoreJournaledMemoryConfig::new("store_20"),
        )
    }

    fn actor_builder() -> ActorBuilder {
        ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", None),
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
            None,
            quote::quote!(::icydb),
        )
    }

    #[test]
    fn profile_changes_bootstrap_without_changing_schema_authority() {
        let baseline = actor_builder();
        for (profile, pages) in [
            (CanisterMemoryProfile::Compact, 4),
            (CanisterMemoryProfile::General, 16),
            (CanisterMemoryProfile::HighHeadroom, 128),
        ] {
            let builder = ActorBuilder::new(
                Arc::clone(&baseline.schema),
                baseline.canister.clone().with_memory_profile(profile),
                icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new()).unwrap(),
                None,
                quote::quote!(::icydb),
            );
            let rendered = compact_tokens(generate_store_wiring(&builder, quote!()));
            assert!(rendered.contains(&format!(
                "ensure_default_memory_manager(\"icydb.test\",&requirements,{pages}u16,crate::icydb_memory_pool,)"
            )));
            assert_eq!(
                builder.schema_fragment_bytes,
                baseline.schema_fragment_bytes
            );
            assert_eq!(
                builder.schema_submission_key,
                baseline.schema_submission_key
            );
        }
    }

    #[test]
    fn ordinary_admission_checks_startup_state() {
        let rendered = compact_tokens(schema_bootstrap_tokens(&actor_builder()));

        assert!(rendered.contains("fnadmit_ordinary_database_work("));
        assert!(rendered.contains("DatabaseStartupState::Ready"));
        assert!(rendered.contains("__startup_recovery_pending()"));
    }

    #[test]
    fn native_startup_driver_advances_bounded_database_work() {
        let rendered = compact_tokens(schema_bootstrap_tokens(&actor_builder()));
        for required in [
            "pub(crate)fnstartup_driver_attempt(",
            "__drive_generated_startup_recovery_page",
            "GeneratedStartupDriverStep::Terminal=>Ok(true)",
            "__icydb_startup_driver_attempt_for_tests",
            "__record_generated_schema_startup_failure",
            "__drive_native_database_for_tests",
            "while!startup_driver_attempt()?{}",
        ] {
            assert!(rendered.contains(required), "missing token: {required}");
        }
    }

    #[test]
    fn startup_observation_is_ungated_and_never_becomes_an_automatic_endpoint() {
        let rendered = compact_tokens(startup_observation_tokens(&quote!(::icydb)));

        assert!(rendered.contains("pubfnstartup_state()->::std::result::Result<"));
        assert!(rendered.contains("ensure_memory_bootstrap()"));
        assert!(rendered.contains("__observe_generated_startup_state::<"));
        for forbidden in [
            "core_db(",
            "db(",
            "with_request_execution",
            "set_timer",
            "ic_cdk::query",
            "ic_cdk::update",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "forbidden token: {forbidden}"
            );
        }
    }

    #[test]
    fn heap_store_wiring_uses_heap_initializers_and_absent_allocation_identity() {
        let store = heap_store();
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                &quote!(::icydb),
                0,
                "demo::schema::ScratchStore",
                &store,
                "demo",
            );
        let rendered = quote! {
            #journal_def
            #data_def
            #index_def
            #schema_def
            #store_init
        }
        .to_string();

        assert!(rendered.contains("DataStore :: init_heap"));
        assert!(rendered.contains("IndexStore :: init_heap"));
        assert!(rendered.contains("SchemaStore :: init_heap"));
        assert!(rendered.contains("StoreAllocationIdentities :: absent"));
        assert!(rendered.contains("StoreRuntimeStorageCapabilities :: heap"));
        assert!(rendered.contains("__ICYDB_STORE_0_DATA"));
        assert!(rendered.contains("__ICYDB_STORE_0_INDEX"));
        assert!(rendered.contains("__ICYDB_STORE_0_SCHEMA"));
        assert_eq!(rendered.matches("open_memory").count(), 0);
        assert_eq!(
            rendered
                .matches("StoreAllocationIdentity :: from_committed_key")
                .count(),
            0
        );
        assert!(!rendered.contains("ensure_memory_bootstrap"));
    }

    #[test]
    fn journaled_store_wiring_declares_journal_memory_and_registers_four_role_allocation() {
        let store = journaled_store();
        let (journal_def, data_def, index_def, schema_def, store_init) =
            store_registry_entry_tokens(
                &quote!(::icydb),
                0,
                "demo::schema::JournaledStore",
                &store,
                "demo",
            );
        let rendered = quote! {
            #journal_def
            #data_def
            #index_def
            #schema_def
            #store_init
        }
        .to_string();

        assert_eq!(rendered.matches("open_memory").count(), 4);
        assert_eq!(
            rendered
                .matches("ic-memory committed allocation unavailable:")
                .count(),
            4
        );
        assert_eq!(
            rendered
                .matches("StoreAllocationIdentity :: from_committed_key")
                .count(),
            4
        );
        assert!(rendered.contains("JournalTailStore :: init"));
        assert_eq!(rendered.matches("init_journaled").count(), 3);
        assert!(rendered.contains("register_journaled_store"));
        assert!(rendered.contains("StoreAllocationIdentities :: new_journaled"));
        assert!(rendered.contains("StoreRuntimeStorageCapabilities :: journaled"));
        for role in ["data", "index", "schema", "journal"] {
            assert!(rendered.contains(&format!("icydb.demo.store.store_20.{role}.v1")));
        }
        assert!(!rendered.contains("init_heap"));
    }

    #[test]
    fn store_registry_wiring_preserves_startup_admission() {
        let mut store_inits = quote!();
        store_inits.extend(
            store_registry_entry_tokens(
                &quote!(::icydb),
                0,
                "demo::schema::ScratchStore",
                &heap_store(),
                "demo",
            )
            .4,
        );
        let registry = StoreRegistryTokens {
            memory_authority: "icydb.demo".to_owned(),
            stable_keys: Vec::new(),
            journal_defs: quote!(),
            data_defs: quote!(),
            index_defs: quote!(),
            schema_defs: quote!(),
            store_inits,
        };

        let rendered = compact_tokens(store_wiring_tokens(
            &quote!(::icydb),
            registry,
            quote!(),
            quote!(),
            CanisterMemoryWiring {
                bucket_size_pages: 16,
                commit_stable_key: "icydb.demo.commit.control.v1",
                startup_stable_key: "icydb.demo.startup.control.v1",
                integrity_progress_stable_key: "icydb.demo.integrity.progress.v1",
            },
        ));

        assert!(rendered.contains("pubfnicydb_memory_requests()"));
        assert!(rendered.contains("\"icydb.demo.startup.control.v1\""));
        assert!(rendered.contains("\"icydb.demo.integrity.progress.v1\""));
        assert!(rendered.contains("Result<(),::icydb::db::DatabaseBootstrapError>"));
        assert!(
            rendered
                .contains("::icydb::__macro::ensure_default_memory_manager(\"icydb.demo\",&requirements,16u16,crate::icydb_memory_pool,)")
        );
        assert!(rendered.contains("ensure_memory_bootstrap()?"));
        assert!(rendered.contains("pubfnstartup_state()->::std::result::Result<"));
        assert!(rendered.contains("__observe_generated_startup_state::<__IcydbGeneratedCanister>"));
        assert!(rendered.contains("::std::cell::OnceCell"));
        assert!(rendered.contains("MEMORY_BOOTSTRAP.with("));
        assert!(rendered.contains("constfnnative_test_startup_recovery_wakeup(){}"));
        assert!(
            rendered.contains(
                "__install_startup_recovery_wakeup(native_test_startup_recovery_wakeup,)"
            )
        );
        assert!(rendered.contains(
            "__install_startup_recovery_wakeup(crate::__icydb_host::ensure_startup_watchdog_scheduled_immediately,)"
        ));
        assert!(rendered.contains(
            "CoreDbSession::<__IcydbGeneratedCanister>::__new_from_current_request(&STORE_REGISTRY,)"
        ));
        assert!(rendered.contains(
            "CoreDbSession::<__IcydbGeneratedCanister>::new(&STORE_REGISTRY,request_root.__core(),)"
        ));
        assert!(rendered.contains("pubfndb()->::std::result::Result<"));
        assert_eq!(
            rendered.matches("admit_ordinary_database_work()?").count(),
            2
        );
        let admission = rendered
            .find("admit_ordinary_database_work()?")
            .expect("ordinary db construction should contain startup admission");
        let session = rendered
            .find("DbSession::new(core_db()?)")
            .expect("ordinary db construction should create its session");
        assert!(
            admission < session,
            "admission must precede session construction"
        );
        assert!(rendered.contains(
            "pubfndb_with_request_root(request_root:&::icydb::db::RequestExecutionRoot,)"
        ));
        assert!(rendered.contains("request_root.__ensure_compatible_with_current()?"));
        assert!(!rendered.contains("panic!("));
    }
}
