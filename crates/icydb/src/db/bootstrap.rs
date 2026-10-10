//! Module: db::bootstrap
//!
//! Responsibility: generated-database memory-manager initialization and typed failure.
//! Does not own: memory allocation policy or generated store initialization.
//! Boundary: ensures that the shared default runtime exists and contains this
//! database's declarations, then preserves any `ic-memory` cause until an
//! interface chooses a compact public error projection.

use crate::db::{MemoryBootstrapAdmissionError, prepare_memory_bootstrap};
use std::{fmt, sync::Arc};

use ic_memory::{
    AllocationPolicy, BootstrapAdmission, MemoryAllocationPool, MemoryAllocationPoolError,
    MemoryManagerConfig, MemoryManagerSlot, MemoryResolutionError, PolicyIdentity,
    PolicyIdentityError, RuntimeAdoptionError, RuntimeBootstrapError, RuntimeBootstrapPolicy,
    RuntimeStateError, StableKey, bootstrap_default_memory_manager_with_config,
    is_default_memory_manager_bootstrapped, sealed_declaration_snapshot,
    verify_default_memory_manager_authority,
};

/// Ensure that the default memory manager contains one database authority.
///
/// This bootstraps an uninitialized runtime and otherwise adopts its committed
/// allocations without reasserting a bootstrap policy. Adoption succeeds only
/// when every declaration registered by this generated database authority
/// appears exactly in the committed allocation capability.
/// The supplied bucket size governs only IcyDB-owned bootstrap; an existing
/// committed host runtime owns its policy and bucket size. Unbootstrapped
/// persisted memory must match the requested size, without resizing or fallback.
/// Cold bootstrap obtains the explicit final host pool from `pool`; adoption
/// never invokes that provider or reasserts its grants and exclusions.
///
/// # Panics
/// Panics if the caller's pool provider panics during cold bootstrap.
#[doc(hidden)]
pub fn ensure_default_memory_manager(
    authority: &str,
    bucket_size_pages: u16,
    pool: impl FnOnce() -> Result<MemoryAllocationPool, MemoryAllocationPoolError>,
) -> Result<(), DatabaseBootstrapError> {
    if !is_default_memory_manager_bootstrapped()
        .map_err(RuntimeBootstrapError::<MemoryBootstrapAdmissionError>::State)?
    {
        let config = MemoryManagerConfig::new(bucket_size_pages)
            .map_err(RuntimeStateError::Construction)
            .map_err(RuntimeBootstrapError::<MemoryBootstrapAdmissionError>::State)?;
        bootstrap_default_memory_manager_with_config(config, &pool()?, &DatabaseMemoryPolicy)?;
    }

    let snapshot = sealed_declaration_snapshot()
        .map_err(RuntimeBootstrapError::<MemoryBootstrapAdmissionError>::Registry)?;
    verify_default_memory_manager_authority(&snapshot, authority)?;
    Ok(())
}

/// Standalone policy uses the same admission function required of composed hosts.
struct DatabaseMemoryPolicy;

impl AllocationPolicy for DatabaseMemoryPolicy {
    type Error = MemoryBootstrapAdmissionError;

    fn validate_key(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }
    fn validate_slot(&self, _: &StableKey, _: &MemoryManagerSlot) -> Result<(), Self::Error> {
        Ok(())
    }
    fn validate_reserved_slot(
        &self,
        _: &StableKey,
        _: &MemoryManagerSlot,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl RuntimeBootstrapPolicy for DatabaseMemoryPolicy {
    fn prepare_bootstrap(&self, admission: &mut BootstrapAdmission<'_>) -> Result<(), Self::Error> {
        prepare_memory_bootstrap(admission)
    }
    fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
        PolicyIdentity::new("icydb.logical_memory", 1)
    }
}

/// Failure to initialize the generated database's stable-memory authority.
///
/// Cloning this error is cheap and preserves the original typed `ic-memory`
/// bootstrap or adoption cause cached by generated database wiring.
#[derive(Clone, Debug)]
pub enum DatabaseBootstrapError {
    /// Cold bootstrap failed before allocation authority could be published.
    Bootstrap(Arc<RuntimeBootstrapError<MemoryBootstrapAdmissionError>>),
    /// Current host allocations do not satisfy this database's declared authority.
    Adoption(Arc<RuntimeAdoptionError>),
}

impl From<RuntimeAdoptionError> for DatabaseBootstrapError {
    fn from(source: RuntimeAdoptionError) -> Self {
        Self::Adoption(Arc::new(source))
    }
}

impl From<MemoryAllocationPoolError> for DatabaseBootstrapError {
    fn from(source: MemoryAllocationPoolError) -> Self {
        RuntimeBootstrapError::Resolution(MemoryResolutionError::Pool(source)).into()
    }
}

impl From<RuntimeBootstrapError<MemoryBootstrapAdmissionError>> for DatabaseBootstrapError {
    fn from(source: RuntimeBootstrapError<MemoryBootstrapAdmissionError>) -> Self {
        Self::Bootstrap(Arc::new(source))
    }
}

impl fmt::Display for DatabaseBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap(source) => source.fmt(f),
            Self::Adoption(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for DatabaseBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bootstrap(source) => Some(source.as_ref()),
            Self::Adoption(source) => Some(source.as_ref()),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    mod public_failures;
    use super::*;
    use ic_memory::{
        AllocationPolicy, MemoryManagerSlot, MemoryRequest, PolicyIdentity, PolicyIdentityError,
        RuntimeBootstrapPolicy, RuntimeOpenError, SchemaMetadata, StableKey,
        bootstrap_default_memory_manager, committed_allocations,
        default_memory_manager_memory_allocation_summary, register_memory_request,
    };

    const TEST_AUTHORITY: &str = "icydb.bootstrap_adoption_test";
    fn pool() -> Result<MemoryAllocationPool, MemoryAllocationPoolError> {
        MemoryAllocationPool::new(
            vec![ic_memory::MemoryAuthority::new(
                TEST_AUTHORITY,
                "icydb.bootstrap_adoption_test.",
            )?],
            vec![],
        )
    }

    struct ExistingRuntimePolicy {
        identity_name: &'static str,
        preparation_calls: std::cell::Cell<usize>,
    }

    impl AllocationPolicy for ExistingRuntimePolicy {
        type Error = MemoryBootstrapAdmissionError;

        fn validate_key(&self, _key: &StableKey) -> Result<(), Self::Error> {
            Ok(())
        }

        fn validate_slot(
            &self,
            _key: &StableKey,
            _slot: &MemoryManagerSlot,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn validate_reserved_slot(
            &self,
            _key: &StableKey,
            _slot: &MemoryManagerSlot,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    impl RuntimeBootstrapPolicy for ExistingRuntimePolicy {
        fn prepare_bootstrap(
            &self,
            admission: &mut ic_memory::BootstrapAdmission<'_>,
        ) -> Result<(), Self::Error> {
            self.preparation_calls.set(self.preparation_calls.get() + 1);
            prepare_memory_bootstrap(admission)
        }

        fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
            PolicyIdentity::new(self.identity_name, 1)
        }
    }

    fn register_test_authority() {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        REGISTER.call_once(|| {
            for role in ["commit.control", "startup.control", "integrity.progress"] {
                register_memory_request(
                    MemoryRequest::new(
                        TEST_AUTHORITY,
                        &format!("{TEST_AUTHORITY}.{role}.v1"),
                        SchemaMetadata::default(),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
        });
    }

    #[test]
    fn configured_bootstrap_selects_fresh_buckets_and_is_idempotent() {
        register_test_authority();
        for pages in [4, 16, 128] {
            std::thread::spawn(move || {
                assert!(matches!(
                    committed_allocations(),
                    Err(RuntimeOpenError::NotBootstrapped)
                ));
                assert!(matches!(
                    ic_memory::open_default_memory_manager_memory(
                        "icydb.bootstrap_adoption_test.commit.control.v1"
                    ),
                    Err(RuntimeOpenError::NotBootstrapped)
                ));
                assert!(matches!(
                    default_memory_manager_memory_allocation_summary(),
                    Err(ic_memory::RuntimeDiagnosticError::NotBootstrapped)
                ));
                for observation in [
                    ic_memory::default_memory_manager_diagnostic_export().map(|_| ()),
                    ic_memory::default_memory_manager_commit_recovery_diagnostic().map(|_| ()),
                    ic_memory::default_memory_manager_doctor_report(&pool().unwrap()).map(|_| ()),
                    ic_memory::default_memory_manager_doctor_report_with_policy(
                        &pool().unwrap(),
                        &ic_memory::GenericAllocationPolicy,
                    )
                    .map(|_| ()),
                ] {
                    assert!(matches!(
                        observation,
                        Err(ic_memory::RuntimeDiagnosticError::NotBootstrapped)
                    ));
                }
                ensure_default_memory_manager(TEST_AUTHORITY, pages, pool).unwrap();
                let committed = committed_allocations().unwrap();
                let before = default_memory_manager_memory_allocation_summary().unwrap();
                assert_eq!(before.bucket_size_pages, pages);
                ensure_default_memory_manager(TEST_AUTHORITY, pages, pool).unwrap();
                assert_eq!(committed_allocations().unwrap(), committed);
                assert_eq!(
                    default_memory_manager_memory_allocation_summary().unwrap(),
                    before
                );
            })
            .join()
            .unwrap();
        }
    }

    #[test]
    fn conflicting_unbootstrapped_layout_rejects_without_changing_allocation() {
        register_test_authority();
        std::thread::spawn(|| {
            // A rejected bootstrap selects a layout without publishing authority.
            let policy = ExistingRuntimePolicy {
                identity_name: "",
                preparation_calls: std::cell::Cell::new(0),
            };
            assert!(matches!(
                bootstrap_default_memory_manager_with_config(
                    MemoryManagerConfig::new(128).unwrap(),
                    &pool().unwrap(),
                    &policy,
                ),
                Err(RuntimeBootstrapError::PolicyIdentity(
                    PolicyIdentityError::EmptyName
                ))
            ));
            let before = default_memory_manager_memory_allocation_summary().unwrap();
            assert_eq!(before.bucket_size_pages, 128);
            let error = ensure_default_memory_manager(TEST_AUTHORITY, 16, pool).unwrap_err();
            assert!(matches!(
                &error,
                DatabaseBootstrapError::Bootstrap(cause) if matches!(cause.as_ref(),
                    RuntimeBootstrapError::State(RuntimeStateError::Construction(
                        ic_memory::RuntimeConstructionError::BucketSizeMismatch {
                            persisted: 128, requested: 16,
                        }
                    ))
                )
            ));
            let public = crate::db::startup::__startup_bootstrap_failure(error);
            assert_eq!(
                public.error().code(),
                icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_BUCKET_SIZE_MISMATCH,
            );
            assert_eq!(
                public.error().core_facts().unwrap(),
                vec![
                    (icydb_diagnostic_code::DiagnosticFactTag::Expected, 16),
                    (icydb_diagnostic_code::DiagnosticFactTag::Actual, 128),
                ],
            );
            assert_eq!(
                default_memory_manager_memory_allocation_summary().unwrap(),
                before
            );
            assert!(matches!(
                committed_allocations(),
                Err(RuntimeOpenError::NotBootstrapped)
            ));
            ensure_default_memory_manager(TEST_AUTHORITY, 128, pool).unwrap();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn unknown_host_authority_rejects_without_changing_committed_allocations() {
        register_test_authority();
        ensure_default_memory_manager(TEST_AUTHORITY, 16, pool).unwrap();
        let committed = committed_allocations().unwrap();
        let before = default_memory_manager_memory_allocation_summary().unwrap();
        let error = ensure_default_memory_manager("icydb.unknown", 4, pool).unwrap_err();
        let public = crate::Error::from(error.clone());
        assert_eq!(
            public.code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH
        );
        assert!(public.facts().is_empty());
        assert_eq!(
            crate::db::__startup_bootstrap_failure(error.clone()).error(),
            &public
        );
        assert!(matches!(error, DatabaseBootstrapError::Adoption(cause)
            if matches!(cause.as_ref(), RuntimeAdoptionError::UnknownAuthority { authority }
                if authority == "icydb.unknown")));
        assert_eq!(committed_allocations().unwrap(), committed);
        assert_eq!(
            default_memory_manager_memory_allocation_summary().unwrap(),
            before
        );
        ensure_default_memory_manager(TEST_AUTHORITY, 4, pool).unwrap();
    }

    #[test]
    fn adopts_runtime_bootstrapped_by_a_different_policy_and_bucket_size() {
        register_test_authority();
        let policy = ExistingRuntimePolicy {
            identity_name: "tests.existing-runtime-policy",
            preparation_calls: std::cell::Cell::new(0),
        };

        let upstream = bootstrap_default_memory_manager_with_config(
            MemoryManagerConfig::new(16).expect("test bucket size should admit"),
            &pool().unwrap(),
            &policy,
        )
        .expect("existing policy identity should bootstrap the shared runtime");
        let generation = upstream.generation();
        assert_eq!(policy.preparation_calls.get(), 1);

        ensure_default_memory_manager(TEST_AUTHORITY, 4, || {
            panic!("adoption must not invoke pool provider")
        })
        .expect("IcyDB should adopt the upstream committed capability");
        ensure_default_memory_manager(TEST_AUTHORITY, 4, pool)
            .expect("repeated adoption should not prepare the host again");
        assert_eq!(policy.preparation_calls.get(), 1);

        assert_eq!(
            committed_allocations()
                .expect("adopted allocations should remain available")
                .generation(),
            generation,
        );
        assert_eq!(
            default_memory_manager_memory_allocation_summary()
                .expect("adopted runtime should report its persisted bucket size")
                .bucket_size_pages,
            16,
        );
        assert!(matches!(
            bootstrap_default_memory_manager(&pool().unwrap()),
            Err(RuntimeBootstrapError::PolicyIdentityMismatch { .. })
        ));
    }
}
