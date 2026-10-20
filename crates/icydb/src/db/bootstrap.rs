//! Module: db::bootstrap
//!
//! Responsibility: generated-database memory-manager initialization and typed failure.
//! Does not own: memory allocation policy or generated store initialization.
//! Boundary: ensures that the bound host runtime exists and contains this
//! database's declarations, then preserves any `ic-memory` cause until an
//! interface chooses a compact public error projection.

use crate::db::{
    MemoryBootstrapAdmissionError, MemoryRuntimeBindingError, bind_memory_runtime, memory_runtime,
    prepare_memory_bootstrap,
};
use std::{fmt, rc::Rc, sync::Arc};

use ic_memory::{
    BootstrapAdmission, MemoryAllocationPool, MemoryAllocationPoolError, MemoryDeclarationError,
    MemoryManagerConfig, MemoryManagerSlot, MemoryResolutionError, MemoryRuntime, PolicyIdentity,
    RuntimeAdoptionError, RuntimeBootstrapError, RuntimeBootstrapPolicy, RuntimeConstructionError,
    SealedDeclarationSnapshot, StableKey,
};

use ic_memory::DefaultMemoryImpl;

/// Verify one database's declarations against the explicitly bound host runtime.
/// This never constructs backing memory or invokes a pool provider.
#[doc(hidden)]
pub fn verify_memory_runtime(
    authority: &str,
    requirements: &SealedDeclarationSnapshot,
) -> Result<(), DatabaseBootstrapError> {
    let runtime = memory_runtime().ok_or(MemoryRuntimeBindingError::NotBootstrapped)?;
    runtime.verify_authority(requirements, authority)?;
    Ok(())
}

/// Construct and bind memory for a database that owns the complete stable backing.
///
/// Call only from the standalone lifecycle root, before database access. Composed
/// hosts bootstrap their complete component set and call `bind_memory_runtime`
/// instead. An existing binding is refused before evaluating the pool provider.
///
/// # Panics
/// Backing IO and the caller's pool provider can panic.
#[doc(hidden)]
pub fn bootstrap_standalone_memory_runtime(
    requirements: &SealedDeclarationSnapshot,
    bucket_size_pages: u16,
    pool: impl FnOnce() -> Result<MemoryAllocationPool, MemoryAllocationPoolError>,
) -> Result<(), DatabaseBootstrapError> {
    if memory_runtime().is_some() {
        return Err(MemoryRuntimeBindingError::AlreadyBound.into());
    }
    let config = MemoryManagerConfig::new(bucket_size_pages)?;
    let pool = pool()?;
    // A provider describes grants only. Binding a runtime is a separate host action.
    if memory_runtime().is_some() {
        return Err(MemoryRuntimeBindingError::AlreadyBound.into());
    }
    let mut runtime = MemoryRuntime::new_with_config(DefaultMemoryImpl::default(), config)?;
    runtime.bootstrap(requirements, &pool, &DatabaseMemoryPolicy)?;
    bind_memory_runtime(Rc::new(runtime))?;
    Ok(())
}

/// Standalone policy uses the same admission function required of composed hosts.
struct DatabaseMemoryPolicy;

impl RuntimeBootstrapPolicy for DatabaseMemoryPolicy {
    type Error = MemoryBootstrapAdmissionError;
    fn validate_allocation(&self, _: &StableKey, _: &MemoryManagerSlot) -> Result<(), Self::Error> {
        Ok(())
    }
    fn prepare_bootstrap(&self, admission: &mut BootstrapAdmission<'_>) -> Result<(), Self::Error> {
        prepare_memory_bootstrap(admission)
    }
    fn runtime_bootstrap_identity(&self) -> PolicyIdentity {
        PolicyIdentity::new([
            162, 211, 99, 60, 182, 106, 249, 175, 69, 210, 132, 12, 241, 103, 92, 177, 182, 86, 63,
            5, 157, 250, 84, 228, 24, 141, 250, 92, 126, 87, 89, 68,
        ])
    }
}

/// Failure to initialize the generated database's stable-memory authority.
///
/// Cloning this error is cheap and preserves the original typed `ic-memory`
/// bootstrap or adoption cause cached by generated database wiring.
#[derive(Clone, Debug)]
pub enum DatabaseBootstrapError {
    /// The backing memory could not be opened under the requested bucket geometry.
    Construction(Arc<RuntimeConstructionError>),
    /// The host runtime could not be bound to IcyDB.
    Binding(MemoryRuntimeBindingError),
    /// Cold bootstrap failed before allocation authority could be published.
    Bootstrap(Arc<RuntimeBootstrapError<MemoryBootstrapAdmissionError>>),
    /// Current host allocations do not satisfy this database's declared authority.
    Adoption(Arc<RuntimeAdoptionError>),
}

impl From<MemoryRuntimeBindingError> for DatabaseBootstrapError {
    fn from(source: MemoryRuntimeBindingError) -> Self {
        Self::Binding(source)
    }
}

impl From<RuntimeConstructionError> for DatabaseBootstrapError {
    fn from(source: RuntimeConstructionError) -> Self {
        Self::Construction(Arc::new(source))
    }
}

impl From<RuntimeAdoptionError> for DatabaseBootstrapError {
    fn from(source: RuntimeAdoptionError) -> Self {
        Self::Adoption(Arc::new(source))
    }
}

impl From<MemoryDeclarationError> for DatabaseBootstrapError {
    fn from(source: MemoryDeclarationError) -> Self {
        RuntimeBootstrapError::Resolution(MemoryResolutionError::Declarations(source)).into()
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
            Self::Construction(source) => source.fmt(f),
            Self::Binding(source) => source.fmt(f),
            Self::Bootstrap(source) => source.fmt(f),
            Self::Adoption(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for DatabaseBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Construction(source) => Some(source.as_ref()),
            Self::Binding(source) => Some(source),
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
    use ic_memory::VectorMemory;
    use ic_memory::{MemoryRequest, RuntimeOpenError};

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
        preparation_calls: std::cell::Cell<usize>,
    }

    impl RuntimeBootstrapPolicy for ExistingRuntimePolicy {
        type Error = MemoryBootstrapAdmissionError;

        fn validate_allocation(
            &self,
            _: &StableKey,
            _: &MemoryManagerSlot,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn prepare_bootstrap(
            &self,
            admission: &mut BootstrapAdmission<'_>,
        ) -> Result<(), Self::Error> {
            self.preparation_calls.set(self.preparation_calls.get() + 1);
            prepare_memory_bootstrap(admission)
        }

        fn runtime_bootstrap_identity(&self) -> PolicyIdentity {
            PolicyIdentity::new([7; 32])
        }
    }

    fn requirements() -> SealedDeclarationSnapshot {
        SealedDeclarationSnapshot::new(
            ["commit.control", "startup.control", "integrity.progress"]
                .into_iter()
                .map(|role| {
                    MemoryRequest::new(TEST_AUTHORITY, &format!("{TEST_AUTHORITY}.{role}.v1"))
                        .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn standalone_bootstrap_selects_fresh_buckets_and_verification_is_read_only() {
        for pages in [4, 16, 128] {
            std::thread::spawn(move || {
                assert!(memory_runtime().is_none());
                assert!(matches!(
                    icydb_core::db::open_memory("icydb.bootstrap_adoption_test.commit.control.v1"),
                    Err(RuntimeOpenError::NotBootstrapped)
                ));
                bootstrap_standalone_memory_runtime(&requirements(), pages, pool).unwrap();
                let runtime = memory_runtime().unwrap();
                let committed = runtime.committed_allocations().unwrap().to_vec();
                let before = runtime.memory_allocation_summary().unwrap();
                assert_eq!(before.bucket_size_pages, pages);
                verify_memory_runtime(TEST_AUTHORITY, &requirements()).unwrap();
                assert_eq!(runtime.committed_allocations().unwrap(), &committed);
                assert_eq!(runtime.memory_allocation_summary().unwrap(), before);
            })
            .join()
            .unwrap();
        }
    }

    #[test]
    fn verification_requires_an_explicit_host_binding() {
        let error = verify_memory_runtime(TEST_AUTHORITY, &requirements()).unwrap_err();
        assert!(matches!(
            error,
            DatabaseBootstrapError::Binding(MemoryRuntimeBindingError::NotBootstrapped)
        ));
        assert!(memory_runtime().is_none());
    }

    #[test]
    fn standalone_refuses_an_existing_host_before_calling_the_pool_provider() {
        bootstrap_standalone_memory_runtime(&requirements(), 16, pool).unwrap();
        let runtime = memory_runtime().unwrap();
        let before = runtime.memory_allocation_summary().unwrap();
        let error = bootstrap_standalone_memory_runtime(&requirements(), 4, || {
            panic!("a bound host must not evaluate a standalone pool provider")
        })
        .unwrap_err();
        assert!(matches!(
            error,
            DatabaseBootstrapError::Binding(MemoryRuntimeBindingError::AlreadyBound)
        ));
        assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));
        assert_eq!(runtime.memory_allocation_summary().unwrap(), before);
        assert_eq!(
            runtime
                .memory_allocation_summary()
                .unwrap()
                .bucket_size_pages,
            16
        );
        assert_eq!(runtime.committed_generation().unwrap(), 1);
    }

    #[test]
    fn conflicting_persisted_layout_rejects_without_changing_allocation() {
        let backing = VectorMemory::default();
        let mut runtime =
            MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(128).unwrap())
                .unwrap();
        runtime
            .bootstrap(&requirements(), &pool().unwrap(), &DatabaseMemoryPolicy)
            .unwrap();
        drop(runtime);
        let before = backing.borrow().clone();
        let error =
            MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(16).unwrap())
                .err()
                .unwrap();
        assert!(matches!(
            error,
            RuntimeConstructionError::BucketSizeMismatch {
                persisted: 128,
                requested: 16
            }
        ));
        let public = crate::db::startup::__startup_bootstrap_failure(error.into());
        assert_eq!(
            public.error().code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_BUCKET_SIZE_MISMATCH
        );
        assert_eq!(
            public.error().core_facts().unwrap(),
            vec![
                (icydb_diagnostic_code::DiagnosticFactTag::Expected, 16),
                (icydb_diagnostic_code::DiagnosticFactTag::Actual, 128),
            ]
        );
        assert_eq!(*backing.borrow(), before);
        assert!(
            MemoryRuntime::new_with_config(backing, MemoryManagerConfig::new(128).unwrap()).is_ok()
        );
    }

    #[test]
    fn unknown_host_authority_rejects_without_changing_committed_allocations() {
        bootstrap_standalone_memory_runtime(&requirements(), 16, pool).unwrap();
        let runtime = memory_runtime().unwrap();
        let committed = runtime.committed_allocations().unwrap().to_vec();
        let before = runtime.memory_allocation_summary().unwrap();
        let error = verify_memory_runtime("icydb.unknown", &requirements()).unwrap_err();
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
        assert_eq!(runtime.committed_allocations().unwrap(), &committed);
        assert_eq!(runtime.memory_allocation_summary().unwrap(), before);
        verify_memory_runtime(TEST_AUTHORITY, &requirements()).unwrap();
    }

    #[test]
    fn adopts_runtime_bootstrapped_by_a_different_policy_and_bucket_size() {
        let policy = ExistingRuntimePolicy {
            preparation_calls: std::cell::Cell::new(0),
        };
        let mut runtime = MemoryRuntime::new_with_config(
            DefaultMemoryImpl::default(),
            MemoryManagerConfig::new(16).unwrap(),
        )
        .unwrap();
        let declarations = requirements();
        let host_pool = pool().unwrap();
        let generation = runtime
            .bootstrap(&declarations, &host_pool, &policy)
            .unwrap();
        assert_eq!(policy.preparation_calls.get(), 1);
        assert_eq!(
            runtime
                .bootstrap(&declarations, &host_pool, &policy)
                .unwrap(),
            generation
        );
        assert_eq!(policy.preparation_calls.get(), 1);
        let runtime = Rc::new(runtime);
        bind_memory_runtime(Rc::clone(&runtime)).unwrap();
        verify_memory_runtime(TEST_AUTHORITY, &requirements()).unwrap();
        verify_memory_runtime(TEST_AUTHORITY, &requirements()).unwrap();
        assert_eq!(policy.preparation_calls.get(), 1);
        assert_eq!(runtime.committed_generation().unwrap(), generation);
        assert_eq!(
            runtime
                .memory_allocation_summary()
                .unwrap()
                .bucket_size_pages,
            16
        );
        assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));
    }
}
