//! Module: db::bootstrap
//!
//! Responsibility: generated-database memory-manager initialization and typed failure.
//! Does not own: memory allocation policy or generated store initialization.
//! Boundary: ensures that the bound host runtime exists and contains this
//! database's declarations, then preserves any `ic-storage` cause until an
//! interface chooses a compact public error projection.

use crate::db::{
    MemoryBootstrapAdmissionError, MemoryRuntimeBindingError, bind_memory_runtime, memory_runtime,
    prepare_memory_bootstrap,
};
use std::{fmt, rc::Rc, sync::Arc};

use ic_storage::{
    AdmissionError, AllocationGrants, BootstrapAdmission, MemoryRuntime, PolicyIdentity,
    RuntimeBootstrapError, RuntimeBootstrapPolicy, RuntimeConstructionError, RuntimeOpenError,
    SealedDeclarationSnapshot, StableKey, StorageConfig,
};

use ic_storage::DefaultMemoryImpl;

/// Ensure that the bound host memory runtime contains one database authority.
///
/// This bootstraps an uninitialized runtime and otherwise adopts its committed
/// allocations without reasserting a bootstrap policy. Adoption succeeds only
/// when every explicit requirement of this generated database authority
/// appears exactly in the committed allocation capability.
/// Storage accepts only its current owned directory format. Earlier Memory or
/// Storage images are rejected without resetting their backing. Same-contract
/// restarts retain named allocations and require current namespace admission.
/// Cold bootstrap obtains host grants from `grants`; an already-bound runtime
/// retains its host configuration and grants.
///
/// # Panics
/// Panics if the caller's grants provider panics during cold bootstrap.
#[doc(hidden)]
pub fn ensure_default_storage_runtime(
    authority: &str,
    requirements: &SealedDeclarationSnapshot,
    config: StorageConfig,
    grants: impl FnOnce() -> Result<AllocationGrants, AdmissionError>,
) -> Result<(), DatabaseBootstrapError> {
    let runtime = if let Some(runtime) = memory_runtime() {
        runtime
    } else {
        let grants = grants()?;
        // The host callback may bind its own runtime. Observe that authority
        // before constructing another manager for the same backing memory.
        if let Some(runtime) = memory_runtime() {
            runtime
        } else {
            let mut runtime = MemoryRuntime::new_with_config(DefaultMemoryImpl::default(), config)?;
            runtime.bootstrap(requirements, &grants, &DatabaseMemoryPolicy(requirements))?;
            let runtime = Rc::new(runtime);
            bind_memory_runtime(Rc::clone(&runtime))?;
            runtime
        }
    };
    runtime.verify_authority(requirements, authority)?;
    Ok(())
}

/// Standalone policy uses the same admission function required of composed hosts.
struct DatabaseMemoryPolicy<'a>(&'a SealedDeclarationSnapshot);

impl RuntimeBootstrapPolicy for DatabaseMemoryPolicy<'_> {
    type Error = MemoryBootstrapAdmissionError;
    fn validate_allocation(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }
    fn prepare_bootstrap(&self, admission: &mut BootstrapAdmission<'_>) -> Result<(), Self::Error> {
        prepare_memory_bootstrap(self.0, admission)
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
/// Cloning this error is cheap and preserves the original typed `ic-storage`
/// bootstrap or adoption cause cached by generated database wiring.
#[derive(Clone, Debug)]
pub enum DatabaseBootstrapError {
    /// The backing memory could not be opened under the requested storage limits.
    Construction(Arc<RuntimeConstructionError>),
    /// The host runtime could not be bound to IcyDB.
    Binding(MemoryRuntimeBindingError),
    /// Cold bootstrap failed before allocation authority could be published.
    Bootstrap(Arc<RuntimeBootstrapError<MemoryBootstrapAdmissionError>>),
    /// Current host allocations do not satisfy this database's declared authority.
    Adoption(Arc<RuntimeOpenError>),
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

impl From<RuntimeOpenError> for DatabaseBootstrapError {
    fn from(source: RuntimeOpenError) -> Self {
        Self::Adoption(Arc::new(source))
    }
}

impl From<AdmissionError> for DatabaseBootstrapError {
    fn from(source: AdmissionError) -> Self {
        RuntimeBootstrapError::Admission(source).into()
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

#[cfg(test)]
mod tests {
    mod public_failures;
    use super::*;
    use ic_storage::{GenericAllocationPolicy, MemoryAuthority, MemoryRequest, VectorMemory};

    const OWNER: &str = "icydb_bootstrap_test";
    const PREFIX: &str = "icydb.bootstrap_test.";

    fn requirements() -> SealedDeclarationSnapshot {
        SealedDeclarationSnapshot::new(
            ["commit.control", "startup.control", "integrity.progress"]
                .into_iter()
                .map(|role| MemoryRequest::new(OWNER, &format!("{PREFIX}{role}.v1")).unwrap())
                .collect(),
        )
        .unwrap()
    }

    fn grants() -> Result<AllocationGrants, AdmissionError> {
        AllocationGrants::new(vec![MemoryAuthority::new(OWNER, PREFIX)?])
    }

    #[test]
    fn adopts_runtime_bound_during_grants_callback() {
        std::thread::spawn(|| {
            ensure_default_storage_runtime(
                OWNER,
                &requirements(),
                StorageConfig::default(),
                || {
                    ensure_default_storage_runtime(
                        OWNER,
                        &requirements(),
                        StorageConfig::default(),
                        grants,
                    )
                    .unwrap();
                    grants()
                },
            )
            .unwrap();
            assert_eq!(memory_runtime().unwrap().committed_generation().unwrap(), 1);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn adopts_host_policy_without_rebootstrap_or_regrant() {
        std::thread::spawn(|| {
            let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
            runtime.bootstrap(&requirements(), &grants().unwrap(), &GenericAllocationPolicy).unwrap();
            let before = runtime.memory_allocations().unwrap();
            let runtime = Rc::new(runtime);
            bind_memory_runtime(runtime.clone()).unwrap();
            ensure_default_storage_runtime(OWNER, &requirements(), StorageConfig::default(), || panic!("warm adoption must not call provider")).unwrap();
            assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));
            assert_eq!(runtime.memory_allocations().unwrap(), before);
            let error = ensure_default_storage_runtime("unknown", &requirements(), StorageConfig::default(), grants).unwrap_err();
            assert!(matches!(error, DatabaseBootstrapError::Adoption(cause) if matches!(cause.as_ref(), RuntimeOpenError::NotAdmitted(_))));
            assert_eq!(runtime.memory_allocations().unwrap(), before);
        }).join().unwrap();
    }
}
