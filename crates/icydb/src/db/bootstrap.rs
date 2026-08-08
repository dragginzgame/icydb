//! Module: db::bootstrap
//!
//! Responsibility: generated-database memory-manager initialization and typed failure.
//! Does not own: memory allocation policy or generated store initialization.
//! Boundary: ensures that the shared default runtime exists and contains this
//! database's declarations, then preserves any `ic-memory` cause until an
//! interface chooses a compact public error projection.

use std::{convert::Infallible, fmt, sync::Arc};

use ic_memory::{
    AllocationDeclaration, CommittedAllocations, RuntimeBootstrapError, RuntimeOpenError,
    RuntimeStateError, StaticMemoryDeclaration, bootstrap_default_memory_manager,
    committed_allocations, sealed_declaration_snapshot,
};

/// Ensure that the default memory manager contains one database authority.
///
/// This bootstraps an uninitialized runtime and otherwise adopts its committed
/// allocations without reasserting a bootstrap policy. Adoption succeeds only
/// when every declaration registered by this generated database authority
/// appears exactly in the committed allocation capability.
#[doc(hidden)]
pub fn ensure_default_memory_manager(authority: &str) -> Result<(), DatabaseBootstrapError> {
    let allocations = match committed_allocations() {
        Ok(allocations) => allocations,
        Err(RuntimeOpenError::NotBootstrapped) => bootstrap_default_memory_manager()?,
        Err(RuntimeOpenError::State(error)) => {
            return Err(RuntimeBootstrapError::State(error).into());
        }
        // This capability lookup cannot open a key. Any future open-error
        // variant therefore represents lifecycle/API drift, not a condition
        // that generated database initialization can safely recover from.
        Err(_) => {
            return Err(
                RuntimeBootstrapError::State(RuntimeStateError::InconsistentLifecycle).into(),
            );
        }
    };

    validate_committed_authority_declarations(authority, &allocations)?;
    Ok(())
}

fn validate_committed_authority_declarations(
    authority: &str,
    allocations: &CommittedAllocations,
) -> Result<(), DatabaseBootstrapError> {
    let snapshot =
        sealed_declaration_snapshot().map_err(RuntimeBootstrapError::<Infallible>::Registry)?;
    if authority_declarations_are_committed(
        authority,
        snapshot.registered_declarations(),
        allocations.declarations(),
    ) {
        Ok(())
    } else {
        Err(RuntimeBootstrapError::DeclarationSnapshotMismatch.into())
    }
}

fn authority_declarations_are_committed(
    authority: &str,
    registrations: &[StaticMemoryDeclaration],
    committed: &[AllocationDeclaration],
) -> bool {
    let mut found = false;
    for registration in registrations {
        if registration.authority() != authority {
            continue;
        }
        found = true;
        if !committed.contains(registration.declaration()) {
            return false;
        }
    }
    found
}

/// Failure to initialize the generated database's stable-memory authority.
///
/// Cloning this error is cheap and preserves the original typed `ic-memory`
/// cause cached by generated database wiring.
#[derive(Clone, Debug)]
pub struct DatabaseBootstrapError {
    source: Arc<RuntimeBootstrapError<Infallible>>,
}

impl DatabaseBootstrapError {
    /// Borrow the authoritative `ic-memory` bootstrap failure.
    #[must_use]
    pub fn cause(&self) -> &RuntimeBootstrapError<Infallible> {
        &self.source
    }
}

impl From<RuntimeBootstrapError<Infallible>> for DatabaseBootstrapError {
    fn from(source: RuntimeBootstrapError<Infallible>) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

impl fmt::Display for DatabaseBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for DatabaseBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use ic_memory::{
        AllocationPolicy, AllocationSlotDescriptor, MemoryManagerRangeMode, PolicyIdentity,
        PolicyIdentityError, RuntimeBootstrapPolicy, StableKey,
        bootstrap_default_memory_manager_with_policy, register_static_memory_manager_declaration,
        register_static_memory_manager_range,
    };

    const TEST_AUTHORITY: &str = "icydb.bootstrap-adoption-test";
    const TEST_STABLE_KEY: &str = "icydb.bootstrap_adoption_test.data.v1";
    const TEST_MEMORY_ID: u8 = 100;

    struct ExistingRuntimePolicy;

    impl AllocationPolicy for ExistingRuntimePolicy {
        type Error = Infallible;

        fn validate_key(&self, _key: &StableKey) -> Result<(), Self::Error> {
            Ok(())
        }

        fn validate_slot(
            &self,
            _key: &StableKey,
            _slot: &AllocationSlotDescriptor,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn validate_reserved_slot(
            &self,
            _key: &StableKey,
            _slot: &AllocationSlotDescriptor,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    impl RuntimeBootstrapPolicy for ExistingRuntimePolicy {
        fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
            PolicyIdentity::new("tests.existing-runtime-policy", 1)
        }
    }

    fn declaration(key: &str, id: u8) -> AllocationDeclaration {
        AllocationDeclaration::memory_manager(key, id, key)
            .expect("test allocation declaration should admit")
    }

    fn registration(authority: &str, key: &str, id: u8) -> StaticMemoryDeclaration {
        StaticMemoryDeclaration::new(authority, declaration(key, id))
            .expect("test static registration should admit")
    }

    #[test]
    fn authority_validation_requires_every_matching_declaration() {
        let first = registration(TEST_AUTHORITY, "icydb.test.first.v1", 101);
        let second = registration(TEST_AUTHORITY, "icydb.test.second.v1", 102);
        let other = registration("other.framework", "other.framework.data.v1", 103);
        let registrations = [first.clone(), second.clone(), other];

        assert!(authority_declarations_are_committed(
            TEST_AUTHORITY,
            &registrations,
            &[first.declaration().clone(), second.declaration().clone()],
        ));
        assert!(!authority_declarations_are_committed(
            TEST_AUTHORITY,
            &registrations,
            &[first.declaration().clone()],
        ));
        assert!(!authority_declarations_are_committed(
            "missing.authority",
            &registrations,
            &[first.declaration().clone(), second.declaration().clone()],
        ));
    }

    #[test]
    fn adopts_runtime_bootstrapped_by_a_different_policy() {
        register_static_memory_manager_range(
            TEST_MEMORY_ID,
            TEST_MEMORY_ID,
            TEST_AUTHORITY,
            MemoryManagerRangeMode::Reserved,
            None,
        )
        .expect("test authority range should register");
        register_static_memory_manager_declaration(
            TEST_MEMORY_ID,
            TEST_AUTHORITY,
            "BootstrapAdoptionTest",
            TEST_STABLE_KEY,
        )
        .expect("test allocation should register");

        let upstream = bootstrap_default_memory_manager_with_policy(&ExistingRuntimePolicy)
            .expect("existing policy identity should bootstrap the shared runtime");
        let generation = upstream.generation();

        ensure_default_memory_manager(TEST_AUTHORITY)
            .expect("IcyDB should adopt the upstream committed capability");

        assert_eq!(
            committed_allocations()
                .expect("adopted allocations should remain available")
                .generation(),
            generation,
        );
        assert!(matches!(
            bootstrap_default_memory_manager(),
            Err(RuntimeBootstrapError::PolicyIdentityMismatch { .. })
        ));
    }
}
