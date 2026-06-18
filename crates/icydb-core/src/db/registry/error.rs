//! Module: db::registry::error
//! Responsibility: typed registry lookup and registration error classification.
//! Does not own: store runtime behavior or persisted storage error taxonomy.
//! Boundary: converts registry-local failures into database internal errors.

use crate::error::{ErrorClass, ErrorOrigin, InternalError};

///
/// StoreRegistryError
///
/// StoreRegistryError is the typed error taxonomy for registry lookup and
/// registration invariants.
/// It stays local to the registry boundary and converts into `InternalError`
/// for callers that operate on the wider database error contract.
///

#[derive(Debug)]
#[expect(clippy::enum_variant_names)]
pub enum StoreRegistryError {
    /// Requested generated store path is not registered.
    StoreNotFound,

    /// Generated store path was registered more than once.
    StoreAlreadyRegistered,

    /// Physical data/index/schema store triplet was reused by another path.
    StoreHandleTripletAlreadyRegistered,

    /// Allocation identity metadata does not match declared storage capabilities.
    StoreAllocationCapabilityMismatch,
}

impl StoreRegistryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::StoreNotFound => ErrorClass::Internal,
            Self::StoreAlreadyRegistered
            | Self::StoreHandleTripletAlreadyRegistered
            | Self::StoreAllocationCapabilityMismatch => ErrorClass::InvariantViolation,
        }
    }
}

impl From<StoreRegistryError> for InternalError {
    fn from(err: StoreRegistryError) -> Self {
        Self::classified(err.class(), ErrorOrigin::Store)
    }
}
