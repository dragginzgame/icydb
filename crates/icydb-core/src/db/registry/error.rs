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
    StoreNotFound,

    StoreAlreadyRegistered,

    StoreHandleTripletAlreadyRegistered,

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
