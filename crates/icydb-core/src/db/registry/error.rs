use crate::error::{ErrorClass, ErrorOrigin, InternalError};
use thiserror::Error as ThisError;

///
/// StoreRegistryError
///
/// StoreRegistryError is the typed error taxonomy for registry lookup and
/// registration invariants.
/// It stays local to the registry boundary and converts into `InternalError`
/// for callers that operate on the wider database error contract.
///

#[derive(Debug, ThisError)]
#[expect(clippy::enum_variant_names)]
pub enum StoreRegistryError {
    #[error("store '{0}' not found")]
    StoreNotFound(String),

    #[error("store '{0}' already registered")]
    StoreAlreadyRegistered(String),

    #[error(
        "store '{name}' reuses the same row/index/schema store triplet already registered as '{existing_name}'"
    )]
    StoreHandleTripletAlreadyRegistered { name: String, existing_name: String },
}

impl StoreRegistryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::StoreNotFound(_) => ErrorClass::Internal,
            Self::StoreAlreadyRegistered(_) | Self::StoreHandleTripletAlreadyRegistered { .. } => {
                ErrorClass::InvariantViolation
            }
        }
    }
}

impl From<StoreRegistryError> for InternalError {
    fn from(err: StoreRegistryError) -> Self {
        Self::classified(err.class(), ErrorOrigin::Store, err.to_string())
    }
}
