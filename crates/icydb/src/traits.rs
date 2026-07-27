//! Module: traits
//!
//! Responsibility: public trait facade and application entity contract.
//! Does not own: core trait implementation semantics.
//! Boundary: re-exports stable trait names and narrows facade-only contracts.

pub use icydb_core::db::{EntityKey, EntityKeyBytes, EntityKeyBytesError};
pub use icydb_core::traits::{CanisterKind, FieldTypeMeta, Inner, Kind, Path, StoreKind, TypeKind};
pub use icydb_core::types::NumericValue;
pub use icydb_core::value::{Collection, MapCollection};
pub use icydb_core::visitor::{
    Normalize, NormalizeAuto, NormalizeCustom, Normalizer, Validate, ValidateAuto, ValidateCustom,
    Validator, Visitable,
};
