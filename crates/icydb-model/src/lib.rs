//! IcyDB application-model authoring and code generation.
//!
//! This package owns application declarations, the opt-in host authoring graph,
//! explicit application validation and normalization, and lowering into the
//! public [`icydb_schema`] proposal contract. It is not database authority.

extern crate self as icydb_model;

pub mod application;
pub mod base;
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
/// Build-time schema registration, validation and actor generation.
pub mod build;
pub mod error;
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
pub mod fragment;
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
pub mod node;
pub mod normalize;
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
mod schema_validate;
mod typed_adapter;
pub mod types;
pub mod validate;
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
mod visit;
pub mod visitor;

#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
use crate::{build::BuildError, node::NodeError};
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
use thiserror::Error as ThisError;

/// Shared schema-building prelude used by validators, macros, and tests.
pub mod prelude {
    #[cfg(all(feature = "build", not(target_arch = "wasm32")))]
    pub(crate) use crate::build::schema_read;
    #[cfg(all(feature = "build", not(target_arch = "wasm32")))]
    pub use crate::node::*;
    pub use crate::{
        EntitySource as _, Inner as _, NormalizeAndValidate as _, Path as _, base, canister,
        entity, enum_, err,
        error::ErrorTree,
        list, map, newtype, normalizer, record,
        schema::*,
        set, store, tuple,
        types::{Cardinality, Primitive},
        validator,
        visitor::{
            Issue, Normalize as _, NormalizeAuto, NormalizeCustom, Normalizer as _, Validate as _,
            ValidateAuto, ValidateCustom, Validator as _, Visitable as _, VisitorContext,
        },
    };
    #[cfg(all(feature = "build", not(target_arch = "wasm32")))]
    pub(crate) use crate::{
        node::{MacroNode, ValidateNode, VisitableNode},
        visit::Visitor,
    };

    pub use serde::{Deserialize, Serialize};
}

pub use application::NormalizeAndValidate;
pub use icydb_model_macros::{
    Add, AddAssign, Deref, DerefMut, Display, Div, DivAssign, Inner, Mul, MulAssign, Neg, Product,
    Rem, RemAssign, Sub, SubAssign, Sum, canister, entity, enum_, list, map, newtype, normalizer,
    record, set, store, tuple, validator,
};
pub use icydb_schema::MAX_INDEX_FIELDS;
pub use normalize::normalize;
#[doc(hidden)]
pub use typed_adapter::{
    TypedAdapterContext, TypedEnumDescriptor, TypedEnumSelection, TypedInputValue, TypedNamedType,
    TypedOutputValue, TypedScalarValue, TypedValueError,
};
pub use validate::validate;

/// Fully-qualified path identity for generated application declarations.
pub trait Path {
    /// Stable Rust declaration path.
    const PATH: &'static str;
}

/// Schema-authored entity source identity emitted for runtime-enabled models.
///
/// This is application vocabulary only. It cannot establish accepted-schema
/// presence, freshness, or authority; runtime APIs resolve the source against
/// their pinned accepted snapshot.
pub trait EntitySource {
    /// Exact authored entity source name.
    const ENTITY: &'static str;
}

/// Borrowed and consuming access to one-field application wrappers.
pub trait Inner<T> {
    /// Borrow the wrapped value.
    fn inner(&self) -> &T;

    /// Consume the wrapper and return its value.
    fn into_inner(self) -> T;
}

/// Exact public proposal vocabulary consumed by application-model lowering.
pub mod schema {
    pub use icydb_schema::*;
}

/// Dependencies intentionally exposed to generated model code.
#[doc(hidden)]
pub mod __reexports {

    #[cfg(all(feature = "build", not(target_arch = "wasm32")))]
    pub use ctor;
    pub use icydb_model_macros;
    pub use remain;
    pub use serde;
}

///
/// Error
///
/// Top-level schema error boundary spanning build-time validation and node
/// lookup/type errors.
///
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    /// Build-time schema collection, validation or sealing failed.
    BuildError(#[from] BuildError),

    #[error(transparent)]
    /// A schema-node lookup or type check failed.
    NodeError(#[from] NodeError),
}

//
// TESTS
//

#[cfg(all(test, feature = "build", not(target_arch = "wasm32")))]
mod tests;

/// Expansion support: collect declarations only in explicitly selected host builds.
#[doc(hidden)]
#[cfg(all(feature = "build", not(target_arch = "wasm32")))]
#[macro_export]
macro_rules! __icydb_register_schema {
    ($($item:item)*) => { $($item)* };
}

#[doc(hidden)]
#[cfg(not(all(feature = "build", not(target_arch = "wasm32"))))]
#[macro_export]
macro_rules! __icydb_register_schema {
    ($($item:item)*) => {};
}
