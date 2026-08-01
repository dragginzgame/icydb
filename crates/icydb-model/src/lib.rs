//! IcyDB application-model authoring and code generation.
//!
//! This package owns application declarations, the host-only authoring graph,
//! explicit application validation and normalization, and lowering into the
//! public [`icydb_schema`] proposal contract. It is not database authority.

extern crate self as icydb_model;

pub mod application;
pub mod base;
pub mod build;
pub mod error;
pub mod fragment;
// Declarations remain available on Wasm, while their whole-graph validation
// helpers are deliberately host-only work.
#[cfg_attr(
    target_arch = "wasm32",
    expect(
        dead_code,
        reason = "Wasm retains declaration nodes while host code owns whole-graph traversal"
    )
)]
pub mod node;
pub mod normalize;
#[cfg_attr(
    target_arch = "wasm32",
    expect(dead_code, reason = "whole-graph schema validation is host-owned")
)]
mod schema_validate;
mod typed_adapter;
pub mod types;
pub mod validate;
#[cfg_attr(
    target_arch = "wasm32",
    expect(
        dead_code,
        reason = "Wasm retains declarations while host code owns graph visitation"
    )
)]
mod visit;
pub mod visitor;

// Maximum length for entity schema identifiers.
pub const MAX_ENTITY_NAME_LEN: usize = 64;

// Maximum length for field schema identifiers.
pub const MAX_FIELD_NAME_LEN: usize = 64;

// Maximum number of fields allowed in a derived index.
pub const MAX_INDEX_FIELDS: usize = 4;

// Maximum length for derived index identifiers.
pub const MAX_INDEX_NAME_LEN: usize =
    MAX_ENTITY_NAME_LEN + (MAX_INDEX_FIELDS * (1 + MAX_FIELD_NAME_LEN));

use crate::{build::BuildError, node::NodeError};
use thiserror::Error as ThisError;

/// Shared schema-building prelude used by validators, macros, and tests.
pub mod prelude {
    pub(crate) use crate::build::schema_read;
    pub use crate::{
        Inner as _, NormalizeAndValidate as _, Path as _, base, canister, entity, enum_, err,
        error::ErrorTree,
        list, map, newtype,
        node::*,
        normalizer, record,
        schema::*,
        set, store, tuple,
        types::{Cardinality, Primitive},
        validator,
        visitor::{
            Issue, Normalize as _, NormalizeAuto, NormalizeCustom, Normalizer as _, Validate as _,
            ValidateAuto, ValidateCustom, Validator as _, Visitable as _, VisitorContext,
        },
    };
    pub(crate) use crate::{
        node::{MacroNode, ValidateNode, VisitableNode},
        visit::Visitor,
    };
    pub use candid::CandidType;
    pub use serde::{Deserialize, Serialize};
}

pub use application::NormalizeAndValidate;
pub use icydb_model_macros::{
    Add, AddAssign, Deref, DerefMut, Display, Div, DivAssign, Inner, Mul, MulAssign, Rem, Sub,
    SubAssign, Sum, canister, entity, enum_, list, map, newtype, normalizer, record, set, store,
    tuple, validator,
};
pub use normalize::normalize;
#[doc(hidden)]
pub use typed_adapter::{
    TypedAdapterContext, TypedEnumOutput, TypedInputValue, TypedNamedType, TypedOutputValue,
    TypedScalarValue, TypedValueError,
};
pub use validate::validate;

/// Fully-qualified path identity for generated application declarations.
pub trait Path {
    /// Stable Rust declaration path.
    const PATH: &'static str;
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
    pub use candid;
    #[cfg(not(target_arch = "wasm32"))]
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
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    BuildError(#[from] BuildError),

    #[error(transparent)]
    NodeError(#[from] NodeError),
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::{Error, build::BuildError, error::ErrorTree, node::NodeError};

    #[test]
    fn build_errors_remain_in_build_boundary() {
        let schema_error = Error::from(BuildError::Validation(ErrorTree::from(
            "missing schema relation target",
        )));

        match schema_error {
            Error::BuildError(BuildError::Validation(tree)) => {
                assert!(
                    tree.messages()
                        .iter()
                        .any(|message| message == "missing schema relation target"),
                    "build validation errors must remain wrapped as build-boundary failures",
                );
            }
            Error::BuildError(BuildError::Graph(error)) => {
                panic!("unexpected graph error: {error}");
            }
            Error::NodeError(_) => {
                panic!("build validation failures must not be remapped into node-boundary errors");
            }
        }
    }

    #[test]
    fn node_errors_remain_in_node_boundary() {
        let schema_error = Error::from(NodeError::PathNotFound("entity.user_id".to_string()));

        match schema_error {
            Error::NodeError(NodeError::PathNotFound(path)) => {
                assert_eq!(path, "entity.user_id");
            }
            Error::NodeError(NodeError::IncorrectNodeType(path)) => {
                panic!("unexpected node error kind after conversion for path {path}");
            }
            Error::BuildError(_) => {
                panic!("node errors must not be remapped into build-boundary failures");
            }
        }
    }
}
