//! Module: base::types::hash
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Sha256
///
/// Canonical SHA-256 digest text wrapper validated by the hash validator.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::hash::Sha256"))
)]
pub struct Sha256 {}
