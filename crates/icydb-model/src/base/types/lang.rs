//! Module: base::types::lang
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Code
/// ISO 639-1 standard language codes
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::intl::iso::Iso639_1"))
)]
pub struct Code {}

///
/// TESTS
///

#[cfg(test)]
mod tests;
