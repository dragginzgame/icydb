//! Module: base::types::time
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Milliseconds
///
/// Duration wrapper expressed in milliseconds.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Milliseconds {}

///
/// Seconds
///
/// Duration wrapper expressed in seconds.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Seconds {}

///
/// Minutes
///
/// Duration wrapper expressed in minutes.
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Minutes {}
