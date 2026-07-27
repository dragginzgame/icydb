//! Module: base::types::time
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Milliseconds
///
/// Duration wrapper expressed in milliseconds.
///

#[newtype(
    source_key = "crates/icydb/src/base/types/time.rs::newtype::1",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Milliseconds {}

///
/// Seconds
///
/// Duration wrapper expressed in seconds.
///

#[newtype(
    source_key = "crates/icydb/src/base/types/time.rs::newtype::2",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Seconds {}

///
/// Minutes
///
/// Duration wrapper expressed in minutes.
///

#[newtype(
    source_key = "crates/icydb/src/base/types/time.rs::newtype::3",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Minutes {}
