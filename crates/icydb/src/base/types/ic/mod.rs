//! Module: base::types::ic
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

pub mod icp;
pub mod icrc1;
pub mod icrc3;

use crate::design::prelude::*;

///
/// Memo
///
/// Opaque blob memo payload used by Internet Computer transfer flows.
///

#[newtype(primitive = "Blob", item(prim = "Blob", unbounded))]
pub struct Memo {}
