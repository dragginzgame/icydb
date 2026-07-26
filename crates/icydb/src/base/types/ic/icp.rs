//! Module: base::types::ic::icp
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Icp Payment
///

#[record(
    source_key = "crates/icydb/src/base/types/ic/icp.rs::record::1",
    fields(
        field(
            source_key = "recipient",
            ident = "recipient",
            value(item(prim = "Principal"))
        ),
        field(source_key = "tokens", ident = "tokens", value(item(is = "Tokens")))
    )
)]
pub struct Payment {}

///
/// Icp Tokens
/// always denominated in e8s
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ic/icp.rs::newtype::1",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Tokens {}
