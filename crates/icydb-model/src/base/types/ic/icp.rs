//! Module: base::types::ic::icp
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Icp Payment
///

#[record(
    name = "IcpPayment",
    fields(
        field(name = "recipient", value(item(prim = "Principal"))),
        field(name = "tokens", value(item(is = "Tokens")))
    )
)]
pub struct Payment {}

///
/// Icp Tokens
/// always denominated in e8s
///

#[newtype(name = "IcpTokens", item(prim = "Nat64"))]
pub struct Tokens {}
