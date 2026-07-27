//! Module: base::types::ic::icrc1
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Icrc1 Payment
///

#[record(
    source_key = "crates/icydb/src/base/types/ic/icrc1.rs::record::1",
    fields(
        field(
            source_key = "recipient",
            ident = "recipient",
            value(item(prim = "Principal"))
        ),
        field(
            source_key = "token_amount",
            ident = "token_amount",
            value(item(is = "TokenAmount"))
        )
    )
)]
pub struct Payment {}

///
/// Icrc1 TokenAmount
/// the Icrc ledger canister + the number of tokens
/// technically ICRC-1 includes ICP, but in that case the ledger_canister is implied
///

#[record(
    source_key = "crates/icydb/src/base/types/ic/icrc1.rs::record::2",
    fields(
        field(
            source_key = "ledger_canister",
            ident = "ledger_canister",
            value(item(prim = "Principal"))
        ),
        field(source_key = "tokens", ident = "tokens", value(item(is = "Tokens")))
    )
)]
pub struct TokenAmount {}

impl TokenAmount {
    #[must_use]
    pub fn units(&self) -> u64 {
        self.tokens.units()
    }
}

///
/// Icrc1 Tokens
/// just the raw number of tokens
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ic/icrc1.rs::newtype::1",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Tokens {}

impl Tokens {
    #[must_use]
    pub fn units(&self) -> u64 {
        *self.inner()
    }
}
