use crate::design::prelude::*;

///
/// Icrc1 Payment
///

#[record(fields(
    field(ident = "recipient", value(item(prim = "Principal"))),
    field(ident = "token_amount", value(item(is = "TokenAmount")))
))]
pub struct Payment {}

///
/// Icrc1 TokenAmount
/// the Icrc ledger canister + the number of tokens
/// technically ICRC-1 includes ICP, but in that case the ledger_canister is implied
///

#[record(fields(
    field(ident = "ledger_canister", value(item(prim = "Principal"))),
    field(ident = "tokens", value(item(is = "Tokens")))
))]
pub struct TokenAmount {}

///
/// Icrc1 Tokens
/// just the raw number of tokens
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Tokens {}
