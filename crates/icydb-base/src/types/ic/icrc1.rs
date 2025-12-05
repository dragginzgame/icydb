use crate::prelude::*;

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
/// technically ICRC-1 includes ICP, but in that case the ledger_canister is implied
///

#[record(fields(
    field(ident = "ledger_canister", value(item(prim = "Principal"))),
    field(ident = "tokens", value(item(prim = "Nat64")))
))]
pub struct TokenAmount {}
