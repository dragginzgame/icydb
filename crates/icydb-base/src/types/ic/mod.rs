pub mod icrc1;
pub mod icrc3;

use crate::prelude::*;

///
/// Memo
///

#[newtype(primitive = "Blob", item(prim = "Blob"))]
pub struct Memo {}

pub mod icp {
    use super::*;

    ///
    /// Icp Payment
    ///

    #[record(fields(
        field(ident = "recipient", value(item(prim = "Principal"))),
        field(ident = "amount", value(item(is = "Amount")))
    ))]
    pub struct Payment {}

    ///
    /// Icp Amount
    /// always denominated in e8s
    ///

    #[newtype(primitive = "Nat64", item(prim = "Nat64"))]
    pub struct Amount {}
}
