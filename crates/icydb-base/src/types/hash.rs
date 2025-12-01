use crate::prelude::*;

///
/// Sha256
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(validator(path = "validator::hash::Sha256"))
)]
pub struct Sha256 {}
