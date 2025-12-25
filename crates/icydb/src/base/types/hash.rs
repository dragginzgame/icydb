use crate::design::prelude::*;

///
/// Sha256
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(validator(path = "base::validator::hash::Sha256"))
)]
pub struct Sha256 {}
