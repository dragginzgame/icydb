use crate::design::prelude::*;

///
/// Sha256
///
/// Canonical SHA-256 digest text wrapper validated by the hash validator.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::hash::Sha256"))
)]
pub struct Sha256 {}
