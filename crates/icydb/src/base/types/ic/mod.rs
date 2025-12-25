pub mod icp;
pub mod icrc1;
pub mod icrc3;

use crate::design::prelude::*;

///
/// Memo
///

#[newtype(primitive = "Blob", item(prim = "Blob"))]
pub struct Memo {}
