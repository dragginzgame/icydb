pub mod icp;
pub mod icrc1;
pub mod icrc3;

use crate::prelude::*;

///
/// Memo
///

#[newtype(primitive = "Blob", item(prim = "Blob"))]
pub struct Memo {}
