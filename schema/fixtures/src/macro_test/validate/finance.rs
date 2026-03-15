use icydb::design::prelude::*;

///
/// Usd
///

#[newtype(item(is = "base::types::finance::Usd"))]
pub struct Usd {}

///
/// E8Fixed
///

#[newtype(item(is = "base::types::finance::E8s"))]
pub struct E8Fixed {}

///
/// E18Fixed
///

#[newtype(item(is = "base::types::finance::E18s"))]
pub struct E18Fixed {}
