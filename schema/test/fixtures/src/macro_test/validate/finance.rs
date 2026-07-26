use icydb::design::prelude::*;

///
/// Usd
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/finance.rs::newtype::1",
    item(is = "base::types::finance::Usd")
)]
pub struct Usd {}

///
/// E8Fixed
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/finance.rs::newtype::2",
    item(is = "base::types::finance::E8s")
)]
pub struct E8Fixed {}

///
/// E18Fixed
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/finance.rs::newtype::3",
    item(is = "base::types::finance::E18s")
)]
pub struct E18Fixed {}
