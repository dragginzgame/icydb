pub mod case;
pub mod collection;
pub mod decimal;
pub mod finance;
pub mod list;
pub mod newtype;
pub mod option;
pub mod record;

use crate::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct Entity {}
