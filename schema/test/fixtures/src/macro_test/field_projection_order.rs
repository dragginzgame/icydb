use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ProjectionOrderEntity
///
/// Representative entity used to lock field-order alignment between
/// `EntityModel::fields()` and `FieldProjection::get_value_by_index` output.
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "title", value(item(prim = "Text", unbounded))),
        field(ident = "score", value(item(prim = "Nat32"))),
        field(ident = "nickname", value(opt, item(prim = "Text", unbounded))),
        field(ident = "tags", value(many, item(prim = "Text", unbounded)))
    )
)]
pub struct ProjectionOrderEntity {}
