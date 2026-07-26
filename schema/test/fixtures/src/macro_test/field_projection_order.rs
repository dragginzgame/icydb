use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ProjectionOrderEntity
///
/// Representative entity used to lock field-order alignment between
/// `EntityModel::fields()` and `FieldProjection::get_value_by_index` output.
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/field_projection_order.rs::entity::1",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "title", ident = "title", value(item(prim = "Text", unbounded))),
        field(source_key = "score", ident = "score", value(item(prim = "Nat32"))),
        field(source_key = "nickname", ident = "nickname", value(opt, item(prim = "Text", unbounded))),
        field(source_key = "tags", ident = "tags", value(many, item(prim = "Text", unbounded)))
    )
)]
pub struct ProjectionOrderEntity {}
