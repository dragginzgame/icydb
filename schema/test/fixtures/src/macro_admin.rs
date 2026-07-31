use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ADMIN TESTS
/// set up to test the admin interface
///

///
/// ComplexEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "string_test", value(item(prim = "Text", unbounded))),
        field(name = "principal_test", value(item(prim = "Principal"))),
        field(name = "blob_test", value(item(prim = "Blob", unbounded))),
        field(name = "int_candid", value(item(prim = "IntBig"))),
        field(name = "int_8", value(item(prim = "Int8"))),
        field(name = "int_16", value(item(prim = "Int16"))),
        field(name = "int_32", value(item(prim = "Int32"))),
        field(name = "int_64", value(item(prim = "Int64"))),
        field(name = "nat_candid", value(item(prim = "NatBig"))),
        field(name = "nat_8", value(item(prim = "Nat8"))),
        field(name = "nat_16", value(item(prim = "Nat16"))),
        field(name = "nat_64", value(item(prim = "Nat64"))),
        field(name = "e8s", value(item(is = "base::types::finance::E8s"))),
        field(name = "e18s", value(item(is = "base::types::finance::E18s"))),
        field(name = "float_32", value(item(prim = "Float32"))),
        field(name = "float_64", value(item(prim = "Float64"))),
        field(name = "bool_test", value(item(prim = "Bool"))),
        field(name = "timestamp", value(item(prim = "Timestamp"))),
        field(name = "utf8_test", value(item(is = "base::types::bytes::Utf8"))),
        field(name = "tuple_test", value(item(is = "Tuple"))),
        field(name = "name_many", value(many, item(prim = "Text", unbounded))),
        field(name = "name_opt", value(opt, item(prim = "Text", unbounded))),
        field(name = "record_a", value(item(is = "RecordA"))),
        field(name = "record_opt", value(opt, item(is = "RecordB"))),
        field(name = "record_many", value(many, item(is = "RecordB"))),
        field(name = "list", value(item(is = "List"))),
        field(name = "map", value(item(is = "Map"))),
        field(name = "set", value(item(is = "Set"))),
        field(name = "variant_complex", value(item(is = "EnumA"))),
        field(name = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(name = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(name = "variant_simple", value(item(is = "EnumB"))),
        field(name = "variant_simple_many", value(many, item(is = "EnumB"))),
        field(name = "variant_simple_opt", value(opt, item(is = "EnumB")))
    ),
    timestamps
)]
pub struct ComplexEntity {}

///
/// AdminEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "simple_text", value(item(prim = "Text", unbounded))),
        field(name = "tuple_test", value(item(is = "Tuple"))),
        field(name = "text_many", value(many, item(prim = "Text", unbounded))),
        field(name = "text_opt", value(opt, item(prim = "Text", unbounded))),
        field(name = "nat_32", value(item(prim = "Nat32"))),
        field(name = "record_a", value(item(is = "RecordA"))),
        field(name = "record_opt", value(opt, item(is = "RecordB"))),
        field(name = "record_many", value(many, item(is = "RecordB"))),
        field(name = "variant_complex", value(item(is = "EnumA"))),
        field(name = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(name = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(name = "variant_simple", value(item(is = "EnumB"))),
        field(name = "variant_simple_opt", value(opt, item(is = "EnumB"))),
        field(name = "variant_simple_many", value(many, item(is = "EnumB"))),
    ),
    timestamps
)]
pub struct AdminEntity {}

///
/// RelatedEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "simple_id", value(item(rel = "SimpleEntity", prim = "Ulid"))),
        field(name = "opt_simple_id",
            value(opt, item(rel = "SimpleEntity", prim = "Ulid"))
        ),
        field(name = "simples_ids",
            value(many, item(rel = "SimpleEntity", prim = "Ulid"))
        )
    ),
    timestamps
)]
pub struct RelatedEntity {}

///
/// SimpleEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct SimpleEntity {}

///
/// RecordA
///

#[record(fields(
    field(
        name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ),
    field(name = "description", value(item(prim = "Text", unbounded))),
))]
pub struct RecordA {}

///
/// RecordB
///

#[record(fields(
    field(name = "name", value(item(prim = "Text", unbounded))),
    field(name = "name_opt", value(opt, item(prim = "Text", unbounded)))
))]
pub struct RecordB {}

///
/// RecordC
///

#[record(fields(field(name = "prim", value(item(prim = "Text", unbounded)))))]
pub struct RecordC {}

///
/// EnumA
///

#[enum_(
    variant(name = "A"),
    variant(name = "B", value(item(prim = "Text", unbounded))),
    variant(name = "C", value(item(is = "RecordB"))),
    variant(name = "D", value(item(is = "RecordC")))
)]
pub struct EnumA {}

///
/// EnumB
///

#[enum_(variant(name = "F"), variant(name = "G"))]
pub struct EnumB {}

///
/// EnumC
///

#[enum_(
    variant(name = "F", value(item(prim = "Text", unbounded))),
    variant(name = "I", value(item(is = "RecordB")))
)]
pub struct EnumC {}

///
/// List
///

#[list(item(prim = "Text", unbounded))]
pub struct List {}

///
/// Map
///

#[map(key(prim = "Nat8"), value(item(prim = "Text", unbounded)))]
pub struct Map {}

///
/// Set
///

#[set(item(prim = "Text", unbounded))]
pub struct Set {}

///
/// Newtype
///

#[newtype(item(prim = "Text", unbounded))]
pub struct Newtype {}

///
/// Tuple
///

#[tuple(
    value(item(prim = "Text", unbounded)),
    value(item(prim = "Text", unbounded))
)]
pub struct Tuple {}
