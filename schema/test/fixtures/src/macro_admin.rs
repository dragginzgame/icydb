use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ADMIN TESTS
/// set up to test the admin interface
///

///
/// ComplexEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_admin.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "string_test", ident = "string_test", value(item(prim = "Text", unbounded))),
        field(source_key = "principal_test", ident = "principal_test", value(item(prim = "Principal"))),
        field(source_key = "blob_test", ident = "blob_test", value(item(prim = "Blob", unbounded))),
        field(source_key = "int_candid", ident = "int_candid", value(item(prim = "IntBig"))),
        field(source_key = "int_8", ident = "int_8", value(item(prim = "Int8"))),
        field(source_key = "int_16", ident = "int_16", value(item(prim = "Int16"))),
        field(source_key = "int_32", ident = "int_32", value(item(prim = "Int32"))),
        field(source_key = "int_64", ident = "int_64", value(item(prim = "Int64"))),
        field(source_key = "nat_candid", ident = "nat_candid", value(item(prim = "NatBig"))),
        field(source_key = "nat_8", ident = "nat_8", value(item(prim = "Nat8"))),
        field(source_key = "nat_16", ident = "nat_16", value(item(prim = "Nat16"))),
        field(source_key = "nat_64", ident = "nat_64", value(item(prim = "Nat64"))),
        field(source_key = "e8s", ident = "e8s", value(item(is = "base::types::finance::E8s"))),
        field(source_key = "e18s", ident = "e18s", value(item(is = "base::types::finance::E18s"))),
        field(source_key = "float_32", ident = "float_32", value(item(prim = "Float32"))),
        field(source_key = "float_64", ident = "float_64", value(item(prim = "Float64"))),
        field(source_key = "bool_test", ident = "bool_test", value(item(prim = "Bool"))),
        field(source_key = "timestamp", ident = "timestamp", value(item(prim = "Timestamp"))),
        field(source_key = "utf8_test", ident = "utf8_test", value(item(is = "base::types::bytes::Utf8"))),
        field(source_key = "tuple_test", ident = "tuple_test", value(item(is = "Tuple"))),
        field(source_key = "name_many", ident = "name_many", value(many, item(prim = "Text", unbounded))),
        field(source_key = "name_opt", ident = "name_opt", value(opt, item(prim = "Text", unbounded))),
        field(source_key = "record_a", ident = "record_a", value(item(is = "RecordA"))),
        field(source_key = "record_opt", ident = "record_opt", value(opt, item(is = "RecordB"))),
        field(source_key = "record_many", ident = "record_many", value(many, item(is = "RecordB"))),
        field(source_key = "list", ident = "list", value(item(is = "List"))),
        field(source_key = "map", ident = "map", value(item(is = "Map"))),
        field(source_key = "set", ident = "set", value(item(is = "Set"))),
        field(source_key = "variant_complex", ident = "variant_complex", value(item(is = "EnumA"))),
        field(source_key = "variant_complex_opt", ident = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(source_key = "variant_complex_many", ident = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(source_key = "variant_simple", ident = "variant_simple", value(item(is = "EnumB"))),
        field(source_key = "variant_simple_many", ident = "variant_simple_many", value(many, item(is = "EnumB"))),
        field(source_key = "variant_simple_opt", ident = "variant_simple_opt", value(opt, item(is = "EnumB")))
    )
)]
pub struct ComplexEntity {}

///
/// AdminEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_admin.rs::entity::2",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "simple_text", ident = "simple_text", value(item(prim = "Text", unbounded))),
        field(source_key = "tuple_test", ident = "tuple_test", value(item(is = "Tuple"))),
        field(source_key = "text_many", ident = "text_many", value(many, item(prim = "Text", unbounded))),
        field(source_key = "text_opt", ident = "text_opt", value(opt, item(prim = "Text", unbounded))),
        field(source_key = "nat_32", ident = "nat_32", value(item(prim = "Nat32"))),
        field(source_key = "record_a", ident = "record_a", value(item(is = "RecordA"))),
        field(source_key = "record_opt", ident = "record_opt", value(opt, item(is = "RecordB"))),
        field(source_key = "record_many", ident = "record_many", value(many, item(is = "RecordB"))),
        field(source_key = "variant_complex", ident = "variant_complex", value(item(is = "EnumA"))),
        field(source_key = "variant_complex_opt", ident = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(source_key = "variant_complex_many", ident = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(source_key = "variant_simple", ident = "variant_simple", value(item(is = "EnumB"))),
        field(source_key = "variant_simple_opt", ident = "variant_simple_opt", value(opt, item(is = "EnumB"))),
        field(source_key = "variant_simple_many", ident = "variant_simple_many", value(many, item(is = "EnumB"))),
    )
)]
pub struct AdminEntity {}

///
/// RelatedEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_admin.rs::entity::3",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "simple_id", ident = "simple_id", value(item(rel = "SimpleEntity", prim = "Ulid"))),
        field(source_key = "opt_simple_id", ident = "opt_simple_id",
            value(opt, item(rel = "SimpleEntity", prim = "Ulid"))
        ),
        field(source_key = "simples_ids", ident = "simples_ids",
            value(many, item(rel = "SimpleEntity", prim = "Ulid"))
        )
    )
)]
pub struct RelatedEntity {}

///
/// SimpleEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_admin.rs::entity::4",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    name = "AdminSimpleEntity",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded)))
    )
)]
pub struct SimpleEntity {}

///
/// RecordA
///

#[record(
    source_key = "schema/test/fixtures/src/macro_admin.rs::record::1",
    fields(
        field(
            source_key = "id",
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(
            source_key = "description",
            ident = "description",
            value(item(prim = "Text", unbounded))
        ),
    )
)]
pub struct RecordA {}

///
/// RecordB
///

#[record(
    source_key = "schema/test/fixtures/src/macro_admin.rs::record::2",
    fields(
        field(
            source_key = "name",
            ident = "name",
            value(item(prim = "Text", unbounded))
        ),
        field(
            source_key = "name_opt",
            ident = "name_opt",
            value(opt, item(prim = "Text", unbounded))
        )
    )
)]
pub struct RecordB {}

///
/// RecordC
///

#[record(
    source_key = "schema/test/fixtures/src/macro_admin.rs::record::3",
    fields(field(
        source_key = "prim",
        ident = "prim",
        value(item(prim = "Text", unbounded))
    ))
)]
pub struct RecordC {}

///
/// EnumA
///

#[enum_(
    source_key = "schema/test/fixtures/src/macro_admin.rs::enum_::nested::1",
    variant(source_key = "A", ident = "A"),
    variant(source_key = "B", ident = "B", value(item(prim = "Text", unbounded))),
    variant(source_key = "C", ident = "C", value(item(is = "RecordB"))),
    variant(source_key = "D", ident = "D", value(item(is = "RecordC")))
)]
pub struct EnumA {}

///
/// EnumB
///

#[enum_(
    source_key = "schema/test/fixtures/src/macro_admin.rs::enum_::nested::2",
    variant(source_key = "F", ident = "F"),
    variant(source_key = "G", ident = "G")
)]
pub struct EnumB {}

///
/// EnumC
///

#[enum_(
    source_key = "schema/test/fixtures/src/macro_admin.rs::enum_::nested::3",
    variant(source_key = "F", ident = "F", value(item(prim = "Text", unbounded))),
    variant(source_key = "I", ident = "I", value(item(is = "RecordB")))
)]
pub struct EnumC {}

///
/// List
///

#[list(
    source_key = "schema/test/fixtures/src/macro_admin.rs::list::1",
    item(prim = "Text", unbounded)
)]
pub struct List {}

///
/// Map
///

#[map(
    source_key = "schema/test/fixtures/src/macro_admin.rs::map::1",
    key(prim = "Nat8"),
    value(item(prim = "Text", unbounded))
)]
pub struct Map {}

///
/// Set
///

#[set(
    source_key = "schema/test/fixtures/src/macro_admin.rs::set::1",
    item(prim = "Text", unbounded)
)]
pub struct Set {}

///
/// Newtype
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_admin.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded)
)]
pub struct Newtype {}

///
/// Tuple
///

#[tuple(
    source_key = "schema/test/fixtures/src/macro_admin.rs::tuple::1",
    value(item(prim = "Text", unbounded)),
    value(item(prim = "Text", unbounded))
)]
pub struct Tuple {}
