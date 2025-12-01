use crate::prelude::*;

///
/// ADMIN TESTS
/// set up to test the admin interface
///

///
/// ComplexEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "string_test", value(item(prim = "Text"))),
        field(ident = "principal_test", value(item(prim = "Principal"))),
        field(ident = "blob_test", value(item(prim = "Blob"))),
        field(ident = "int_candid", value(item(prim = "Int"))),
        field(ident = "int_8", value(item(prim = "Int8"))),
        field(ident = "int_16", value(item(prim = "Int16"))),
        field(ident = "int_32", value(item(prim = "Int32"))),
        field(ident = "int_64", value(item(prim = "Int64"))),
        field(ident = "nat_candid", value(item(prim = "Nat"))),
        field(ident = "nat_8", value(item(prim = "Nat8"))),
        field(ident = "nat_16", value(item(prim = "Nat16"))),
        field(ident = "nat_64", value(item(prim = "Nat64"))),
        field(ident = "e8s", value(item(prim = "E8s"))),
        field(ident = "e18s", value(item(prim = "E18s"))),
        field(ident = "float_32", value(item(prim = "Float32"))),
        field(ident = "float_64", value(item(prim = "Float64"))),
        field(ident = "bool_test", value(item(prim = "Bool"))),
        field(ident = "timestamp", value(item(prim = "Timestamp"))),
        field(ident = "utf8_test", value(item(is = "base::types::bytes::Utf8"))),
        field(ident = "tuple_test", value(item(is = "Tuple"))),
        field(ident = "name_many", value(many, item(prim = "Text"))),
        field(ident = "name_opt", value(opt, item(prim = "Text"))),
        field(ident = "record_a", value(item(is = "RecordA"))),
        field(ident = "record_opt", value(opt, item(is = "RecordB"))),
        field(ident = "record_many", value(many, item(is = "RecordB"))),
        field(ident = "list", value(item(is = "List"))),
        field(ident = "map", value(item(is = "Map"))),
        field(ident = "set", value(item(is = "Set"))),
        field(ident = "variant_complex", value(item(is = "EnumA"))),
        field(ident = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(ident = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(ident = "variant_simple", value(item(is = "EnumB"))),
        field(ident = "variant_simple_many", value(many, item(is = "EnumB"))),
        field(ident = "variant_simple_opt", value(opt, item(is = "EnumB")))
    )
)]
pub struct ComplexEntity {}

///
/// AdminEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "simple_text", value(item(prim = "Text"))),
        field(ident = "tuple_test", value(item(is = "Tuple"))),
        field(ident = "text_many", value(many, item(prim = "Text"))),
        field(ident = "text_opt", value(opt, item(prim = "Text"))),
        field(ident = "nat_32", value(item(prim = "Nat32"))),
        field(ident = "record_a", value(item(is = "RecordA"))),
        field(ident = "record_opt", value(opt, item(is = "RecordB"))),
        field(ident = "record_many", value(many, item(is = "RecordB"))),
        field(ident = "variant_complex", value(item(is = "EnumA"))),
        field(ident = "variant_complex_opt", value(opt, item(is = "EnumA"))),
        field(ident = "variant_complex_many", value(many, item(is = "EnumA"))),
        field(ident = "variant_simple", value(item(is = "EnumB"))),
        field(ident = "variant_simple_opt", value(opt, item(is = "EnumB"))),
        field(ident = "variant_simple_many", value(many, item(is = "EnumB"))),
    )
)]
pub struct AdminEntity {}

///
/// RelatedEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "simple_id", value(item(rel = "SimpleEntity"))),
        field(ident = "opt_simple_id", value(opt, item(rel = "SimpleEntity"))),
        field(ident = "simple_ids", value(many, item(rel = "SimpleEntity")))
    )
)]
pub struct RelatedEntity {}

///
/// SimpleEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text")))
    )
)]
pub struct SimpleEntity {}

///
/// RecordA
///

#[record(fields(
    field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
    field(ident = "variant_a", value(item(is = "EnumA"))),
    field(ident = "description", value(item(prim = "Text"))),
))]
pub struct RecordA {}

///
/// RecordB
///

#[record(fields(
    field(ident = "name", value(item(prim = "Text"))),
    field(ident = "name_opt", value(opt, item(prim = "Text")))
))]
pub struct RecordB {}

///
/// RecordC
///

#[record(fields(field(ident = "prim", value(item(prim = "Text")))))]
pub struct RecordC {}

///
/// EnumA
///

#[enum_(
    variant(ident = "A", default),
    variant(ident = "B", value(item(prim = "Text"))),
    variant(ident = "C", value(item(is = "RecordB"))),
    variant(ident = "D", value(item(is = "RecordC")))
)]
pub struct EnumA {}

///
/// EnumB
///

#[enum_(variant(ident = "F", default), variant(ident = "G"))]
pub struct EnumB {}

///
/// EnumC
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "F", value(item(prim = "Text"))),
    variant(ident = "I", value(item(is = "RecordB")))
)]
pub struct EnumC {}

///
/// List
///

#[list(item(prim = "Text"))]
pub struct List {}

///
/// Map
///

#[map(key(prim = "Nat8"), value(item(prim = "Text")))]
pub struct Map {}

///
/// Set
///

#[set(item(prim = "Text"))]
pub struct Set {}

///
/// Newtype
///

#[newtype(primitive = "Text", item(prim = "Text"))]
pub struct Newtype {}

///
/// Tuple
///

#[tuple(value(item(prim = "Text")), value(item(prim = "Text")))]
pub struct Tuple {}
