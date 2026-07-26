use icydb::design::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::newtype::1",
    primitive = "Text",
    default = "String::new",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Lower"))
)]
pub struct VisitorLowerText {}

///
/// VisitorLowerTextList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::list::1",
    item(is = "VisitorLowerText")
)]
pub struct VisitorLowerTextList {}

///
/// VisitorLowerTextTuple
///

#[tuple(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::tuple::1",
    value(item(is = "VisitorLowerText")),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextTuple {}

///
/// VisitorLowerTextMap
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::map::1",
    key(prim = "Text", unbounded),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextMap {}

///
/// VisitorInner
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::1",
    fields(field(
        source_key = "leaf",
        ident = "leaf",
        value(item(is = "VisitorLowerText"))
    ))
)]
pub struct VisitorInner {}

///
/// VisitorOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::2",
    fields(
        field(
            source_key = "list",
            ident = "list",
            value(item(is = "VisitorLowerTextList"))
        ),
        field(source_key = "rec", ident = "rec", value(item(is = "VisitorInner"))),
        field(
            source_key = "tup",
            ident = "tup",
            value(item(is = "VisitorLowerTextTuple"))
        ),
        field(
            source_key = "map",
            ident = "map",
            value(item(is = "VisitorLowerTextMap"))
        ),
    )
)]
pub struct VisitorOuter {}

///
/// VisitorLowerTextSetValidated
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::set::1",
    item(
        prim = "Text",
        unbounded,
        validator(path = "base::validator::text::case::Lower")
    )
)]
pub struct VisitorLowerTextSetValidated {}

///
/// VisitorLowerTextKeyMapValidated
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::map::2",
    key(
        prim = "Text",
        unbounded,
        validator(path = "base::validator::text::case::Lower")
    ),
    value(item(prim = "Text", unbounded))
)]
pub struct VisitorLowerTextKeyMapValidated {}

///
/// VisitorLowerTextValueMapValidated
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::map::3",
    key(prim = "Text", unbounded),
    value(item(
        prim = "Text",
        unbounded,
        validator(path = "base::validator::text::case::Lower")
    ))
)]
pub struct VisitorLowerTextValueMapValidated {}

///
/// VisitorSetOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::3",
    fields(field(
        source_key = "set",
        ident = "set",
        value(item(is = "VisitorLowerTextSetValidated"))
    ))
)]
pub struct VisitorSetOuter {}

///
/// VisitorMapKeyOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::4",
    fields(field(
        source_key = "map",
        ident = "map",
        value(item(is = "VisitorLowerTextKeyMapValidated"))
    ))
)]
pub struct VisitorMapKeyOuter {}

///
/// VisitorMapValueOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::5",
    fields(field(
        source_key = "map",
        ident = "map",
        value(item(is = "VisitorLowerTextValueMapValidated"))
    ))
)]
pub struct VisitorMapValueOuter {}

///
/// VisitorLengthList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::list::2",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthList {}

///
/// VisitorLengthSet
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::set::2",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthSet {}

///
/// VisitorLengthMap
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::map::4",
    key(prim = "Text", unbounded),
    value(item(prim = "Text", unbounded)),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthMap {}

///
/// VisitorLengthOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/visitor.rs::record::6",
    fields(
        field(
            source_key = "list",
            ident = "list",
            value(item(is = "VisitorLengthList"))
        ),
        field(
            source_key = "set",
            ident = "set",
            value(item(is = "VisitorLengthSet"))
        ),
        field(
            source_key = "map",
            ident = "map",
            value(item(is = "VisitorLengthMap"))
        ),
    )
)]
pub struct VisitorLengthOuter {}
