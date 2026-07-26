use icydb::design::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::newtype::1",
    primitive = "Text",
    default = "String::new",
    item(prim = "Text", unbounded),
    ty(sanitizer(path = "base::sanitizer::text::case::Lower"))
)]
pub struct VisitorLowerText {}

///
/// VisitorLowerTextList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::list::1",
    item(is = "VisitorLowerText")
)]
pub struct VisitorLowerTextList {}

///
/// VisitorLowerTextTuple
///

#[tuple(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::tuple::1",
    value(item(is = "VisitorLowerText")),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextTuple {}

///
/// VisitorLowerTextMap
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::map::1",
    key(prim = "Text", unbounded),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextMap {}

///
/// VisitorOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::record::1",
    fields(
        field(
            source_key = "list",
            ident = "list",
            value(item(is = "VisitorLowerTextList"))
        ),
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
/// Reject
///

#[sanitizer]
pub struct Reject;

impl Sanitizer<String> for Reject {
    fn sanitize(&self, _value: &mut String) -> Result<(), String> {
        Err("rejected".to_string())
    }
}

///
/// VisitorRejectText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(sanitizer(path = "crate::macro_test::sanitize::visitor::Reject"))
)]
pub struct VisitorRejectText {}

///
/// VisitorRejectTextList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::list::2",
    item(is = "VisitorRejectText")
)]
pub struct VisitorRejectTextList {}

///
/// VisitorRejectTextMap
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::map::2",
    key(prim = "Text", unbounded),
    value(item(is = "VisitorRejectText"))
)]
pub struct VisitorRejectTextMap {}

///
/// VisitorRejectOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::record::2",
    fields(
        field(
            source_key = "field",
            ident = "field",
            value(item(
                prim = "Text",
                unbounded,
                sanitizer(path = "crate::macro_test::sanitize::visitor::Reject")
            ))
        ),
        field(
            source_key = "list",
            ident = "list",
            value(item(is = "VisitorRejectTextList"))
        ),
    )
)]
pub struct VisitorRejectOuter {}

///
/// VisitorRejectMapOuter
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/visitor.rs::record::3",
    fields(field(
        source_key = "map",
        ident = "map",
        value(item(is = "VisitorRejectTextMap"))
    ))
)]
pub struct VisitorRejectMapOuter {}
