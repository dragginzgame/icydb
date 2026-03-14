use icydb::design::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Lower"))
)]
pub struct VisitorLowerText {}

///
/// VisitorLowerTextList
///

#[list(item(is = "VisitorLowerText"))]
pub struct VisitorLowerTextList {}

///
/// VisitorLowerTextTuple
///

#[tuple(
    value(item(is = "VisitorLowerText")),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextTuple {}

///
/// VisitorLowerTextMap
///

#[map(key(prim = "Text"), value(item(is = "VisitorLowerText")))]
pub struct VisitorLowerTextMap {}

///
/// VisitorOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLowerTextList"))),
    field(ident = "tup", value(item(is = "VisitorLowerTextTuple"))),
    field(ident = "map", value(item(is = "VisitorLowerTextMap"))),
))]
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
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "crate::macro_test::sanitize::visitor::Reject"))
)]
pub struct VisitorRejectText {}

///
/// VisitorRejectTextList
///

#[list(item(is = "VisitorRejectText"))]
pub struct VisitorRejectTextList {}

///
/// VisitorRejectTextMap
///

#[map(key(prim = "Text"), value(item(is = "VisitorRejectText")))]
pub struct VisitorRejectTextMap {}

///
/// VisitorRejectOuter
///

#[record(fields(
    field(
        ident = "field",
        value(item(
            prim = "Text",
            sanitizer(path = "crate::macro_test::sanitize::visitor::Reject")
        ))
    ),
    field(ident = "list", value(item(is = "VisitorRejectTextList"))),
))]
pub struct VisitorRejectOuter {}

///
/// VisitorRejectMapOuter
///

#[record(fields(field(ident = "map", value(item(is = "VisitorRejectTextMap")))))]
pub struct VisitorRejectMapOuter {}
