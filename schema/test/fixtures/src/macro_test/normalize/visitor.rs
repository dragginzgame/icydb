use icydb_model::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    default = "String::new",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
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

#[map(key(prim = "Text", unbounded), value(item(is = "VisitorLowerText")))]
pub struct VisitorLowerTextMap {}

///
/// VisitorOuter
///

#[record(fields(
    field(name = "list", value(item(is = "VisitorLowerTextList"))),
    field(name = "tup", value(item(is = "VisitorLowerTextTuple"))),
    field(name = "map", value(item(is = "VisitorLowerTextMap"))),
))]
pub struct VisitorOuter {}

///
/// Reject
///

#[normalizer]
pub struct Reject;

impl icydb_model::visitor::Normalizer<String> for Reject {
    fn normalize(&self, _value: &mut String) -> Result<(), String> {
        Err("rejected".to_string())
    }
}

///
/// VisitorRejectText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "crate::macro_test::normalize::visitor::Reject"))
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

#[map(key(prim = "Text", unbounded), value(item(is = "VisitorRejectText")))]
pub struct VisitorRejectTextMap {}

///
/// VisitorRejectOuter
///

#[record(fields(
    field(
        name = "field",
        value(item(
            prim = "Text",
            unbounded,
            normalizer(path = "crate::macro_test::normalize::visitor::Reject")
        ))
    ),
    field(name = "list", value(item(is = "VisitorRejectTextList"))),
))]
pub struct VisitorRejectOuter {}

///
/// VisitorRejectMapOuter
///

#[record(fields(field(name = "map", value(item(is = "VisitorRejectTextMap")))))]
pub struct VisitorRejectMapOuter {}
