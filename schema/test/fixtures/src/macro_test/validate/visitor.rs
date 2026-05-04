use icydb::design::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Lower"))
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
/// VisitorInner
///

#[record(fields(field(ident = "leaf", value(item(is = "VisitorLowerText")))))]
pub struct VisitorInner {}

///
/// VisitorOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLowerTextList"))),
    field(ident = "rec", value(item(is = "VisitorInner"))),
    field(ident = "tup", value(item(is = "VisitorLowerTextTuple"))),
    field(ident = "map", value(item(is = "VisitorLowerTextMap"))),
))]
pub struct VisitorOuter {}

///
/// VisitorLowerTextSetValidated
///

#[set(item(
    prim = "Text",
    unbounded,
    validator(path = "base::validator::text::case::Lower")
))]
pub struct VisitorLowerTextSetValidated {}

///
/// VisitorLowerTextKeyMapValidated
///

#[map(
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

#[record(fields(field(ident = "set", value(item(is = "VisitorLowerTextSetValidated")))))]
pub struct VisitorSetOuter {}

///
/// VisitorMapKeyOuter
///

#[record(fields(field(ident = "map", value(item(is = "VisitorLowerTextKeyMapValidated")))))]
pub struct VisitorMapKeyOuter {}

///
/// VisitorMapValueOuter
///

#[record(fields(field(ident = "map", value(item(is = "VisitorLowerTextValueMapValidated")))))]
pub struct VisitorMapValueOuter {}

///
/// VisitorLengthList
///

#[list(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthList {}

///
/// VisitorLengthSet
///

#[set(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthSet {}

///
/// VisitorLengthMap
///

#[map(
    key(prim = "Text", unbounded),
    value(item(prim = "Text", unbounded)),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthMap {}

///
/// VisitorLengthOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLengthList"))),
    field(ident = "set", value(item(is = "VisitorLengthSet"))),
    field(ident = "map", value(item(is = "VisitorLengthMap"))),
))]
pub struct VisitorLengthOuter {}
