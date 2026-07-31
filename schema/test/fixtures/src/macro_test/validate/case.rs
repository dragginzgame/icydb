use icydb_model::prelude::*;

///
/// CamelCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Camel"))
)]
pub struct CamelCaseText {}

///
/// LowerCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// LowerUnderscoreText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::LowerUscore"))
)]
pub struct LowerUnderscoreText {}

///
/// UpperCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperKebabText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperKebab"))
)]
pub struct UpperKebabText {}

///
/// UpperSnakeText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SentenceCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Sentence"))
)]
pub struct SentenceCaseText {}

///
/// SnakeCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperCamel"))
)]
pub struct UpperCamelText {}

///
/// SnakeCaseTextListValidated
///

#[list(item(is = "SnakeCaseText"))]
pub struct SnakeCaseTextListValidated {}

///
/// UpperKeyTitleValueMapValidated
///

#[map(
    key(
        prim = "Text",
        unbounded,
        validator(path = "base::validator::text::case::Upper")
    ),
    value(item(is = "TitleCaseText"))
)]
pub struct UpperKeyTitleValueMapValidated {}

///
/// KebabCaseTextSetValidated
///

#[set(item(is = "KebabCaseText"))]
pub struct KebabCaseTextSetValidated {}
