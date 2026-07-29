use icydb_model::prelude::*;

///
/// LowerCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// UpperCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperSnakeText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SnakeCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::UpperCamel"))
)]
pub struct UpperCamelText {}

///
/// SnakeCaseTextList
///

#[list(item(is = "SnakeCaseText"))]
pub struct SnakeCaseTextList {}

///
/// TitleCaseValueMap
///

#[map(key(prim = "Text", unbounded), value(item(is = "TitleCaseText")))]
pub struct TitleCaseValueMap {}
