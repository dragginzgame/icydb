use icydb_model::prelude::*;

///
/// LowerCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// UpperCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperSnakeText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SnakeCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
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
