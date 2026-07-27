use icydb_model::prelude::*;

///
/// LowerCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// UpperCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperSnakeText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::3",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SnakeCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::4",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::5",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::6",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::newtype::7",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::UpperCamel"))
)]
pub struct UpperCamelText {}

///
/// SnakeCaseTextList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::list::1",
    item(is = "SnakeCaseText")
)]
pub struct SnakeCaseTextList {}

///
/// TitleCaseValueMap
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/case.rs::map::1",
    key(prim = "Text", unbounded),
    value(item(is = "TitleCaseText"))
)]
pub struct TitleCaseValueMap {}
