use icydb::design::prelude::*;

///
/// CamelCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Camel"))
)]
pub struct CamelCaseText {}

///
/// LowerCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// LowerUnderscoreText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::3",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::LowerUscore"))
)]
pub struct LowerUnderscoreText {}

///
/// UpperCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::4",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperKebabText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::5",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperKebab"))
)]
pub struct UpperKebabText {}

///
/// UpperSnakeText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::6",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SentenceCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::7",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Sentence"))
)]
pub struct SentenceCaseText {}

///
/// SnakeCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::8",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::9",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::10",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::newtype::11",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(validator(path = "base::validator::text::case::UpperCamel"))
)]
pub struct UpperCamelText {}

///
/// SnakeCaseTextListValidated
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::list::1",
    item(is = "SnakeCaseText")
)]
pub struct SnakeCaseTextListValidated {}

///
/// UpperKeyTitleValueMapValidated
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::map::1",
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

#[set(
    source_key = "schema/test/fixtures/src/macro_test/validate/case.rs::set::1",
    item(is = "KebabCaseText")
)]
pub struct KebabCaseTextSetValidated {}
