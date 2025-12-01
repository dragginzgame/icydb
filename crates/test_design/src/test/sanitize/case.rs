use crate::prelude::*;

///
/// LowerCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Lower"))
)]
pub struct LowerCaseText {}

///
/// UpperCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Upper"))
)]
pub struct UpperCaseText {}

///
/// UpperSnakeText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::UpperSnake"))
)]
pub struct UpperSnakeText {}

///
/// SnakeCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Snake"))
)]
pub struct SnakeCaseText {}

///
/// KebabCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Kebab"))
)]
pub struct KebabCaseText {}

///
/// TitleCaseText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Title"))
)]
pub struct TitleCaseText {}

///
/// UpperCamelText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::UpperCamel"))
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

#[map(key(prim = "Text"), value(item(is = "TitleCaseText")))]
pub struct TitleCaseValueMap {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::sanitize;
    use std::collections::HashMap;

    #[test]
    fn lower_sanitizer_to_lowercase() {
        let mut value = LowerCaseText::from("MiXeD Case");
        sanitize(&mut value);
        assert_eq!(&*value, "mixed case");
    }

    #[test]
    fn upper_sanitizer_to_uppercase() {
        let mut value = UpperCaseText::from("MiXeD Case");
        sanitize(&mut value);
        assert_eq!(&*value, "MIXED CASE");
    }

    #[test]
    fn snake_sanitizer_to_snake_case() {
        let mut value = SnakeCaseText::from("Mixed Case Text");
        sanitize(&mut value);
        assert_eq!(&*value, "mixed_case_text");
    }

    #[test]
    fn kebab_sanitizer_to_kebab_case() {
        let mut value = KebabCaseText::from("Mixed Case Text");
        sanitize(&mut value);
        assert_eq!(&*value, "mixed-case-text");
    }

    #[test]
    fn title_sanitizer_to_title_case() {
        let mut value = TitleCaseText::from("the lord of the rings");
        sanitize(&mut value);
        assert_eq!(&*value, "The Lord of the Rings");
    }

    #[test]
    fn upper_snake_sanitizer_to_upper_snake_case() {
        let mut value = UpperSnakeText::from("Mixed Case Text");
        sanitize(&mut value);
        assert_eq!(&*value, "MIXED_CASE_TEXT");
    }

    #[test]
    fn upper_camel_sanitizer_to_upper_camel_case() {
        let mut value = UpperCamelText::from("mixed case text");
        sanitize(&mut value);
        assert_eq!(&*value, "MixedCaseText");
    }

    #[test]
    fn snake_case_list_sanitizes_all_entries() {
        let mut list = SnakeCaseTextList::from(vec![
            "Mixed Case Text".to_string(),
            "another Value".to_string(),
        ]);

        sanitize(&mut list);

        let expected = vec!["mixed_case_text".to_string(), "another_value".to_string()];
        assert_eq!(*list, expected);
    }

    #[test]
    fn title_case_value_map_sanitizes_entries() {
        let mut map = TitleCaseValueMap::from(vec![
            (
                "account name".to_string(),
                "the fellowship of the ring".to_string(),
            ),
            ("owner".to_string(), "gandalf the grey".to_string()),
        ]);

        sanitize(&mut map);

        let actual: HashMap<_, _> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();

        let expected = HashMap::from([
            (
                "account name".to_string(),
                "The Fellowship of the Ring".to_string(),
            ),
            ("owner".to_string(), "Gandalf the Grey".to_string()),
        ]);

        assert_eq!(actual, expected);
    }
}
