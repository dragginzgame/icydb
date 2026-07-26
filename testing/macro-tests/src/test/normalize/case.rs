#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::normalize::case::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::normalize;
    use std::collections::HashMap;

    #[test]
    fn lower_normalizer_to_lowercase() {
        let mut value = LowerCaseText::from("MiXeD Case");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "mixed case");
    }

    #[test]
    fn upper_normalizer_to_uppercase() {
        let mut value = UpperCaseText::from("MiXeD Case");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "MIXED CASE");
    }

    #[test]
    fn snake_normalizer_to_snake_case() {
        let mut value = SnakeCaseText::from("Mixed Case Text");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "mixed_case_text");
    }

    #[test]
    fn kebab_normalizer_to_kebab_case() {
        let mut value = KebabCaseText::from("Mixed Case Text");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "mixed-case-text");
    }

    #[test]
    fn title_normalizer_to_title_case() {
        let mut value = TitleCaseText::from("the lord of the rings");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "The Lord of the Rings");
    }

    #[test]
    fn upper_snake_normalizer_to_upper_snake_case() {
        let mut value = UpperSnakeText::from("Mixed Case Text");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "MIXED_CASE_TEXT");
    }

    #[test]
    fn upper_camel_normalizer_to_upper_camel_case() {
        let mut value = UpperCamelText::from("mixed case text");
        normalize(&mut value).unwrap();
        assert_eq!(value.inner().as_str(), "MixedCaseText");
    }

    #[test]
    fn snake_case_list_normalizes_all_entries() {
        let mut list = SnakeCaseTextList::from(vec![
            "Mixed Case Text".to_string(),
            "another Value".to_string(),
        ]);

        normalize(&mut list).unwrap();

        let expected = vec!["mixed_case_text".to_string(), "another_value".to_string()];
        let actual: Vec<_> = list.iter().map(|value| value.inner().clone()).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn title_case_value_map_normalizes_entries() {
        let mut map = TitleCaseValueMap::from(vec![
            (
                "account name".to_string(),
                "the fellowship of the ring".to_string(),
            ),
            ("owner".to_string(), "gandalf the grey".to_string()),
        ]);

        normalize(&mut map).unwrap();

        let actual: HashMap<_, _> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.inner().clone()))
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
