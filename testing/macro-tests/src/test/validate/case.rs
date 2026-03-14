#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_fixtures::macro_test::validate::case::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_case_text_validation() {
        assert_valid!(LowerCaseText::from("lowercase"));
        assert_invalid!(LowerCaseText::from("NotLower"));
    }

    #[test]
    fn lower_underscore_text_validation() {
        assert_valid!(LowerUnderscoreText::from("lower_case"));
        assert_invalid!(LowerUnderscoreText::from("Lower_Case"));
        assert_invalid!(LowerUnderscoreText::from("lowercase1"));
    }

    #[test]
    fn upper_case_text_validation() {
        assert_valid!(UpperCaseText::from("UPPERCASE"));
        assert_invalid!(UpperCaseText::from("Uppercase"));
    }

    #[test]
    fn upper_snake_text_validation() {
        assert_valid!(UpperSnakeText::from("HELLO_WORLD"));
        assert_invalid!(UpperSnakeText::from("Hello_World"));
        assert_invalid!(UpperSnakeText::from("HELLO-WORLD"));
    }

    #[test]
    fn snake_case_text_validation() {
        assert_valid!(SnakeCaseText::from("snake_case"));
        assert_invalid!(SnakeCaseText::from("Snake_Case"));
        assert_invalid!(SnakeCaseText::from("snake-case"));
    }

    #[test]
    fn kebab_case_text_validation() {
        assert_valid!(KebabCaseText::from("kebab-case"));
        assert_invalid!(KebabCaseText::from("Kebab-Case"));
        assert_invalid!(KebabCaseText::from("kebab_case"));
    }

    #[test]
    fn title_case_text_validation() {
        assert_valid!(TitleCaseText::from("The Lord of the Rings"));
        assert_invalid!(TitleCaseText::from("the lord of the rings"));
    }

    #[test]
    fn upper_camel_text_validation() {
        assert_valid!(UpperCamelText::from("UpperCamel"));
        assert_invalid!(UpperCamelText::from("upperCamel"));
    }

    #[test]
    fn snake_case_list_validation() {
        assert_valid!(SnakeCaseTextListValidated::from(vec![
            "snake_case".to_string(),
            "another_value".to_string(),
        ]));

        assert_invalid!(SnakeCaseTextListValidated::from(vec![
            "snake_case".to_string(),
            "InvalidCase".to_string(),
        ]));
    }

    #[test]
    fn upper_key_title_value_map_validation() {
        assert_valid!(UpperKeyTitleValueMapValidated::from(vec![
            ("OWNER".to_string(), "The Fellowship".to_string()),
            ("ADMIN".to_string(), "Guardians of the Gate".to_string()),
        ]));

        assert_invalid!(UpperKeyTitleValueMapValidated::from(vec![(
            "Owner".to_string(),
            "The Fellowship".to_string(),
        ),]));

        assert_invalid!(UpperKeyTitleValueMapValidated::from(vec![(
            "OWNER".to_string(),
            "the fellowship".to_string(),
        ),]));
    }

    #[test]
    fn kebab_case_set_validation() {
        assert_valid!(KebabCaseTextSetValidated::from(vec![
            "kebab-case".to_string(),
            "another-value".to_string(),
        ]));

        assert_invalid!(KebabCaseTextSetValidated::from(vec![
            "Kebab-Case".to_string(),
        ]));
    }
}
