use super::*;

#[test]
fn test_to_snake() {
    let test_cases = vec![
        ("PascalCase", "pascal_case"),
        ("camelCase", "camel_case"),
        ("Shape2D", "shape2_d"),
        ("Shape2d", "shape2d"),
        ("a a", "a_a"),
        ("a       a", "a_a"),
        ("CAPITALS", "capitals"),
        ("utf8", "utf8"),
        ("Utf8", "utf8"),
        ("UTF8", "utf8"),
        ("UTF8___UTF8", "utf8_utf8"),
        (" the the the ", "the_the_the"),
        ("MyExampleString123", "my_example_string123"),
        ("HTTPServer", "http_server"),
        ("XMLHttpRequest", "xml_http_request"),
        ("Already_Snake", "already_snake"),
        (" Mixed_Case And  Spacing ", "mixed_case_and_spacing"),
        ("TokenID", "token_id"),
        ("SomeURL123", "some_url123"),
        ("!@#$", ""),
        ("  _Hello_World_  ", "hello_world"),
    ];

    for (input, expected) in test_cases {
        assert_eq!(to_snake_case(input), expected);
    }
}
