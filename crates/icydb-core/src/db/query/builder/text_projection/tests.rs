use super::*;
use crate::value::Value;

#[test]
fn lower_text_projection_renders_projection_label() {
    assert_eq!(lower("name").projection_label(), "LOWER(name)");
}

#[test]
fn replace_text_projection_applies_shared_transform() {
    let value = replace("name", "Ada", "Eve")
        .apply_value(Value::Text("Ada Ada".to_string()))
        .expect("replace projection should apply");

    assert_eq!(value, Value::Text("Eve Eve".to_string()));
}
