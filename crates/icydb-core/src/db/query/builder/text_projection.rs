//! Module: query::builder::text_projection
//! Responsibility: shared narrow text-projection builder surface for fluent
//! terminals and SQL computed projection execution.
//! Does not own: generic query planning, grouped semantics, or SQL parsing.
//! Boundary: models the admitted text transform family and applies it to one
//! already-loaded scalar value.

use crate::{db::QueryError, traits::FieldValue, value::Value};

///
/// TextProjectionTransform
///
/// Canonical narrow text-projection transform taxonomy shared by fluent and
/// SQL computed projection surfaces.
/// This is intentionally limited to the admitted single-field text function
/// family already shipped on the SQL surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextProjectionTransform {
    Field,
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    Position,
    Replace,
    Substring,
}

impl TextProjectionTransform {
    /// Return the stable uppercase function label for this transform.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Field => "FIELD",
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Substring => "SUBSTRING",
        }
    }
}

///
/// TextProjectionExpr
///
/// Shared narrow text-projection expression over one source field.
/// This remains a terminal/projection helper, not a generic expression system.
/// Literal slots preserve the exact shipped SQL text-function argument family.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextProjectionExpr {
    field: String,
    transform: TextProjectionTransform,
    literal: Option<Value>,
    literal2: Option<Value>,
    literal3: Option<Value>,
}

impl TextProjectionExpr {
    /// Build one no-literal text projection over a source field.
    #[must_use]
    pub fn new(field: impl Into<String>, transform: TextProjectionTransform) -> Self {
        Self {
            field: field.into(),
            transform,
            literal: None,
            literal2: None,
            literal3: None,
        }
    }

    /// Build one text projection carrying one literal argument.
    #[must_use]
    pub fn with_literal(
        field: impl Into<String>,
        transform: TextProjectionTransform,
        literal: impl FieldValue,
    ) -> Self {
        Self {
            field: field.into(),
            transform,
            literal: Some(literal.to_value()),
            literal2: None,
            literal3: None,
        }
    }

    /// Build one text projection carrying two literal arguments.
    #[must_use]
    pub fn with_two_literals(
        field: impl Into<String>,
        transform: TextProjectionTransform,
        literal: impl FieldValue,
        literal2: impl FieldValue,
    ) -> Self {
        Self {
            field: field.into(),
            transform,
            literal: Some(literal.to_value()),
            literal2: Some(literal2.to_value()),
            literal3: None,
        }
    }

    /// Borrow the source field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Return the transform taxonomy for this projection expression.
    #[must_use]
    pub const fn transform(&self) -> TextProjectionTransform {
        self.transform
    }

    /// Borrow the first optional literal argument.
    #[must_use]
    pub const fn literal(&self) -> Option<&Value> {
        self.literal.as_ref()
    }

    /// Borrow the second optional literal argument.
    #[must_use]
    pub const fn literal2(&self) -> Option<&Value> {
        self.literal2.as_ref()
    }

    /// Borrow the third optional literal argument.
    #[must_use]
    pub const fn literal3(&self) -> Option<&Value> {
        self.literal3.as_ref()
    }

    /// Override the first optional literal argument.
    #[must_use]
    pub fn with_optional_literal(mut self, literal: Option<Value>) -> Self {
        self.literal = literal;
        self
    }

    /// Override the second optional literal argument.
    #[must_use]
    pub fn with_optional_second_literal(mut self, literal: Option<Value>) -> Self {
        self.literal2 = literal;
        self
    }

    /// Override the third optional literal argument.
    #[must_use]
    pub fn with_optional_third_literal(mut self, literal: Option<Value>) -> Self {
        self.literal3 = literal;
        self
    }

    /// Render the stable SQL-style output label for this projection.
    #[must_use]
    pub fn sql_label(&self) -> String {
        let function_name = self.transform.label();
        let field = self.field.as_str();

        match (
            self.transform,
            self.literal.as_ref(),
            self.literal2.as_ref(),
            self.literal3.as_ref(),
        ) {
            (TextProjectionTransform::Field, _, _, _) => field.to_string(),
            (TextProjectionTransform::Position, Some(literal), _, _) => format!(
                "{function_name}({}, {field})",
                render_text_projection_literal(literal),
            ),
            (
                TextProjectionTransform::StartsWith
                | TextProjectionTransform::EndsWith
                | TextProjectionTransform::Contains,
                Some(literal),
                _,
                _,
            ) => format!(
                "{function_name}({field}, {})",
                render_text_projection_literal(literal),
            ),
            (TextProjectionTransform::Replace, Some(from), Some(to), _) => format!(
                "{function_name}({field}, {}, {})",
                render_text_projection_literal(from),
                render_text_projection_literal(to),
            ),
            (
                TextProjectionTransform::Left | TextProjectionTransform::Right,
                Some(length),
                _,
                _,
            ) => {
                format!(
                    "{function_name}({field}, {})",
                    render_text_projection_literal(length),
                )
            }
            (TextProjectionTransform::Substring, Some(start), Some(len), _) => format!(
                "{function_name}({field}, {}, {})",
                render_text_projection_literal(start),
                render_text_projection_literal(len),
            ),
            (TextProjectionTransform::Substring, Some(start), None, _) => format!(
                "{function_name}({field}, {})",
                render_text_projection_literal(start),
            ),
            _ => format!("{function_name}({field})"),
        }
    }

    /// Apply this projection to one already-loaded scalar value.
    pub fn apply_value(&self, value: Value) -> Result<Value, QueryError> {
        match self.transform {
            TextProjectionTransform::Field => Ok(value),
            TextProjectionTransform::Trim
            | TextProjectionTransform::Ltrim
            | TextProjectionTransform::Rtrim
            | TextProjectionTransform::Lower
            | TextProjectionTransform::Upper
            | TextProjectionTransform::Length
            | TextProjectionTransform::Left
            | TextProjectionTransform::Right
            | TextProjectionTransform::StartsWith
            | TextProjectionTransform::EndsWith
            | TextProjectionTransform::Contains
            | TextProjectionTransform::Position
            | TextProjectionTransform::Replace
            | TextProjectionTransform::Substring => match value {
                Value::Null => Ok(Value::Null),
                Value::Text(text) => self.apply_non_null_text(text),
                other => Err(self.text_input_error(&other)),
            },
        }
    }

    // Build the deterministic text-input mismatch error for this projection.
    fn text_input_error(&self, other: &Value) -> QueryError {
        QueryError::unsupported_query(format!(
            "{}({}) requires text input, found {other:?}",
            self.transform.label(),
            self.field,
        ))
    }

    // Resolve the optional text literal argument used by the binary text helpers.
    fn text_literal(&self) -> Result<Option<&str>, QueryError> {
        match self.literal.as_ref() {
            Some(Value::Null) => Ok(None),
            Some(Value::Text(text)) => Ok(Some(text.as_str())),
            Some(other) => Err(QueryError::unsupported_query(format!(
                "{}({}, ...) requires text literal argument, found {other:?}",
                self.transform.label(),
                self.field,
            ))),
            None => Err(QueryError::invariant(format!(
                "{} projection item was missing its literal argument",
                self.transform.label(),
            ))),
        }
    }

    // Resolve the second optional text literal used by `REPLACE`.
    fn second_text_literal(&self) -> Result<Option<&str>, QueryError> {
        match self.literal2.as_ref() {
            Some(Value::Null) => Ok(None),
            Some(Value::Text(text)) => Ok(Some(text.as_str())),
            Some(other) => Err(QueryError::unsupported_query(format!(
                "{}({}, ..., ...) requires text literal argument, found {other:?}",
                self.transform.label(),
                self.field,
            ))),
            None => Err(QueryError::invariant(format!(
                "{} projection item was missing its second literal argument",
                self.transform.label(),
            ))),
        }
    }

    // Resolve one integer-like literal used by the numeric text helpers.
    fn numeric_literal(
        &self,
        label: &'static str,
        value: Option<&Value>,
    ) -> Result<Option<i64>, QueryError> {
        match value {
            Some(Value::Null) => Ok(None),
            Some(Value::Int(value)) => Ok(Some(*value)),
            Some(Value::Uint(value)) => Ok(Some(i64::try_from(*value).unwrap_or(i64::MAX))),
            Some(other) => Err(QueryError::unsupported_query(format!(
                "{}({}, ...) requires integer or NULL {label}, found {other:?}",
                self.transform.label(),
                self.field,
            ))),
            None if label == "length" => Ok(None),
            None => Err(QueryError::invariant(format!(
                "{} projection item was missing its {label} literal",
                self.transform.label(),
            ))),
        }
    }

    // Apply one numeric text transform using the current narrow contract.
    fn apply_numeric_text(&self, text: &str) -> Result<Value, QueryError> {
        match self.transform {
            TextProjectionTransform::Left => {
                let len = self.numeric_literal("length", self.literal.as_ref())?;

                Ok(match len {
                    Some(len) => Value::Text(left_chars(text, len)),
                    None => Value::Null,
                })
            }
            TextProjectionTransform::Right => {
                let len = self.numeric_literal("length", self.literal.as_ref())?;

                Ok(match len {
                    Some(len) => Value::Text(right_chars(text, len)),
                    None => Value::Null,
                })
            }
            TextProjectionTransform::Substring => {
                let start = self.numeric_literal("start", self.literal.as_ref())?;
                let len = self.numeric_literal("length", self.literal2.as_ref())?;

                Ok(match start {
                    Some(start) => Value::Text(substring_1_based(text, start, len)),
                    None => Value::Null,
                })
            }
            _ => Err(QueryError::invariant(
                "numeric text projection helper received a non-numeric transform",
            )),
        }
    }

    // Apply one nullable boolean text predicate after resolving the shared literal contract.
    fn apply_binary_text_predicate(
        &self,
        text: &str,
        predicate: impl FnOnce(&str, &str) -> bool,
    ) -> Result<Value, QueryError> {
        let literal = self.text_literal()?;

        Ok(match literal {
            Some(needle) => Value::Bool(predicate(text, needle)),
            None => Value::Null,
        })
    }

    // Apply one non-null text transform after the caller has already resolved
    // the source value.
    fn apply_non_null_text(&self, text: String) -> Result<Value, QueryError> {
        match self.transform {
            TextProjectionTransform::Field => Ok(Value::Text(text)),
            TextProjectionTransform::Trim => Ok(Value::Text(text.trim().to_string())),
            TextProjectionTransform::Ltrim => Ok(Value::Text(text.trim_start().to_string())),
            TextProjectionTransform::Rtrim => Ok(Value::Text(text.trim_end().to_string())),
            TextProjectionTransform::Lower => Ok(Value::Text(text.to_lowercase())),
            TextProjectionTransform::Upper => Ok(Value::Text(text.to_uppercase())),
            TextProjectionTransform::Length => {
                let len = u64::try_from(text.chars().count()).unwrap_or(u64::MAX);

                Ok(Value::Uint(len))
            }
            TextProjectionTransform::Left
            | TextProjectionTransform::Right
            | TextProjectionTransform::Substring => self.apply_numeric_text(text.as_str()),
            TextProjectionTransform::StartsWith => self
                .apply_binary_text_predicate(text.as_str(), |text, needle| {
                    text.starts_with(needle)
                }),
            TextProjectionTransform::EndsWith => self
                .apply_binary_text_predicate(text.as_str(), |text, needle| text.ends_with(needle)),
            TextProjectionTransform::Contains => self
                .apply_binary_text_predicate(text.as_str(), |text, needle| text.contains(needle)),
            TextProjectionTransform::Position => {
                let literal = self.text_literal()?;

                Ok(match literal {
                    Some(needle) => Value::Uint(text_position_1_based(text.as_str(), needle)),
                    None => Value::Null,
                })
            }
            TextProjectionTransform::Replace => {
                let from = self.text_literal()?;
                let to = self.second_text_literal()?;

                Ok(match (from, to) {
                    (Some(from), Some(to)) => Value::Text(text.replace(from, to)),
                    _ => Value::Null,
                })
            }
        }
    }
}

/// Build `TRIM(field)`.
#[must_use]
pub fn trim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Trim)
}

/// Build `LTRIM(field)`.
#[must_use]
pub fn ltrim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Ltrim)
}

/// Build `RTRIM(field)`.
#[must_use]
pub fn rtrim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Rtrim)
}

/// Build `LOWER(field)`.
#[must_use]
pub fn lower(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Lower)
}

/// Build `UPPER(field)`.
#[must_use]
pub fn upper(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Upper)
}

/// Build `LENGTH(field)`.
#[must_use]
pub fn length(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::new(field.as_ref().to_string(), TextProjectionTransform::Length)
}

/// Build `LEFT(field, length)`.
#[must_use]
pub fn left(field: impl AsRef<str>, length: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::Left,
        length,
    )
}

/// Build `RIGHT(field, length)`.
#[must_use]
pub fn right(field: impl AsRef<str>, length: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::Right,
        length,
    )
}

/// Build `STARTS_WITH(field, literal)`.
#[must_use]
pub fn starts_with(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::StartsWith,
        literal,
    )
}

/// Build `ENDS_WITH(field, literal)`.
#[must_use]
pub fn ends_with(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::EndsWith,
        literal,
    )
}

/// Build `CONTAINS(field, literal)`.
#[must_use]
pub fn contains(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::Contains,
        literal,
    )
}

/// Build `POSITION(literal, field)`.
#[must_use]
pub fn position(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::Position,
        literal,
    )
}

/// Build `REPLACE(field, from, to)`.
#[must_use]
pub fn replace(
    field: impl AsRef<str>,
    from: impl FieldValue,
    to: impl FieldValue,
) -> TextProjectionExpr {
    TextProjectionExpr::with_two_literals(
        field.as_ref().to_string(),
        TextProjectionTransform::Replace,
        from,
        to,
    )
}

/// Build `SUBSTRING(field, start)`.
#[must_use]
pub fn substring(field: impl AsRef<str>, start: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(
        field.as_ref().to_string(),
        TextProjectionTransform::Substring,
        start,
    )
}

/// Build `SUBSTRING(field, start, length)`.
#[must_use]
pub fn substring_with_length(
    field: impl AsRef<str>,
    start: impl FieldValue,
    length: impl FieldValue,
) -> TextProjectionExpr {
    TextProjectionExpr::with_two_literals(
        field.as_ref().to_string(),
        TextProjectionTransform::Substring,
        start,
        length,
    )
}

// Render one projection literal back into a stable SQL-style label fragment.
fn render_text_projection_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Int(value) => value.to_string(),
        Value::Uint(value) => value.to_string(),
        _ => "<invalid-text-literal>".to_string(),
    }
}

// Return the SQL-style one-based character position of `needle` in `haystack`.
fn text_position_1_based(haystack: &str, needle: &str) -> u64 {
    let Some(byte_index) = haystack.find(needle) else {
        return 0;
    };
    let char_offset = haystack[..byte_index].chars().count();

    u64::try_from(char_offset)
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

// Return the first `count` characters from `text` using character semantics.
fn left_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    text.chars()
        .take(usize::try_from(count).unwrap_or(usize::MAX))
        .collect()
}

// Return the last `count` characters from `text` using character semantics.
fn right_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    let count = usize::try_from(count).unwrap_or(usize::MAX);
    let total = text.chars().count();
    let skip = total.saturating_sub(count);

    text.chars().skip(skip).collect()
}

// Apply the narrow SQL-style `SUBSTRING(text, start, len?)` contract using
// 1-based character indexing.
fn substring_1_based(text: &str, start: i64, len: Option<i64>) -> String {
    if start <= 0 {
        return String::new();
    }
    if matches!(len, Some(length) if length <= 0) {
        return String::new();
    }

    let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
    let chars = text.chars().skip(start_index);

    match len {
        Some(length) => chars
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect(),
        None => chars.collect(),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_text_projection_renders_sql_label() {
        assert_eq!(lower("name").sql_label(), "LOWER(name)");
    }

    #[test]
    fn replace_text_projection_applies_shared_transform() {
        let value = replace("name", "Ada", "Eve")
            .apply_value(Value::Text("Ada Ada".to_string()))
            .expect("replace projection should apply");

        assert_eq!(value, Value::Text("Eve Eve".to_string()));
    }
}
