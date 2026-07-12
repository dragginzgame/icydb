use crate::types::{
    Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
    Timestamp, Ulid,
};
use candid::CandidType;
use serde::Deserialize;

#[cfg(test)]
use crate::value::Value;

//
// OutputValue
//
// Public output-side value boundary used by API and wire surfaces.
// This stays separate from runtime `Value` so public result payloads can move
// off the internal execution representation without forcing a storage or
// planner rewrite in the same slice.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum OutputValue {
    Account(Account),
    Blob(Vec<u8>),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(OutputValueEnum),
    Float32(Float32),
    Float64(Float64),
    #[serde(rename = "Int")]
    Int64(i64),
    Int128(i128),
    IntBig(IntBig),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    #[serde(rename = "Nat")]
    Nat64(u64),
    Nat128(u128),
    NatBig(NatBig),
    Ulid(Ulid),
    Unit,
}

//
// OutputValueEnum
//
// Output-side enum payload contract paired with `OutputValue`.
// Payload stays recursive through `OutputValue` so public boundary conversion
// remains total for data-carrying enum values already representable by
// runtime `Value`.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OutputValueEnum {
    variant: String,
    path: Option<String>,
    payload: Option<Box<OutputValue>>,
}

impl OutputValueEnum {
    /// Build output enum metadata resolved from one accepted catalog revision.
    #[must_use]
    pub(crate) fn from_catalog_parts(
        variant: &str,
        path: &str,
        payload: Option<OutputValue>,
    ) -> Self {
        Self {
            variant: variant.to_string(),
            path: Some(path.to_string()),
            payload: payload.map(Box::new),
        }
    }

    #[must_use]
    pub const fn variant(&self) -> &str {
        self.variant.as_str()
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    pub fn payload(&self) -> Option<&OutputValue> {
        self.payload.as_deref()
    }
}

#[cfg(test)]
impl From<Value> for OutputValue {
    fn from(value: Value) -> Self {
        output_value_from_non_enum_test_value(&value)
    }
}

#[cfg(test)]
fn output_value_from_non_enum_test_value(value: &Value) -> OutputValue {
    match value {
        Value::Account(value) => OutputValue::Account(*value),
        Value::Blob(value) => OutputValue::Blob(value.clone()),
        Value::Bool(value) => OutputValue::Bool(*value),
        Value::Date(value) => OutputValue::Date(*value),
        Value::Decimal(value) => OutputValue::Decimal(*value),
        Value::Duration(value) => OutputValue::Duration(*value),
        Value::Enum(_) => panic!("test output conversion requires accepted enum catalog"),
        Value::Float32(value) => OutputValue::Float32(*value),
        Value::Float64(value) => OutputValue::Float64(*value),
        Value::Int64(value) => OutputValue::Int64(*value),
        Value::Int128(value) => OutputValue::Int128(*value),
        Value::IntBig(value) => OutputValue::IntBig(value.clone()),
        Value::List(values) => OutputValue::List(
            values
                .iter()
                .map(output_value_from_non_enum_test_value)
                .collect(),
        ),
        Value::Map(entries) => OutputValue::Map(
            entries
                .iter()
                .map(|(key, value)| {
                    (
                        output_value_from_non_enum_test_value(key),
                        output_value_from_non_enum_test_value(value),
                    )
                })
                .collect(),
        ),
        Value::Null => OutputValue::Null,
        Value::Principal(value) => OutputValue::Principal(*value),
        Value::Subaccount(value) => OutputValue::Subaccount(*value),
        Value::Text(value) => OutputValue::Text(value.clone()),
        Value::Timestamp(value) => OutputValue::Timestamp(*value),
        Value::Nat64(value) => OutputValue::Nat64(*value),
        Value::Nat128(value) => OutputValue::Nat128(*value),
        Value::NatBig(value) => OutputValue::NatBig(value.clone()),
        Value::Ulid(value) => OutputValue::Ulid(*value),
        Value::Unit => OutputValue::Unit,
    }
}

/// Render one output value into a stable text form for row projection payloads.
#[must_use]
pub fn render_output_value_text(value: &OutputValue) -> String {
    match value {
        OutputValue::Account(v) => v.to_string(),
        OutputValue::Blob(v) => render_blob_value(v),
        OutputValue::Bool(v) => v.to_string(),
        OutputValue::Date(v) => v.to_string(),
        OutputValue::Decimal(v) => v.to_string(),
        OutputValue::Duration(v) => render_duration_value(v.as_millis()),
        OutputValue::Enum(v) => render_enum(v),
        OutputValue::Float32(v) => v.to_string(),
        OutputValue::Float64(v) => v.to_string(),
        OutputValue::Int64(v) => v.to_string(),
        OutputValue::Int128(v) => v.to_string(),
        OutputValue::IntBig(v) => v.to_string(),
        OutputValue::List(items) => render_list_value(items.as_slice()),
        OutputValue::Map(entries) => render_map_value(entries.as_slice()),
        OutputValue::Null => "null".to_string(),
        OutputValue::Principal(v) => v.to_string(),
        OutputValue::Subaccount(v) => v.to_string(),
        OutputValue::Text(v) => v.clone(),
        OutputValue::Timestamp(v) => v.as_millis().to_string(),
        OutputValue::Nat64(v) => v.to_string(),
        OutputValue::Nat128(v) => v.to_string(),
        OutputValue::NatBig(v) => v.to_string(),
        OutputValue::Ulid(v) => v.to_string(),
        OutputValue::Unit => "()".to_string(),
    }
}

fn render_blob_value(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(encode_hex_lower_output_value(bytes).as_str());

    rendered
}

fn encode_hex_lower_output_value(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut rendered = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let byte = *byte;
        rendered.push(char::from(HEX[usize::from(byte >> 4)]));
        rendered.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }

    rendered
}

fn render_duration_value(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

fn render_list_value(items: &[OutputValue]) -> String {
    let mut rendered = String::from("[");

    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_output_value_text(item).as_str());
    }

    rendered.push(']');

    rendered
}

fn render_map_value(entries: &[(OutputValue, OutputValue)]) -> String {
    let mut rendered = String::from("{");

    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_output_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_output_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

fn render_enum(value: &OutputValueEnum) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(render_output_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::value::{OutputValue, Value};

    #[test]
    fn output_value_from_runtime_value_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Nat64(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(
            OutputValue::from(runtime),
            OutputValue::List(vec![
                OutputValue::Nat64(7),
                OutputValue::Map(vec![(
                    OutputValue::Text("x".to_string()),
                    OutputValue::Bool(true),
                )]),
            ]),
        );
    }
}
