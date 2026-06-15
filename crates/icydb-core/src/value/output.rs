use crate::{
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::CandidType;
use serde::Deserialize;

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

impl From<Value> for OutputValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Account(value) => Self::Account(value),
            Value::Blob(value) => Self::Blob(value),
            Value::Bool(value) => Self::Bool(value),
            Value::Date(value) => Self::Date(value),
            Value::Decimal(value) => Self::Decimal(value),
            Value::Duration(value) => Self::Duration(value),
            Value::Enum(value) => Self::Enum(OutputValueEnum::from(value)),
            Value::Float32(value) => Self::Float32(value),
            Value::Float64(value) => Self::Float64(value),
            Value::Int64(value) => Self::Int64(value),
            Value::Int128(value) => Self::Int128(value),
            Value::IntBig(value) => Self::IntBig(value),
            Value::List(items) => Self::List(items.into_iter().map(Self::from).collect()),
            Value::Map(entries) => Self::Map(
                entries
                    .into_iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(value),
            Value::Subaccount(value) => Self::Subaccount(value),
            Value::Text(value) => Self::Text(value),
            Value::Timestamp(value) => Self::Timestamp(value),
            Value::Nat64(value) => Self::Nat64(value),
            Value::Nat128(value) => Self::Nat128(value),
            Value::NatBig(value) => Self::NatBig(value),
            Value::Ulid(value) => Self::Ulid(value),
            Value::Unit => Self::Unit,
        }
    }
}

impl From<&Value> for OutputValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Account(value) => Self::Account(*value),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Bool(value) => Self::Bool(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Enum(value) => Self::Enum(OutputValueEnum::from(value)),
            Value::Float32(value) => Self::Float32(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Int128(value) => Self::Int128(*value),
            Value::IntBig(value) => Self::IntBig(value.clone()),
            Value::List(items) => Self::List(items.iter().map(Self::from).collect()),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(*value),
            Value::Subaccount(value) => Self::Subaccount(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Nat64(value) => Self::Nat64(*value),
            Value::Nat128(value) => Self::Nat128(*value),
            Value::NatBig(value) => Self::NatBig(value.clone()),
            Value::Ulid(value) => Self::Ulid(*value),
            Value::Unit => Self::Unit,
        }
    }
}

impl From<ValueEnum> for OutputValueEnum {
    fn from(value: ValueEnum) -> Self {
        let ValueEnum {
            variant,
            path,
            payload,
        } = value;

        Self {
            variant,
            path,
            payload: payload.map(|payload| Box::new(OutputValue::from(*payload))),
        }
    }
}

impl From<&ValueEnum> for OutputValueEnum {
    fn from(value: &ValueEnum) -> Self {
        Self {
            variant: value.variant().to_string(),
            path: value.path().map(ToString::to_string),
            payload: value
                .payload()
                .map(|payload| Box::new(OutputValue::from(payload))),
        }
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
    use crate::value::{OutputValue, OutputValueEnum, Value, ValueEnum};

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

    #[test]
    fn output_value_from_owned_blob_moves_payload_without_clone() {
        let bytes = vec![0x10, 0x20, 0x30];
        let original_ptr = bytes.as_ptr();

        let OutputValue::Blob(output) = OutputValue::from(Value::Blob(bytes)) else {
            panic!("owned blob conversion should preserve the blob variant");
        };

        assert_eq!(
            output.as_ptr(),
            original_ptr,
            "owned output conversion should move blob bytes instead of cloning them",
        );
    }

    #[test]
    fn output_value_enum_from_runtime_enum_keeps_payload() {
        let runtime =
            ValueEnum::new("Example", Some("test::OutputEnum")).with_payload(Value::Nat64(9));

        assert_eq!(
            OutputValueEnum::from(runtime),
            OutputValueEnum {
                variant: "Example".to_string(),
                path: Some("test::OutputEnum".to_string()),
                payload: Some(Box::new(OutputValue::Nat64(9))),
            },
        );
    }
}
