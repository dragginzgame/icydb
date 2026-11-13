use crate::{
    codec::hex::encode_hex_lower,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
        Ulid,
    },
    value::Value,
};

use serde::Deserialize;

/// Canonical recursive value carried by public input and output boundaries.
///
/// `InputValue` and `OutputValue` remain distinct root types. Their recursive
/// payload is shared here so Wire and Serde derive one value visitor family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub enum PublicValue {
    /// An account identified by its owner and subaccount.
    Account(Account),
    /// An uninterpreted sequence of bytes.
    Blob(#[serde(with = "serde_bytes")] Vec<u8>),
    /// A boolean truth value.
    Bool(bool),
    /// A calendar date.
    Date(Date),
    /// An exact fixed-point decimal.
    Decimal(Decimal),
    /// A duration measured in milliseconds.
    Duration(Duration),
    /// A nominal enum variant with its canonical payload.
    Enum(PublicEnumValue),
    /// A normalized finite 32-bit floating-point value.
    Float32(Float32),
    /// A normalized finite 64-bit floating-point value.
    Float64(Float64),
    #[serde(rename = "Int")]
    /// A signed 64-bit integer.
    Int64(i64),
    /// A signed 128-bit integer.
    Int128(i128),
    /// An ordered sequence of values.
    List(Vec<Self>),
    /// A collection of key/value entries.
    Map(Vec<(Self, Self)>),
    /// An absent optional value, represented as SQL NULL.
    Null,
    /// An Internet Computer principal identifier.
    Principal(Principal),
    /// A 32-byte account subidentifier.
    Subaccount(Subaccount),
    /// A UTF-8 string.
    Text(String),
    /// A timestamp measured in milliseconds since the Unix epoch.
    Timestamp(Timestamp),
    #[serde(rename = "Nat")]
    /// An unsigned 64-bit integer.
    Nat64(u64),
    /// An unsigned 128-bit integer.
    Nat128(u128),
    /// A 128-bit lexicographically sortable identifier.
    Ulid(Ulid),
    /// The unit value, carrying no payload.
    Unit,
    /// An unsigned 256-bit integer.
    U256(U256),
}

impl PublicValue {
    /// Render one public value into the stable row-projection text form.
    #[must_use]
    pub fn render_text(&self) -> String {
        match self {
            Self::Account(value) => value.to_string(),
            Self::Blob(value) => render_blob_value(value),
            Self::Bool(value) => value.to_string(),
            Self::Date(value) => value.to_string(),
            Self::Decimal(value) => value.to_string(),
            Self::Duration(value) => render_duration_value(value.as_millis()),
            Self::Enum(value) => render_enum(value),
            Self::Float32(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
            Self::Int64(value) => value.to_string(),
            Self::Int128(value) => value.to_string(),
            Self::List(items) => render_list_value(items),
            Self::Map(entries) => render_map_value(entries),
            Self::Null => "null".to_string(),
            Self::Principal(value) => value.to_string(),
            Self::Subaccount(value) => value.to_string(),
            Self::Text(value) => value.clone(),
            Self::Timestamp(value) => value.as_millis().to_string(),
            Self::Nat64(value) => value.to_string(),
            Self::Nat128(value) => value.to_string(),
            Self::Ulid(value) => value.to_string(),
            Self::Unit => "()".to_string(),
            Self::U256(value) => value.to_string(),
        }
    }

    pub(crate) fn try_into_runtime_non_enum(self) -> Option<Value> {
        Some(match self {
            Self::Account(value) => Value::Account(value),
            Self::Blob(value) => Value::Blob(value),
            Self::Bool(value) => Value::Bool(value),
            Self::Date(value) => Value::Date(value),
            Self::Decimal(value) => Value::Decimal(value),
            Self::Duration(value) => Value::Duration(value),
            Self::Enum(_) => return None,
            Self::Float32(value) => Value::Float32(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Int64(value) => Value::Int64(value),
            Self::Int128(value) => Value::Int128(value),
            Self::List(values) => Value::List(
                values
                    .into_iter()
                    .map(Self::try_into_runtime_non_enum)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Self::Map(entries) => Value::from_map(
                entries
                    .into_iter()
                    .map(|(key, value)| {
                        Some((
                            key.try_into_runtime_non_enum()?,
                            value.try_into_runtime_non_enum()?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            )
            .ok()?,
            Self::Null => Value::Null,
            Self::Principal(value) => Value::Principal(value),
            Self::Subaccount(value) => Value::Subaccount(value),
            Self::Text(value) => Value::Text(value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Nat64(value) => Value::Nat64(value),
            Self::Nat128(value) => Value::Nat128(value),
            Self::Ulid(value) => Value::Ulid(value),
            Self::Unit => Value::Unit,
            Self::U256(value) => Value::U256(value),
        })
    }

    pub(crate) fn try_from_runtime_non_enum(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Account(value) => Self::Account(*value),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Bool(value) => Self::Bool(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Enum(_) => return None,
            Value::Float32(value) => Self::Float32(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Int128(value) => Self::Int128(*value),
            Value::List(values) => Self::List(
                values
                    .iter()
                    .map(Self::try_from_runtime_non_enum)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| {
                        Some((
                            Self::try_from_runtime_non_enum(key)?,
                            Self::try_from_runtime_non_enum(value)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(*value),
            Value::Subaccount(value) => Self::Subaccount(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Nat64(value) => Self::Nat64(*value),
            Value::Nat128(value) => Self::Nat128(*value),
            Value::Ulid(value) => Self::Ulid(*value),
            Value::Unit => Self::Unit,
            Value::U256(value) => Self::U256(*value),
        })
    }
}

/// Enum metadata shared by public input and output values.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct PublicEnumValue {
    variant: String,
    path: Option<String>,
    payload: Option<Box<PublicValue>>,
}

impl PublicEnumValue {
    /// Build enum metadata for caller-authored input.
    #[must_use]
    pub fn new(variant: &str, path: Option<&str>) -> Self {
        Self {
            variant: variant.to_string(),
            path: path.map(ToString::to_string),
            payload: None,
        }
    }

    /// Build enum metadata whose type is resolved from its expected contract.
    #[must_use]
    pub fn loose(variant: impl Into<String>) -> Self {
        Self {
            variant: variant.into(),
            path: None,
            payload: None,
        }
    }

    /// Attach one unresolved public payload value.
    #[must_use]
    pub fn with_payload(mut self, payload: PublicValue) -> Self {
        self.payload = Some(Box::new(payload));
        self
    }

    /// Return the enum variant name.
    #[must_use]
    pub const fn variant(&self) -> &str {
        self.variant.as_str()
    }

    /// Return the optional schema-visible type path.
    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Return the optional recursive payload.
    #[must_use]
    pub fn payload(&self) -> Option<&PublicValue> {
        self.payload.as_deref()
    }

    pub(crate) fn from_catalog_parts(
        variant: &str,
        path: &str,
        payload: Option<PublicValue>,
    ) -> Self {
        Self {
            variant: variant.to_string(),
            path: Some(path.to_string()),
            payload: payload.map(Box::new),
        }
    }

    /// Consume enum metadata and its payload without copying.
    #[must_use]
    pub fn into_parts(self) -> (String, Option<String>, Option<PublicValue>) {
        (
            self.variant,
            self.path,
            self.payload.map(|payload| *payload),
        )
    }
}

fn render_blob_value(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(encode_hex_lower(bytes).as_str());
    rendered
}

fn render_duration_value(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");
    rendered
}

fn render_list_value(items: &[PublicValue]) -> String {
    let mut rendered = String::from("[");
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }
        rendered.push_str(item.render_text().as_str());
    }
    rendered.push(']');
    rendered
}

fn render_map_value(entries: &[(PublicValue, PublicValue)]) -> String {
    let mut rendered = String::from("{");
    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }
        rendered.push_str(key.render_text().as_str());
        rendered.push_str(": ");
        rendered.push_str(value.render_text().as_str());
    }
    rendered.push('}');
    rendered
}

fn render_enum(value: &PublicEnumValue) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(payload.render_text().as_str());
        rendered.push(')');
    }
    rendered
}
