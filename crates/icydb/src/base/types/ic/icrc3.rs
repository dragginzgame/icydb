//! Module: base::types::ic::icrc3
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Icrc3 Value
/// Generic value in accordance with ICRC-3
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "Array", value(many, item(is = "Value", indirect))),
    variant(ident = "Blob", value(item(prim = "Blob", unbounded))),
    variant(ident = "Int", value(item(prim = "Int64"))),
    variant(ident = "Map", value(item(is = "value::Map", indirect))),
    variant(ident = "Nat", value(item(prim = "Nat64"))),
    variant(ident = "Text", value(item(prim = "Text", unbounded)))
)]
pub struct Value {}

impl Value {
    #[must_use]
    pub fn text(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl icydb_core::traits::RuntimeValueEncode for Value {
    fn to_value(&self) -> icydb_core::value::Value {
        match self {
            Self::Unspecified => icydb_core::value::Value::Unit,
            Self::Array(values) => icydb_core::value::Value::List(
                values
                    .iter()
                    .map(|value| icydb_core::traits::RuntimeValueEncode::to_value(value.as_ref()))
                    .collect(),
            ),
            Self::Blob(value) => icydb_core::value::Value::Blob(value.to_vec()),
            Self::Int(value) => icydb_core::value::Value::Int64(*value),
            Self::Map(value) => icydb_core::value::Value::Map(
                value
                    .iter()
                    .map(|(key, value)| {
                        (
                            icydb_core::value::Value::Text(key.clone()),
                            icydb_core::traits::RuntimeValueEncode::to_value(value),
                        )
                    })
                    .collect(),
            ),
            Self::Nat(value) => icydb_core::value::Value::Nat64(*value),
            Self::Text(value) => icydb_core::value::Value::Text(value.clone()),
        }
    }
}

impl icydb_core::traits::PersistedStructuredFieldCodec for Value {
    fn encode_persisted_structured_payload(
        &self,
    ) -> Result<Vec<u8>, icydb_core::error::InternalError> {
        let value = icydb_core::traits::RuntimeValueEncode::to_value(self);
        icydb_core::__macro::encode_non_enum_protocol_value_bytes(&value)
    }

    fn decode_persisted_structured_payload(
        bytes: &[u8],
    ) -> Result<Self, icydb_core::error::InternalError> {
        let value = icydb_core::__macro::decode_non_enum_protocol_value_bytes(bytes)?;
        Self::from_protocol_value(value).ok_or_else(|| {
            icydb_core::__macro::generated_persisted_structured_payload_decode_failed(
                "invalid ICRC-3 protocol value",
            )
        })
    }
}

impl Value {
    fn from_protocol_value(value: icydb_core::value::Value) -> Option<Self> {
        match value {
            icydb_core::value::Value::Unit => Some(Self::Unspecified),
            icydb_core::value::Value::List(values) => values
                .into_iter()
                .map(Self::from_protocol_value)
                .map(|value| value.map(Box::new))
                .collect::<Option<Vec<_>>>()
                .map(Self::Array),
            icydb_core::value::Value::Blob(value) => {
                Some(Self::Blob(crate::types::Blob::from(value)))
            }
            icydb_core::value::Value::Int64(value) => Some(Self::Int(value)),
            icydb_core::value::Value::Map(entries) => entries
                .into_iter()
                .map(|(key, value)| {
                    let icydb_core::value::Value::Text(key) = key else {
                        return None;
                    };
                    Some((key, Self::from_protocol_value(value)?))
                })
                .collect::<Option<Vec<_>>>()
                .map(value::Map::from)
                .map(Box::new)
                .map(Self::Map),
            icydb_core::value::Value::Nat64(value) => Some(Self::Nat(value)),
            icydb_core::value::Value::Text(value) => Some(Self::Text(value)),
            _ => None,
        }
    }
}

pub mod value {
    use super::*;
    use crate::base::types::ic::icrc3::Value;

    ///
    /// Icrc3 Value Map
    ///

    #[map(key(prim = "Text", unbounded), value(item(is = "Value")))]
    pub struct Map {}
}

#[cfg(test)]
mod tests {
    use super::{Value, value::Map};
    use crate::{
        traits::Path,
        value::{InputValue, InputValueEnum},
    };
    use icydb_core::traits::PersistedStructuredFieldCodec;

    #[test]
    fn protocol_value_structural_codec_round_trips_without_catalog_enum_identity() {
        let value = Value::Array(vec![
            Box::new(Value::Nat(7)),
            Box::new(Value::Map(Box::new(Map::from(vec![(
                "label",
                Value::text("ready"),
            )])))),
        ]);
        let encoded = value
            .encode_persisted_structured_payload()
            .expect("ICRC-3 protocol value should encode");
        let decoded = Value::decode_persisted_structured_payload(encoded.as_slice())
            .expect("ICRC-3 protocol value should decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn generated_enum_input_conversion_stays_name_based_and_recursive() {
        let value = Value::Array(vec![
            Box::new(Value::Blob(crate::types::Blob::from(vec![1, 2, 3]))),
            Box::new(Value::text("nested")),
        ]);

        let InputValue::Enum(array) = InputValue::from(&value) else {
            panic!("generated enum should produce unresolved enum input");
        };
        assert_eq!(array.path(), Some(Value::PATH));
        assert_eq!(array.variant(), "Array");
        let Some(InputValue::List(items)) = array.payload() else {
            panic!("array variant should retain its recursive input payload");
        };
        assert_eq!(items.len(), 2);
        assert!(matches!(
            items.first(),
            Some(InputValue::Enum(value))
                if value.variant() == "Blob"
                    && matches!(value.payload(), Some(InputValue::Blob(bytes)) if bytes == &[1, 2, 3])
        ));
        assert!(matches!(
            items.get(1),
            Some(InputValue::Enum(value))
                if value.variant() == "Text"
                    && matches!(value.payload(), Some(InputValue::Text(text)) if text == "nested")
        ));

        let unit = InputValue::from(Value::Unspecified);
        assert_eq!(
            unit,
            InputValue::Enum(InputValueEnum::new("Unspecified", Some(Value::PATH))),
        );

        let mapped = InputValue::from(Value::Map(Box::new(Map::from(vec![(
            "answer",
            Value::Nat(42),
        )]))));
        assert!(matches!(
            mapped,
            InputValue::Enum(value)
                if matches!(
                    value.payload(),
                    Some(InputValue::Map(entries))
                        if matches!(
                            entries.as_slice(),
                            [(InputValue::Text(key), InputValue::Enum(nat))]
                                if key == "answer"
                                    && nat.variant() == "Nat"
                                    && matches!(nat.payload(), Some(InputValue::Nat64(42)))
                        )
                )
        ));
    }
}
