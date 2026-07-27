//! ICRC-3 application value declarations.
//!
//! This module owns only the reusable protocol-shaped Rust model. Persisted
//! row encoding and database runtime values remain database-owned concerns.

use crate::prelude::*;

/// Generic ICRC-3 value.
#[enum_(
    source_key = "crates/icydb/src/base/types/ic/icrc3.rs::enum_::nested::1",
    variant(
        source_key = "Array",
        ident = "Array",
        value(many, item(is = "Value", indirect))
    ),
    variant(
        source_key = "Blob",
        ident = "Blob",
        value(item(prim = "Blob", unbounded))
    ),
    variant(source_key = "Int", ident = "Int", value(item(prim = "Int64"))),
    variant(
        source_key = "Map",
        ident = "Map",
        value(item(is = "value::Map", indirect))
    ),
    variant(source_key = "Nat", ident = "Nat", value(item(prim = "Nat64"))),
    variant(
        source_key = "Text",
        ident = "Text",
        value(item(prim = "Text", unbounded))
    )
)]
pub struct Value {}

impl Value {
    /// Construct a text value.
    #[must_use]
    pub fn text(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

/// ICRC-3 nested value declarations.
pub mod value {
    use super::*;

    /// Map from text keys to ICRC-3 values.
    #[map(
        source_key = "crates/icydb/src/base/types/ic/icrc3.rs::map::nested::1",
        key(prim = "Text", unbounded),
        value(item(is = "Value"))
    )]
    pub struct Map {}
}

#[cfg(test)]
mod tests {
    use super::{Value, value::Map};

    #[test]
    fn protocol_shape_remains_recursive_without_database_codecs() {
        let value = Value::Array(vec![
            Box::new(Value::Nat(7)),
            Box::new(Value::Map(Box::new(Map::from(vec![(
                "label",
                Value::text("ready"),
            )])))),
        ]);

        assert!(matches!(value, Value::Array(values) if values.len() == 2));
    }
}
