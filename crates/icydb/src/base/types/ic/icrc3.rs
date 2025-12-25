use crate::design::prelude::*;

///
/// Icrc3 Value
/// Generic value in accordance with ICRC-3
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "Array", value(many, item(is = "Value", indirect))),
    variant(ident = "Blob", value(item(prim = "Blob"))),
    variant(ident = "Int", value(item(prim = "Int64"))),
    variant(ident = "Map", value(item(is = "value::Map", indirect))),
    variant(ident = "Nat", value(item(prim = "Nat64"))),
    variant(ident = "Text", value(item(prim = "Text")))
)]
pub struct Value {}

impl Value {
    #[must_use]
    pub fn text(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

pub mod value {
    use super::*;

    ///
    /// Icrc3 Value Map
    ///

    #[map(key(prim = "Text"), value(item(is = "Value")))]
    pub struct Map {}
}
