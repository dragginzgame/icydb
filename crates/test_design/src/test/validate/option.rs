use crate::prelude::*;

///
/// OptionalThreshold
///
/// Demonstrates optional validation: the field is only validated when set.
/// - `None` is allowed.
/// - `Some(n)` must satisfy `n > 10`.
///
/// The `opt` flag makes the field optional; the validator still
/// operates on the inner value when present.
///

#[record(fields(field(
    ident = "threshold",
    value(
        opt,
        item(prim = "Nat32", validator(path = "base::validator::num::Gt", args(10)))
    )
)))]
pub struct OptionalThreshold {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::validate;

    #[test]
    fn none_is_valid() {
        let value = OptionalThreshold { threshold: None };
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn above_threshold_is_valid() {
        let value = OptionalThreshold {
            threshold: Some(42),
        };
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn below_threshold_is_invalid() {
        let value = OptionalThreshold { threshold: Some(5) };
        assert!(validate(&value).is_err());
    }
}
