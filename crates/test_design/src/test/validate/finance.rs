use crate::prelude::*;

///
/// Usd
///

#[newtype(item(is = "base::types::finance::Usd"))]
pub struct Usd {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::{sanitize, types::Decimal, validate};
    use std::str::FromStr;

    #[test]
    fn valid_two_decimal_places() {
        let value = Usd::from(Decimal::from_str("12.34").unwrap());
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn rejects_negative() {
        let value = Usd::from(Decimal::from_str("-1.00").unwrap());
        assert!(validate(&value).is_err());
    }

    #[test]
    fn rejects_more_than_two_decimals() {
        let value = Usd::from(Decimal::from_str("1.234").unwrap());
        assert!(validate(&value).is_err());
    }

    #[test]
    fn sanitizer_rounds_to_two_decimals() {
        let mut value = Usd::from(Decimal::from_str("1.239").unwrap());
        sanitize(&mut value);
        assert_eq!(value.into_inner(), Decimal::from_str("1.24").unwrap());
    }
}
