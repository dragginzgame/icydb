use crate::prelude::*;

///
/// Usd
///

#[newtype(item(is = "base::types::finance::Usd"))]
pub struct Usd {}

///
/// E8Fixed
///

#[newtype(item(is = "base::types::finance::E8s"))]
pub struct E8Fixed {}

///
/// E18Fixed
///

#[newtype(item(is = "base::types::finance::E18s"))]
pub struct E18Fixed {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{sanitize, types::Decimal, validate};
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
        sanitize(&mut value).unwrap();
        assert_eq!(value.into_inner(), Decimal::from_str("1.24").unwrap());
    }

    #[test]
    fn e8s_compat_accepts_up_to_8_decimal_places() {
        let value = E8Fixed::from(Decimal::from_str("12.12345678").unwrap());
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn e8s_compat_rejects_more_than_8_decimal_places() {
        let value = E8Fixed::from(Decimal::from_str("12.123456789").unwrap());
        assert!(validate(&value).is_err());
    }

    #[test]
    fn e18s_compat_accepts_up_to_18_decimal_places() {
        let value = E18Fixed::from(Decimal::from_str("1.123456789012345678").unwrap());
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn e18s_compat_rejects_negative_values() {
        let value = E18Fixed::from(Decimal::from_str("-0.1").unwrap());
        assert!(validate(&value).is_err());
    }
}
