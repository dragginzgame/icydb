use crate::types::decimal::{Decimal, ParseDecimalError};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

impl Display for Decimal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (mantissa, scale) = self.normalized_parts();

        if mantissa == 0 {
            return f.write_str("0");
        }

        let negative = mantissa.is_negative();
        let mut digits = mantissa.unsigned_abs().to_string();

        if scale == 0 {
            if negative {
                return write!(f, "-{digits}");
            }

            return f.write_str(&digits);
        }

        let scale_usize = usize::try_from(scale).map_err(|_| std::fmt::Error)?;

        if digits.len() <= scale_usize {
            let zeros = "0".repeat(scale_usize - digits.len());
            let body = format!("0.{zeros}{digits}");
            if negative {
                write!(f, "-{body}")
            } else {
                f.write_str(&body)
            }
        } else {
            let split = digits.len() - scale_usize;
            let frac = digits.split_off(split);
            if negative {
                write!(f, "-{digits}.{frac}")
            } else {
                write!(f, "{digits}.{frac}")
            }
        }
    }
}

impl FromStr for Decimal {
    type Err = ParseDecimalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Phase 1: parse sign.
        let input = s.trim();
        if input.is_empty() {
            return Err(ParseDecimalError::new("empty decimal string"));
        }

        let (negative, unsigned) = if let Some(rest) = input.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = input.strip_prefix('+') {
            (false, rest)
        } else {
            (false, input)
        };

        // Exponent notation is intentionally unsupported for predictable decimal
        // parsing semantics in 0.23.
        if unsigned.contains(['e', 'E']) {
            return Err(ParseDecimalError::new("exponent notation is not supported"));
        }

        // Phase 2: parse base-10 digits and decimal point.
        let (int_digits, frac_digits) = split_decimal_significand(unsigned)?;
        let combined = format!("{int_digits}{frac_digits}");
        let combined = strip_leading_zeros(&combined);

        let scale_i64 = i64::try_from(frac_digits.len())
            .map_err(|_| ParseDecimalError::new("decimal fractional length overflow"))?;
        let digits = combined.to_string();

        let scale = u32::try_from(scale_i64)
            .map_err(|_| ParseDecimalError::new("decimal scale overflow"))?;

        // Phase 3: materialize mantissa without floating-point fallback.
        let signed_digits = if negative {
            format!("-{digits}")
        } else {
            digits
        };
        let mantissa = signed_digits
            .parse::<i128>()
            .map_err(|_| ParseDecimalError::new("decimal mantissa overflow"))?;

        Self::checked_from_mantissa_scale(mantissa, scale)
            .ok_or_else(|| ParseDecimalError::new("decimal scale exceeds supported range"))
    }
}

fn split_decimal_significand(input: &str) -> Result<(&str, &str), ParseDecimalError> {
    let mut segments = input.split('.');
    let int_digits = segments
        .next()
        .ok_or_else(|| ParseDecimalError::new("invalid decimal significand"))?;
    let frac_digits = segments.next().unwrap_or("");

    if segments.next().is_some() {
        return Err(ParseDecimalError::new("invalid decimal significand"));
    }

    if int_digits.is_empty() && frac_digits.is_empty() {
        return Err(ParseDecimalError::new("invalid decimal significand"));
    }

    if !int_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new("invalid decimal digits"));
    }

    if !frac_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new("invalid decimal digits"));
    }

    Ok((int_digits, frac_digits))
}

fn strip_leading_zeros(digits: &str) -> &str {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() { "0" } else { trimmed }
}
