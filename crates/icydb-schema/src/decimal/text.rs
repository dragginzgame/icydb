//! Decimal text conversion; parsing borrows digits and float scratch stays on-stack.

use crate::decimal::{
    DECIMAL_DIGIT_BUFFER_LEN, Decimal, MAX_SUPPORTED_SCALE, ParseDecimalError,
    ParseDecimalErrorReason,
};
use std::{
    fmt::{Arguments, Display, Formatter},
    io::Write,
    str::FromStr,
};

impl Decimal {
    // Only the f32/f64 constructors use this adapter. Their unpadded Display
    // text has no redundant leading or fractional trailing zeros: an accepted
    // value needs at most sign + 39 integer digits + dot + 28 fractional digits.
    // Longer output already fails mantissa/scale admission. Keep the standard
    // formatter and parser as authorities, without heap strings or a fallback.
    pub(in crate::decimal) fn from_float_text(args: Arguments<'_>) -> Option<Self> {
        const CAPACITY: usize = 1 + DECIMAL_DIGIT_BUFFER_LEN + 1 + MAX_SUPPORTED_SCALE as usize;
        let mut bytes = [0; CAPACITY];
        let mut remaining = bytes.as_mut_slice();
        remaining.write_fmt(args).ok()?;
        let len = CAPACITY - remaining.len();
        Self::from_str(std::str::from_utf8(&bytes[..len]).ok()?).ok()
    }
}

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
            return Err(ParseDecimalError::new(ParseDecimalErrorReason::Empty));
        }

        let (negative, unsigned) = if let Some(rest) = input.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = input.strip_prefix('+') {
            (false, rest)
        } else {
            (false, input)
        };

        // Exponent notation is intentionally unsupported so decimal parsing
        // retains one predictable textual form.
        if unsigned.contains(['e', 'E']) {
            return Err(ParseDecimalError::new(
                ParseDecimalErrorReason::ExponentNotationUnsupported,
            ));
        }

        // Phase 2: parse base-10 digits and decimal point.
        let (int_digits, frac_digits) = split_decimal_significand(unsigned)?;
        let scale_i64 = i64::try_from(frac_digits.len()).map_err(|_| {
            ParseDecimalError::new(ParseDecimalErrorReason::FractionalLengthOverflow)
        })?;

        let scale = u32::try_from(scale_i64)
            .map_err(|_| ParseDecimalError::new(ParseDecimalErrorReason::ScaleOverflow))?;

        // Phase 3: accumulate already-validated digits without joining or
        // copying them. Subtract negative digits so i128::MIN remains valid;
        // leading zeros naturally leave the accumulator unchanged.
        let mantissa = int_digits
            .bytes()
            .chain(frac_digits.bytes())
            .try_fold(0i128, |mantissa, digit| {
                let mantissa = mantissa.checked_mul(10)?;
                let digit = i128::from(digit - b'0');
                if negative {
                    mantissa.checked_sub(digit)
                } else {
                    mantissa.checked_add(digit)
                }
            })
            .ok_or_else(|| ParseDecimalError::new(ParseDecimalErrorReason::MantissaOverflow))?;

        Self::checked_from_mantissa_scale(mantissa, scale).ok_or_else(|| {
            ParseDecimalError::new(ParseDecimalErrorReason::ScaleExceedsSupportedRange)
        })
    }
}

fn split_decimal_significand(input: &str) -> Result<(&str, &str), ParseDecimalError> {
    let mut segments = input.split('.');
    let int_digits = segments
        .next()
        .ok_or_else(|| ParseDecimalError::new(ParseDecimalErrorReason::InvalidSignificand))?;
    let frac_digits = segments.next().unwrap_or("");

    if segments.next().is_some() {
        return Err(ParseDecimalError::new(
            ParseDecimalErrorReason::InvalidSignificand,
        ));
    }

    if int_digits.is_empty() && frac_digits.is_empty() {
        return Err(ParseDecimalError::new(
            ParseDecimalErrorReason::InvalidSignificand,
        ));
    }

    if !int_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new(
            ParseDecimalErrorReason::InvalidDigits,
        ));
    }

    if !frac_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(ParseDecimalError::new(
            ParseDecimalErrorReason::InvalidDigits,
        ));
    }

    Ok((int_digits, frac_digits))
}
