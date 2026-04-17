use crate::types::decimal::{DECIMAL_DIGIT_BUFFER_LEN, Decimal};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

impl Decimal {
    pub(in crate::types::decimal) fn cmp_decimal(&self, other: &Self) -> Ordering {
        let (lhs_m, lhs_s) = self.normalized_parts();
        let (rhs_m, rhs_s) = other.normalized_parts();

        if lhs_m == rhs_m && lhs_s == rhs_s {
            return Ordering::Equal;
        }

        if lhs_m == 0 {
            return if rhs_m.is_negative() {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        if rhs_m == 0 {
            return if lhs_m.is_negative() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        if lhs_m.is_negative() != rhs_m.is_negative() {
            return if lhs_m.is_negative() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let positive = !lhs_m.is_negative();
        let mut lhs_digits = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
        let mut rhs_digits = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
        let lhs_len = write_u128_decimal_digits(lhs_m.unsigned_abs(), &mut lhs_digits);
        let rhs_len = write_u128_decimal_digits(rhs_m.unsigned_abs(), &mut rhs_digits);

        let lhs_exponent = compare_exponent(lhs_s, lhs_len).unwrap_or(i64::MIN);
        let rhs_exponent = compare_exponent(rhs_s, rhs_len).unwrap_or(i64::MIN);

        let exponent_cmp = lhs_exponent.cmp(&rhs_exponent);
        if exponent_cmp != Ordering::Equal {
            return if positive {
                exponent_cmp
            } else {
                exponent_cmp.reverse()
            };
        }

        let significand_cmp =
            cmp_significand_digits(&lhs_digits[..lhs_len], &rhs_digits[..rhs_len]);
        if positive {
            significand_cmp
        } else {
            significand_cmp.reverse()
        }
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        let (lhs_m, lhs_s) = self.normalized_parts();
        let (rhs_m, rhs_s) = other.normalized_parts();
        lhs_m == rhs_m && lhs_s == rhs_s
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_decimal(other)
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (mantissa, scale) = self.normalized_parts();
        mantissa.hash(state);
        scale.hash(state);
    }
}

fn write_u128_decimal_digits(mut value: u128, out: &mut [u8; DECIMAL_DIGIT_BUFFER_LEN]) -> usize {
    let mut write_idx = DECIMAL_DIGIT_BUFFER_LEN;

    loop {
        write_idx = write_idx.saturating_sub(1);
        out[write_idx] = match value % 10 {
            0 => b'0',
            1 => b'1',
            2 => b'2',
            3 => b'3',
            4 => b'4',
            5 => b'5',
            6 => b'6',
            7 => b'7',
            8 => b'8',
            9 => b'9',
            _ => unreachable!("decimal digit remainder must be in 0..=9"),
        };
        value /= 10;

        if value == 0 {
            break;
        }
    }

    let len = DECIMAL_DIGIT_BUFFER_LEN.saturating_sub(write_idx);
    out.copy_within(write_idx..DECIMAL_DIGIT_BUFFER_LEN, 0);
    len
}

fn compare_exponent(scale: u32, digit_len: usize) -> Option<i64> {
    let digit_count = i64::try_from(digit_len).ok()?;
    let scale = i64::from(scale);
    digit_count.checked_sub(1)?.checked_sub(scale)
}

fn cmp_significand_digits(lhs: &[u8], rhs: &[u8]) -> Ordering {
    let width = lhs.len().max(rhs.len());
    for idx in 0..width {
        let l = lhs.get(idx).copied().unwrap_or(b'0');
        let r = rhs.get(idx).copied().unwrap_or(b'0');
        let cmp = l.cmp(&r);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    Ordering::Equal
}
