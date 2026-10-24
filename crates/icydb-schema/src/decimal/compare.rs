use crate::decimal::Decimal;
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

impl Decimal {
    pub(in crate::decimal) fn cmp_decimal(&self, other: &Self) -> Ordering {
        match self.scale.cmp(&other.scale) {
            Ordering::Equal => self.mantissa.cmp(&other.mantissa),
            Ordering::Less => {
                let aligned = Self::checked_pow10(other.scale - self.scale)
                    .and_then(|factor| self.mantissa.checked_mul(factor));
                // Scale deltas are at most 28, so the factor fits i128. If the
                // product overflows, its sign puts it beyond every rhs mantissa.
                aligned.map_or_else(|| self.mantissa.cmp(&0), |lhs| lhs.cmp(&other.mantissa))
            }
            Ordering::Greater => {
                let aligned = Self::checked_pow10(self.scale - other.scale)
                    .and_then(|factor| other.mantissa.checked_mul(factor));
                // The same bound applies with rhs aligned; reverse its ordering.
                aligned.map_or_else(|| 0.cmp(&other.mantissa), |rhs| self.mantissa.cmp(&rhs))
            }
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
