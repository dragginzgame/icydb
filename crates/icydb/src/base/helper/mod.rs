//! Module: base::helper
//!
//! Responsibility: shared base helper routines.
//! Does not own: domain validation policy or runtime persistence.
//! Boundary: supports base sanitizers and validators with local conversions.

use crate::{design::prelude::*, traits::NumericValue};

/// Convert an arbitrary numeric value into `Decimal`.
pub(crate) fn try_cast_decimal<N: NumericValue>(value: &N) -> Option<Decimal> {
    value.try_to_decimal()
}
