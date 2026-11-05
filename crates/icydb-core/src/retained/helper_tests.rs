//! Test-only constructors and adapters for the parent module.

use super::*;

impl RetainedBytes {
    pub(crate) fn measure<T: Retained + ?Sized>(value: &T, limit: usize) -> Option<usize> {
        let mut bytes = Self::new(limit);
        bytes.add(size_of_val(value))?;
        bytes.visit(value)?;
        Some(bytes.bytes)
    }
}
