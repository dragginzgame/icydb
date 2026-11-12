//! Explicit application-owned normalization and validation composition.
//!
//! This module does not perform database admission, persistence, retries, or
//! accepted-schema constraint evaluation.

use crate::{normalize, validate, visitor::Visitable, visitor::VisitorError};

/// Consuming normalize-then-validate convenience for application values.
///
/// The value is normalized first. Validation runs only if normalization
/// succeeds, and the normalized value is returned only after both traversals
/// succeed. The method consumes `self`, so it neither clones the value nor
/// creates a second callback owner.
pub trait NormalizeAndValidate: Visitable + Sized {
    /// Normalize and then validate this application-owned value.
    ///
    /// # Errors
    ///
    /// Returns a typed [`VisitorError`] from the first failing stage. Use
    /// [`VisitorError::operation`] to distinguish normalization from
    /// validation without inspecting diagnostic prose.
    fn normalize_and_validate(mut self) -> Result<Self, VisitorError> {
        normalize(&mut self)?;
        validate(&self)?;
        Ok(self)
    }
}

impl<T> NormalizeAndValidate for T where T: Visitable {}

#[cfg(test)]
mod tests;
