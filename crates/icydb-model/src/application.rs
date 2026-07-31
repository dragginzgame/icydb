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
mod tests {
    use std::{cell::Cell, rc::Rc};

    use crate::visitor::{
        ApplicationOperation, NormalizeAuto, NormalizeCustom, ValidateAuto, ValidateCustom,
        Visitable, VisitorContext,
    };

    use super::NormalizeAndValidate as _;

    #[derive(Debug)]
    struct Probe {
        value: String,
        validation_calls: Rc<Cell<u32>>,
        normalization_fails: bool,
    }

    impl Visitable for Probe {}

    impl NormalizeAuto for Probe {
        fn normalize_self(&mut self, ctx: &mut dyn VisitorContext) {
            self.value.make_ascii_lowercase();
            if self.normalization_fails {
                ctx.issue("normalization rejected");
            }
        }
    }

    impl NormalizeCustom for Probe {}

    impl ValidateAuto for Probe {
        fn validate_self(&self, ctx: &mut dyn VisitorContext) {
            self.validation_calls
                .set(self.validation_calls.get().saturating_add(1));
            if self.value != "accepted" {
                ctx.issue("validation rejected");
            }
        }
    }

    impl ValidateCustom for Probe {}

    #[test]
    fn composition_normalizes_before_validation_without_cloning() {
        let validation_calls = Rc::new(Cell::new(0));
        let probe = Probe {
            value: "ACCEPTED".to_string(),
            validation_calls: Rc::clone(&validation_calls),
            normalization_fails: false,
        };

        let normalized = probe
            .normalize_and_validate()
            .expect("normalized value should validate");

        assert_eq!(normalized.value, "accepted");
        assert_eq!(validation_calls.get(), 1);
    }

    #[test]
    fn composition_stops_before_validation_when_normalization_fails() {
        let validation_calls = Rc::new(Cell::new(0));
        let probe = Probe {
            value: "ACCEPTED".to_string(),
            validation_calls: Rc::clone(&validation_calls),
            normalization_fails: true,
        };

        let error = probe
            .normalize_and_validate()
            .expect_err("normalization issue should stop composition");

        assert_eq!(error.operation(), ApplicationOperation::Normalize);
        assert_eq!(validation_calls.get(), 0);
    }

    #[test]
    fn composition_reports_validation_as_the_failing_stage() {
        let validation_calls = Rc::new(Cell::new(0));
        let probe = Probe {
            value: "REJECTED".to_string(),
            validation_calls: Rc::clone(&validation_calls),
            normalization_fails: false,
        };

        let error = probe
            .normalize_and_validate()
            .expect_err("normalized rejected value should fail validation");

        assert_eq!(error.operation(), ApplicationOperation::Validate);
        assert_eq!(validation_calls.get(), 1);
    }
}
