use crate::{
    Error, ThisError,
    traits::Visitable,
    visitor::{PathSegment, Visitor, VisitorAdapter, VisitorContext, VisitorError, perform_visit},
};
use std::collections::BTreeMap;

///
/// ValidateError
///

#[derive(Debug, ThisError)]
pub enum ValidateError {
    #[error("validation failed")]
    ValidationFailed(BTreeMap<String, Vec<String>>),

    #[error("invalid validator configuration: {0}")]
    InvalidConfig(String),
}

impl From<ValidateError> for Error {
    fn from(err: ValidateError) -> Self {
        VisitorError::from(err).into()
    }
}
///
/// validate
/// Validate a visitable tree, collecting issues by path.
///
pub fn validate(node: &dyn Visitable) -> Result<(), ValidateError> {
    let visitor = ValidateVisitor::new();
    let mut adapter = VisitorAdapter::new(visitor);

    perform_visit(&mut adapter, node, PathSegment::Empty);

    // Check fatal errors first
    let issues = adapter.issues().clone();
    adapter.finish()?;

    if issues.is_empty() {
        Ok(())
    } else {
        Err(ValidateError::ValidationFailed(issues))
    }
}

///
/// ValidateVisitor
///

#[derive(Debug, Default)]
pub struct ValidateVisitor;

impl ValidateVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Visitor<ValidateError> for ValidateVisitor {
    fn enter(
        &mut self,
        node: &dyn Visitable,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), ValidateError> {
        node.validate_self(ctx);
        node.validate_custom(ctx);

        Ok(())
    }

    fn exit(&mut self, _: &dyn Visitable) -> Result<(), ValidateError> {
        Ok(())
    }
}
