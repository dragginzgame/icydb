use crate::{
    traits::Visitable,
    visitor::{PathSegment, ValidateVisitor, VisitorAdapter, VisitorError, perform_visit},
};

///
/// validate
/// Validate a visitable tree, collecting issues by path.
///
/// Validation is non-failing at the traversal level. All validation
/// issues are collected and returned to the caller, which may choose
/// how to interpret them.
///
pub fn validate(node: &dyn Visitable) -> Result<(), VisitorError> {
    let visitor = ValidateVisitor::new();
    let mut adapter = VisitorAdapter::new(visitor);

    perform_visit(&mut adapter, node, PathSegment::Empty);

    adapter.result().map_err(VisitorError::from)
}
