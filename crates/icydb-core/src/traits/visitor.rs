use crate::visitor::{SanitizeIssue, VisitorContext, VisitorCore, VisitorMutCore};

///
/// Visitable
///

pub trait Visitable: Sanitize + Validate {
    fn drive(&self, _: &mut dyn VisitorCore) {}
    fn drive_mut(&mut self, _: &mut dyn VisitorMutCore) {}
}

impl_primitive!(Visitable);

///
/// Validate
///

pub trait Validate: ValidateAuto + ValidateCustom {}

impl<T> Validate for T where T: ValidateAuto + ValidateCustom {}

///
/// Sanitize
///

pub trait Sanitize: SanitizeAuto + SanitizeCustom {}

impl<T> Sanitize for T where T: SanitizeAuto + SanitizeCustom {}

///
/// SanitizeAuto
///
/// Auto-generated schema sanitization.
/// Must mutate only `self`.
/// Must NOT recurse.
/// May return a fatal `SanitizeIssue`.
///

pub trait SanitizeAuto {
    fn sanitize_self(&mut self, _ctx: &mut dyn VisitorContext) -> Result<(), SanitizeIssue> {
        Ok(())
    }
}

impl_primitive!(SanitizeAuto);

///
/// SanitizeCustom
///
/// User-defined sanitization hooks.
/// Same rules as `SanitizeAuto`.
///

pub trait SanitizeCustom {
    fn sanitize_custom(&mut self, _ctx: &mut dyn VisitorContext) -> Result<(), SanitizeIssue> {
        Ok(())
    }
}

impl_primitive!(SanitizeCustom);

///
/// ValidateAuto
///
/// Auto-generated schema validation.
/// Must NOT recurse, aggregate, or fail-fast.
/// Reports issues via `VisitorContext`.
///

pub trait ValidateAuto {
    /// Validate this node according to schema-defined rules.
    fn validate_self(&self, _ctx: &mut dyn VisitorContext) {}
}

impl_primitive!(ValidateAuto);

///
/// ValidateCustom
///
/// User-defined validation hooks.
/// Also must NOT recurse or aggregate.
///

pub trait ValidateCustom {
    /// Custom validation logic for this node.
    fn validate_custom(&self, _ctx: &mut dyn VisitorContext) {}
}

impl_primitive!(ValidateCustom);
