use crate::visitor::{
    PathSegment, VisitorContext, VisitorCore, VisitorMutCore, perform_visit, perform_visit_mut,
};

//
// ============================================================================
// Visitable
// ============================================================================
//

/// A node that participates in visitor-based traversal.
///
/// Invariants:
/// - Traversal is owned by the visitor, not by sanitize/validate hooks.
/// - `drive` / `drive_mut` describe *structure only*.
/// - No validation or sanitization logic lives here.
pub trait Visitable: Sanitize + Validate {
    fn drive(&self, _: &mut dyn VisitorCore) {}
    fn drive_mut(&mut self, _: &mut dyn VisitorMutCore) {}
}

//
// -------------------- Container forwarding --------------------
//

impl<T: Visitable> Visitable for Option<T> {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        if let Some(value) = self.as_ref() {
            perform_visit(visitor, value, PathSegment::Empty);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        if let Some(value) = self.as_mut() {
            perform_visit_mut(visitor, value, PathSegment::Empty);
        }
    }
}

impl<T: Visitable> Visitable for Vec<T> {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        for (i, value) in self.iter().enumerate() {
            perform_visit(visitor, value, i);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        for (i, value) in self.iter_mut().enumerate() {
            perform_visit_mut(visitor, value, i);
        }
    }
}

impl<T: Visitable + ?Sized> Visitable for Box<T> {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        (**self).drive(visitor);
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        (**self).drive_mut(visitor);
    }
}

// Primitive leaf nodes: no structure
impl_primitive!(Visitable);

//
// ============================================================================
// Sanitize
// ============================================================================
//

/// Marker trait: a type supports sanitization.
pub trait Sanitize: SanitizeAuto + SanitizeCustom {}

impl<T> Sanitize for T where T: SanitizeAuto + SanitizeCustom {}

//
// -------------------- SanitizeAuto --------------------
//

/// Schema-defined sanitization for this node only.
///
/// Rules:
/// - May mutate only `self`
/// - Must NOT recurse
/// - Must NOT fail-fast
/// - Must report issues via `VisitorContext`
pub trait SanitizeAuto {
    fn sanitize_self(&mut self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: SanitizeAuto> SanitizeAuto for Option<T> {
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        if let Some(v) = self.as_mut() {
            v.sanitize_self(ctx);
        }
    }
}

impl<T: SanitizeAuto> SanitizeAuto for Vec<T> {
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_self(ctx);
        }
    }
}

impl<T: SanitizeAuto + ?Sized> SanitizeAuto for Box<T> {
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        (**self).sanitize_self(ctx);
    }
}

impl_primitive!(SanitizeAuto);

//
// -------------------- SanitizeCustom --------------------
//

/// User-defined sanitization hooks.
///
/// Same rules as `SanitizeAuto`.
pub trait SanitizeCustom {
    fn sanitize_custom(&mut self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: SanitizeCustom> SanitizeCustom for Option<T> {
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        if let Some(v) = self.as_mut() {
            v.sanitize_custom(ctx);
        }
    }
}

impl<T: SanitizeCustom> SanitizeCustom for Vec<T> {
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_custom(ctx);
        }
    }
}

impl<T: SanitizeCustom + ?Sized> SanitizeCustom for Box<T> {
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        (**self).sanitize_custom(ctx);
    }
}

impl_primitive!(SanitizeCustom);

//
// ============================================================================
// Validate
// ============================================================================
//

/// Marker trait: a type supports validation.
pub trait Validate: ValidateAuto + ValidateCustom {}

impl<T> Validate for T where T: ValidateAuto + ValidateCustom {}

//
// -------------------- ValidateAuto --------------------
//

/// Schema-defined validation for this node only.
///
/// Rules:
/// - Must NOT recurse
/// - Must NOT aggregate
/// - Must NOT return errors
/// - Must report issues via `VisitorContext`
pub trait ValidateAuto {
    fn validate_self(&self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: ValidateAuto> ValidateAuto for Option<T> {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        if let Some(v) = self.as_ref() {
            v.validate_self(ctx);
        }
    }
}

impl<T: ValidateAuto> ValidateAuto for Vec<T> {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_self(ctx);
        }
    }
}

impl<T: ValidateAuto + ?Sized> ValidateAuto for Box<T> {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        (**self).validate_self(ctx);
    }
}

impl_primitive!(ValidateAuto);

//
// -------------------- ValidateCustom --------------------
//

/// User-defined validation hooks.
///
/// Same rules as `ValidateAuto`.
pub trait ValidateCustom {
    fn validate_custom(&self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: ValidateCustom> ValidateCustom for Option<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        if let Some(v) = self.as_ref() {
            v.validate_custom(ctx);
        }
    }
}

impl<T: ValidateCustom> ValidateCustom for Vec<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_custom(ctx);
        }
    }
}

impl<T: ValidateCustom + ?Sized> ValidateCustom for Box<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        (**self).validate_custom(ctx);
    }
}

impl_primitive!(ValidateCustom);
