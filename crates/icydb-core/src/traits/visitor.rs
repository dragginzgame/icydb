//! Module: traits::visitor
//! Responsibility: visitable-node traits and default container traversal wiring.
//! Does not own: concrete sanitize/validate visitor implementations.
//! Boundary: structural traversal contract implemented by domain types.

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

// `Option` and `Vec` describe child structure here; their sanitize and
// validate hooks remain node-local no-ops. `Box` is transparent instead, so
// its hook forwarding supplies the boxed node's one logical hook call.

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

impl<T: SanitizeAuto> SanitizeAuto for Option<T> {}

impl<T: SanitizeAuto> SanitizeAuto for Vec<T> {}

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

impl<T: SanitizeCustom> SanitizeCustom for Option<T> {}

impl<T: SanitizeCustom> SanitizeCustom for Vec<T> {}

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

impl<T: ValidateAuto> ValidateAuto for Option<T> {}

impl<T: ValidateAuto> ValidateAuto for Vec<T> {}

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

impl<T: ValidateCustom> ValidateCustom for Option<T> {}

impl<T: ValidateCustom> ValidateCustom for Vec<T> {}

impl<T: ValidateCustom + ?Sized> ValidateCustom for Box<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        (**self).validate_custom(ctx);
    }
}

impl_primitive!(ValidateCustom);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sanitize::sanitize,
        validate::validate,
        visitor::{Issue, VisitorError},
    };
    use std::cell::Cell;

    const AUTO_SANITIZE_ISSUE: &str = "automatic sanitize";
    const CUSTOM_SANITIZE_ISSUE: &str = "custom sanitize";
    const AUTO_VALIDATE_ISSUE: &str = "automatic validate";
    const CUSTOM_VALIDATE_ISSUE: &str = "custom validate";

    #[derive(Default)]
    struct HookProbe {
        auto_sanitize: u32,
        custom_sanitize: u32,
        auto_validate: Cell<u32>,
        custom_validate: Cell<u32>,
    }

    impl Visitable for HookProbe {}

    impl SanitizeAuto for HookProbe {
        fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
            self.auto_sanitize += 1;
            ctx.issue(AUTO_SANITIZE_ISSUE);
        }
    }

    impl SanitizeCustom for HookProbe {
        fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
            self.custom_sanitize += 1;
            ctx.issue(CUSTOM_SANITIZE_ISSUE);
        }
    }

    impl ValidateAuto for HookProbe {
        fn validate_self(&self, ctx: &mut dyn VisitorContext) {
            self.auto_validate.set(self.auto_validate.get() + 1);
            ctx.issue(AUTO_VALIDATE_ISSUE);
        }
    }

    impl ValidateCustom for HookProbe {
        fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
            self.custom_validate.set(self.custom_validate.get() + 1);
            ctx.issue(CUSTOM_VALIDATE_ISSUE);
        }
    }

    fn assert_issues(error: &VisitorError, path: &str, expected: [&str; 2]) {
        let issues = error
            .issues()
            .get(path)
            .unwrap_or_else(|| panic!("expected visitor issues at {path}"));
        let messages = issues.iter().map(Issue::message).collect::<Vec<_>>();
        assert_eq!(messages, expected);
    }

    #[test]
    fn option_vec_sanitize_hooks_run_once_at_each_indexed_path() {
        let mut value = Some(vec![HookProbe::default(), HookProbe::default()]);

        let error = sanitize(&mut value).expect_err("probe sanitizers should report issues");

        let Some(probes) = value.as_ref() else {
            panic!("sanitize should preserve the populated option");
        };
        for probe in probes {
            assert_eq!(probe.auto_sanitize, 1);
            assert_eq!(probe.custom_sanitize, 1);
        }
        assert!(error.issues().get("").is_none());
        assert_issues(&error, "[0]", [AUTO_SANITIZE_ISSUE, CUSTOM_SANITIZE_ISSUE]);
        assert_issues(&error, "[1]", [AUTO_SANITIZE_ISSUE, CUSTOM_SANITIZE_ISSUE]);
    }

    #[test]
    fn option_vec_validate_hooks_run_once_at_each_indexed_path() {
        let value = Some(vec![HookProbe::default(), HookProbe::default()]);

        let error = validate(&value).expect_err("probe validators should report issues");

        let Some(probes) = value.as_ref() else {
            panic!("validate should preserve the populated option");
        };
        for probe in probes {
            assert_eq!(probe.auto_validate.get(), 1);
            assert_eq!(probe.custom_validate.get(), 1);
        }
        assert!(error.issues().get("").is_none());
        assert_issues(&error, "[0]", [AUTO_VALIDATE_ISSUE, CUSTOM_VALIDATE_ISSUE]);
        assert_issues(&error, "[1]", [AUTO_VALIDATE_ISSUE, CUSTOM_VALIDATE_ISSUE]);
    }

    #[test]
    fn box_transparency_keeps_one_forwarded_hook_call() {
        let mut sanitized = Box::new(HookProbe::default());
        let _ = sanitize(&mut sanitized).expect_err("probe sanitizers should report issues");
        assert_eq!(sanitized.auto_sanitize, 1);
        assert_eq!(sanitized.custom_sanitize, 1);

        let validated = Box::new(HookProbe::default());
        let _ = validate(&validated).expect_err("probe validators should report issues");
        assert_eq!(validated.auto_validate.get(), 1);
        assert_eq!(validated.custom_validate.get(), 1);
    }
}
