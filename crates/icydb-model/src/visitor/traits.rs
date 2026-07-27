//! Module: visitor::traits
//! Responsibility: visitable-node traits and default container traversal wiring.
//! Does not own: concrete normalize/validate visitor implementations.
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
/// - Traversal is owned by the visitor, not by normalize/validate hooks.
/// - `drive` / `drive_mut` describe *structure only*.
/// - No validation or normalization logic lives here.
pub trait Visitable: Normalize + Validate {
    fn drive(&self, _: &mut dyn VisitorCore) {}
    fn drive_mut(&mut self, _: &mut dyn VisitorMutCore) {}
}

//
// -------------------- Container forwarding --------------------
//

// `Option` and `Vec` describe child structure here; their normalize and
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

impl<T: Visitable> Visitable for Box<T> {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        (**self).drive(visitor);
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        (**self).drive_mut(visitor);
    }
}

// Primitive leaf nodes: no structure
macro_rules! impl_primitive_visitable {
    ($($ty:ty),* $(,)?) => {
        $(impl Visitable for $ty {})*
    };
}

impl_primitive_visitable!(
    i8,
    i16,
    i32,
    i64,
    i128,
    u8,
    u16,
    u32,
    u64,
    u128,
    f32,
    f64,
    bool,
    String,
    crate::schema::Account,
    crate::schema::Blob,
    crate::schema::Date,
    crate::schema::Decimal,
    crate::schema::Duration,
    crate::schema::Float32,
    crate::schema::Float64,
    crate::schema::IntBig,
    crate::schema::NatBig,
    crate::schema::Principal,
    crate::schema::Subaccount,
    crate::schema::Timestamp,
    crate::schema::Ulid,
    crate::schema::Unit,
);

//
// ============================================================================
// Normalize
// ============================================================================
//

/// Marker trait: a type supports normalization.
pub trait Normalize: NormalizeAuto + NormalizeCustom {}

impl<T> Normalize for T where T: NormalizeAuto + NormalizeCustom {}

//
// -------------------- NormalizeAuto --------------------
//

/// Schema-defined normalization for this node only.
///
/// Rules:
/// - May mutate only `self`
/// - Must NOT recurse
/// - Must NOT fail-fast
/// - Must report issues via `VisitorContext`
pub trait NormalizeAuto {
    fn normalize_self(&mut self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: NormalizeAuto> NormalizeAuto for Option<T> {}

impl<T: NormalizeAuto> NormalizeAuto for Vec<T> {}

impl<T: NormalizeAuto + ?Sized> NormalizeAuto for Box<T> {
    fn normalize_self(&mut self, ctx: &mut dyn VisitorContext) {
        (**self).normalize_self(ctx);
    }
}

impl_primitive!(NormalizeAuto);

//
// -------------------- NormalizeCustom --------------------
//

/// User-defined normalization hooks.
///
/// Same rules as `NormalizeAuto`.
pub trait NormalizeCustom {
    fn normalize_custom(&mut self, _ctx: &mut dyn VisitorContext) {}
}

impl<T: NormalizeCustom> NormalizeCustom for Option<T> {}

impl<T: NormalizeCustom> NormalizeCustom for Vec<T> {}

impl<T: NormalizeCustom + ?Sized> NormalizeCustom for Box<T> {
    fn normalize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        (**self).normalize_custom(ctx);
    }
}

impl_primitive!(NormalizeCustom);

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

/// Transforms a value into a normalized version.
pub trait Normalizer<T> {
    fn normalize(&self, value: &mut T) -> Result<(), String>;

    fn normalize_with_context(
        &self,
        value: &mut T,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), String> {
        let _ = ctx;

        self.normalize(value)
    }
}

/// Allows a node to validate values.
pub trait Validator<T: ?Sized> {
    fn validate(&self, value: &T, ctx: &mut dyn VisitorContext);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        normalize::normalize,
        validate::validate,
        visitor::{Issue, VisitorError},
    };
    use std::cell::Cell;

    const AUTO_NORMALIZE_ISSUE: &str = "automatic normalize";
    const CUSTOM_NORMALIZE_ISSUE: &str = "custom normalize";
    const AUTO_VALIDATE_ISSUE: &str = "automatic validate";
    const CUSTOM_VALIDATE_ISSUE: &str = "custom validate";

    #[derive(Default)]
    struct HookProbe {
        auto_normalize: u32,
        custom_normalize: u32,
        auto_validate: Cell<u32>,
        custom_validate: Cell<u32>,
    }

    impl Visitable for HookProbe {}

    impl NormalizeAuto for HookProbe {
        fn normalize_self(&mut self, ctx: &mut dyn VisitorContext) {
            self.auto_normalize += 1;
            ctx.issue(AUTO_NORMALIZE_ISSUE);
        }
    }

    impl NormalizeCustom for HookProbe {
        fn normalize_custom(&mut self, ctx: &mut dyn VisitorContext) {
            self.custom_normalize += 1;
            ctx.issue(CUSTOM_NORMALIZE_ISSUE);
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
    fn option_vec_normalize_hooks_run_once_at_each_indexed_path() {
        let mut value = Some(vec![HookProbe::default(), HookProbe::default()]);

        let error = normalize(&mut value).expect_err("probe normalizers should report issues");

        let Some(probes) = value.as_ref() else {
            panic!("normalize should preserve the populated option");
        };
        for probe in probes {
            assert_eq!(probe.auto_normalize, 1);
            assert_eq!(probe.custom_normalize, 1);
        }
        assert!(error.issues().get("").is_none());
        assert_issues(
            &error,
            "[0]",
            [AUTO_NORMALIZE_ISSUE, CUSTOM_NORMALIZE_ISSUE],
        );
        assert_issues(
            &error,
            "[1]",
            [AUTO_NORMALIZE_ISSUE, CUSTOM_NORMALIZE_ISSUE],
        );
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
        let mut normalized = Box::new(HookProbe::default());
        let _ = normalize(&mut normalized).expect_err("probe normalizers should report issues");
        assert_eq!(normalized.auto_normalize, 1);
        assert_eq!(normalized.custom_normalize, 1);

        let validated = Box::new(HookProbe::default());
        let _ = validate(&validated).expect_err("probe validators should report issues");
        assert_eq!(validated.auto_validate.get(), 1);
        assert_eq!(validated.custom_validate.get(), 1);
    }
}
