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
    /// Return the concrete Rust application-value type used for callback
    /// diagnostics.
    fn type_identity(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

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
    fn type_identity(&self) -> &'static str {
        (**self).type_identity()
    }

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
    crate::schema::Principal,
    crate::schema::Subaccount,
    crate::schema::Timestamp,
    crate::schema::U256,
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
mod tests;
