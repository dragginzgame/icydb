use std::collections::{HashMap, HashSet};

///
/// Sanitize
///

pub trait Sanitize: SanitizeAuto + SanitizeCustom {}

impl<T> Sanitize for T where T: SanitizeAuto + SanitizeCustom {}

///
/// SanitizeContext
///
/// Context that can be provided during sanitization.
/// This may include runtime or request-specific data (timestamp, mode, actor, etc.).
///
/// NOTE: SanitizeContext is reserved for future context-aware sanitization.
/// The *_with() methods are currently thin wrappers that delegate to the
/// stateless versions. In the future, we may pass runtime data (e.g. now, is_new,
/// actor) here so sanitizers can behave contextually without changing the trait shape.
///

#[derive(Clone, Debug, Default)]
pub struct SanitizeContext {
    pub now: Option<u64>,
    pub is_new: Option<bool>,
}

impl SanitizeContext {
    #[must_use]
    pub const fn new(now: Option<u64>, is_new: Option<bool>) -> Self {
        Self { now, is_new }
    }
}

///
/// SanitizeAuto
///
/// Derived code that is used to generate sanitization rules for a type and
/// its children, via schema sanitizers.
///
/// This shouldn’t be used with primitive types directly, only for generated code.
///

pub trait SanitizeAuto {
    fn sanitize_self(&mut self) {}
    fn sanitize_children(&mut self) {}

    fn sanitize_self_with(&mut self, _ctx: &SanitizeContext) {
        self.sanitize_self();
    }

    fn sanitize_children_with(&mut self, _ctx: &SanitizeContext) {
        self.sanitize_children();
    }
}

impl<T: SanitizeAuto> SanitizeAuto for Box<T> {
    fn sanitize_self(&mut self) {
        (**self).sanitize_self();
    }

    fn sanitize_children(&mut self) {
        (**self).sanitize_children();
    }
}

impl<T: SanitizeAuto> SanitizeAuto for Option<T> {
    fn sanitize_self(&mut self) {
        if let Some(inner) = self.as_mut() {
            inner.sanitize_self();
        }
    }

    fn sanitize_children(&mut self) {
        if let Some(inner) = self.as_mut() {
            inner.sanitize_children();
        }
    }
}

impl<T: SanitizeAuto> SanitizeAuto for Vec<T> {
    fn sanitize_self(&mut self) {
        for v in self {
            v.sanitize_self();
        }
    }

    fn sanitize_children(&mut self) {
        for v in self {
            v.sanitize_children();
        }
    }
}

impl<T: SanitizeAuto, S> SanitizeAuto for HashSet<T, S> {
    fn sanitize_self(&mut self) {
        // keys must not change
    }

    fn sanitize_children(&mut self) {
        // keys must not change
    }
}

impl<K: SanitizeAuto, V: SanitizeAuto, S> SanitizeAuto for HashMap<K, V, S> {
    fn sanitize_self(&mut self) {
        for v in self.values_mut() {
            v.sanitize_self();
        }
    }

    fn sanitize_children(&mut self) {
        // keys must not change
        for v in self.values_mut() {
            v.sanitize_children();
        }
    }
}

impl_primitive!(SanitizeAuto);

///
/// SanitizeCustom
///
/// Custom sanitization behaviour that can be added to any type.
///

pub trait SanitizeCustom {
    fn sanitize_custom(&mut self) {}

    fn sanitize_custom_with(&mut self, _ctx: &SanitizeContext) {
        self.sanitize_custom();
    }
}

impl<T: SanitizeCustom> SanitizeCustom for Box<T> {
    fn sanitize_custom(&mut self) {
        (**self).sanitize_custom();
    }
}

impl<T: SanitizeCustom> SanitizeCustom for Option<T> {
    fn sanitize_custom(&mut self) {
        if let Some(inner) = self.as_mut() {
            inner.sanitize_custom();
        }
    }
}

impl<T: SanitizeCustom> SanitizeCustom for Vec<T> {
    fn sanitize_custom(&mut self) {
        for v in self {
            v.sanitize_custom();
        }
    }
}

impl<T: SanitizeCustom, S> SanitizeCustom for HashSet<T, S> {
    // keys must not change
}

impl<K: SanitizeCustom, V: SanitizeCustom, S> SanitizeCustom for HashMap<K, V, S> {
    fn sanitize_custom(&mut self) {
        // keys must not change
        for v in self.values_mut() {
            v.sanitize_custom();
        }
    }
}

impl_primitive!(SanitizeCustom);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    /// Dummy type that mutates itself during sanitization
    #[derive(Debug, Eq, Hash, PartialEq)]
    struct NeedsSanitizing(i32);

    impl SanitizeAuto for NeedsSanitizing {
        fn sanitize_self(&mut self) {
            if self.0 < 0 {
                self.0 = 0;
            }
        }
    }

    impl SanitizeCustom for NeedsSanitizing {
        fn sanitize_custom(&mut self) {
            if self.0 > 100 {
                self.0 = 100;
            }
        }
    }

    #[test]
    fn test_sanitize_auto_and_custom() {
        let mut x = NeedsSanitizing(-5);
        x.sanitize_self();
        assert_eq!(x.0, 0);

        let mut y = NeedsSanitizing(200);
        y.sanitize_custom();
        assert_eq!(y.0, 100);
    }

    #[test]
    fn test_vec_sanitization() {
        let mut v = vec![NeedsSanitizing(-1), NeedsSanitizing(150)];
        v.sanitize_self();
        v.sanitize_custom();
        assert_eq!(v[0].0, 0);
        assert_eq!(v[1].0, 100);
    }
}
