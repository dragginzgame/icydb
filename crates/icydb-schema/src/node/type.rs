use crate::prelude::*;

///
/// Type
///
/// Canonical runtime type descriptor for one schema node's attached sanitizers
/// and validators.
///

#[derive(Clone, Debug, Serialize)]
pub struct Type {
    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    sanitizers: &'static [TypeSanitizer],

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],
}

impl Type {
    #[must_use]
    pub const fn new(
        sanitizers: &'static [TypeSanitizer],
        validators: &'static [TypeValidator],
    ) -> Self {
        Self {
            sanitizers,
            validators,
        }
    }

    #[must_use]
    pub const fn sanitizers(&self) -> &'static [TypeSanitizer] {
        self.sanitizers
    }

    #[must_use]
    pub const fn validators(&self) -> &'static [TypeValidator] {
        self.validators
    }
}

impl ValidateNode for Type {}

impl VisitableNode for Type {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.sanitizers() {
            node.accept(v);
        }
        for node in self.validators() {
            node.accept(v);
        }
    }
}

///
/// TypeSanitizer
///
/// Reference to one sanitizer node plus its bound argument list.
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeSanitizer {
    path: &'static str,
    args: Args,
}

impl TypeSanitizer {
    #[must_use]
    pub const fn new(path: &'static str, args: Args) -> Self {
        Self { path, args }
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    #[must_use]
    pub const fn args(&self) -> &Args {
        &self.args
    }
}

impl ValidateNode for TypeSanitizer {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // Resolve the referenced sanitizer path against the schema graph.
        let res = schema_read().check_node_as::<Sanitizer>(self.path());
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeSanitizer {}

///
/// TypeValidator
///
/// Reference to one validator node plus its bound argument list.
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeValidator {
    path: &'static str,
    args: Args,
}

impl TypeValidator {
    #[must_use]
    pub const fn new(path: &'static str, args: Args) -> Self {
        Self { path, args }
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    #[must_use]
    pub const fn args(&self) -> &Args {
        &self.args
    }
}

impl ValidateNode for TypeValidator {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // Resolve the referenced validator path against the schema graph.
        let res = schema_read().check_node_as::<Validator>(self.path());
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeValidator {}
