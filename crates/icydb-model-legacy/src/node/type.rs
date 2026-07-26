use crate::prelude::*;

///
/// Type
///
/// Canonical runtime type descriptor for one schema node's attached normalizers
/// and validators.
///

#[derive(Clone, Debug, Serialize)]
pub struct Type {
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    normalizers: &'static [TypeNormalizer],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],
}

impl Type {
    #[must_use]
    pub const fn new(
        normalizers: &'static [TypeNormalizer],
        validators: &'static [TypeValidator],
    ) -> Self {
        Self {
            normalizers,
            validators,
        }
    }

    #[must_use]
    pub const fn normalizers(&self) -> &'static [TypeNormalizer] {
        self.normalizers
    }

    #[must_use]
    pub const fn validators(&self) -> &'static [TypeValidator] {
        self.validators
    }
}

impl ValidateNode for Type {}

impl VisitableNode for Type {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.normalizers() {
            node.accept(v);
        }
        for node in self.validators() {
            node.accept(v);
        }
    }
}

///
/// TypeNormalizer
///
/// Reference to one normalizer node plus its bound argument list.
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeNormalizer {
    path: &'static str,
    args: Args,
}

impl TypeNormalizer {
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

impl ValidateNode for TypeNormalizer {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // Resolve the referenced normalizer path against the schema graph.
        let res = schema_read().check_node_as::<Normalizer>(self.path());
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeNormalizer {}

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
