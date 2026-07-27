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

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    rules: &'static [SourceRule],
}

impl Type {
    #[must_use]
    pub const fn new(
        normalizers: &'static [TypeNormalizer],
        validators: &'static [TypeValidator],
        rules: &'static [SourceRule],
    ) -> Self {
        Self {
            normalizers,
            validators,
            rules,
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

    /// Borrow explicitly declared durable rules.
    #[must_use]
    pub const fn rules(&self) -> &'static [SourceRule] {
        self.rules
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
        for node in self.rules() {
            node.accept(v);
        }
    }
}

///
/// SourceRule
///
/// Compiler-authored durable rule template carried by one reusable type.
/// Fragment lowering instantiates it for each persisted field use; it is not
/// an application callback or a database runtime evaluator.
///

#[derive(Clone, Debug, Serialize)]
pub struct SourceRule {
    source_key: &'static str,
    kind: SourceRuleKind,
    args: Args,
}

impl SourceRule {
    /// Construct one explicit reusable rule template.
    #[must_use]
    pub const fn new(source_key: &'static str, kind: SourceRuleKind, args: Args) -> Self {
        Self {
            source_key,
            kind,
            args,
        }
    }

    /// Return the immutable base-rule identity.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
    }

    /// Return the frozen rule operation.
    #[must_use]
    pub const fn kind(&self) -> SourceRuleKind {
        self.kind
    }

    /// Borrow rule operands.
    #[must_use]
    pub const fn args(&self) -> &Args {
        &self.args
    }
}

impl ValidateNode for SourceRule {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "rule",
            self.source_key(),
            icydb_schema::RuleSourceKey::try_new,
        );
        let expected_args = match self.kind() {
            SourceRuleKind::NumericMinimum => 1,
            SourceRuleKind::LengthRange | SourceRuleKind::NumericRange => 2,
        };
        if self.args().0.len() != expected_args
            || self
                .args()
                .0
                .iter()
                .any(|arg| !matches!(arg, Arg::Number(_)))
        {
            err!(
                errs,
                "rule '{}' requires {expected_args} numeric argument(s)",
                self.source_key(),
            );
        }
        errs.result()
    }
}

impl VisitableNode for SourceRule {}

///
/// SourceRuleKind
///
/// Closed durable-rule vocabulary translated into accepted constraints.
/// This enum describes authoring metadata only; accepted schema owns runtime
/// evaluation after fragment lowering.
///

#[derive(Clone, Copy, Debug, Serialize)]
pub enum SourceRuleKind {
    /// Inclusive character/octet/collection length range.
    LengthRange,
    /// Inclusive numeric minimum.
    NumericMinimum,
    /// Inclusive numeric range.
    NumericRange,
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
