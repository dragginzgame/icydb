//! Accepted-check declarations retained by the host authoring graph.

use crate::prelude::*;

///
/// CheckConstraint
///
/// Source-keyed accepted-check declaration. The SQL spelling remains authored
/// input until the compiler projection lowers it into the public source AST.
///

#[derive(Clone, Debug, Serialize)]
pub struct CheckConstraint {
    source_key: &'static str,
    name: &'static str,
    check: &'static str,
}

impl CheckConstraint {
    /// Construct one source-keyed accepted-check declaration.
    #[must_use]
    pub const fn new(source_key: &'static str, name: &'static str, check: &'static str) -> Self {
        Self {
            source_key,
            name,
            check,
        }
    }

    /// Borrow the immutable constraint source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
    }

    /// Borrow the editable accepted-check name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrow the authored check expression.
    #[must_use]
    pub const fn check(&self) -> &'static str {
        self.check
    }
}

impl ValidateNode for CheckConstraint {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "constraint",
            self.source_key(),
            icydb_schema::ConstraintSourceKey::try_new,
        );
        errs.result()
    }
}

impl VisitableNode for CheckConstraint {
    fn route_key(&self) -> String {
        self.name().to_string()
    }
}
