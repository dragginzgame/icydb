//! Accepted-check declarations retained by the host authoring graph.

use crate::prelude::*;
use icydb_schema::{SchemaContractError, SourceCheckExpr};

use crate::node::Schema;

/// Compiler-produced source-expression projection for one accepted check.
pub type SourceExpressionResolver = fn(&Schema) -> Result<SourceCheckExpr, SchemaContractError>;

///
/// CheckConstraint
///
/// Named accepted-check declaration. The SQL spelling remains authored input
/// until the compiler projection lowers it into the public source AST.
///

#[derive(Clone, Debug, Serialize)]
pub struct CheckConstraint {
    name: &'static str,
    check: &'static str,
    #[serde(skip)]
    expression: SourceExpressionResolver,
}

impl CheckConstraint {
    /// Construct one named accepted-check declaration.
    #[must_use]
    pub const fn new(
        name: &'static str,
        check: &'static str,
        expression: SourceExpressionResolver,
    ) -> Self {
        Self {
            name,
            check,
            expression,
        }
    }

    /// Borrow the current accepted-check name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrow the authored check expression.
    #[must_use]
    pub const fn check(&self) -> &'static str {
        self.check
    }

    /// Lower the compiler-validated expression into the public source AST.
    ///
    /// # Errors
    ///
    /// Returns a typed proposal error when an enum literal no longer resolves
    /// through the sealed graph or the expression violates public bounds.
    pub fn source_expression(
        &self,
        schema: &Schema,
    ) -> Result<SourceCheckExpr, SchemaContractError> {
        (self.expression)(schema)
    }
}

impl ValidateNode for CheckConstraint {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_name(
            &mut errs,
            "constraint",
            self.name(),
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
