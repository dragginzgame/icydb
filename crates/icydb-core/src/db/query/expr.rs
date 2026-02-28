//! Module: query::expr
//! Responsibility: schema-agnostic filter/sort expression wrappers and lowering.
//! Does not own: planner route selection or executor evaluation.
//! Boundary: intent boundary lowers these to validated predicate/order forms.

use crate::db::query::plan::{OrderDirection, PlanError, validate::validate_order};
use crate::db::{
    predicate::{
        Predicate, SchemaInfo, ValidateError, normalize, normalize_enum_literals,
        reject_unsupported_query_features, validate,
    },
    query::plan::OrderSpec,
};
use thiserror::Error as ThisError;

///
/// FilterExpr
/// Schema-agnostic filter expression for dynamic query input.
/// Lowered into a validated predicate at the intent boundary.
///

#[derive(Clone, Debug)]
pub struct FilterExpr(pub Predicate);

impl FilterExpr {
    /// Lower the filter expression into a validated predicate for the provided schema.
    pub(crate) fn lower_with(&self, schema: &SchemaInfo) -> Result<Predicate, ValidateError> {
        // Phase 1: normalize enum literals using schema enum metadata.
        let normalized_enum_literals = normalize_enum_literals(schema, &self.0)?;

        // Phase 2: reject unsupported query features and validate against schema.
        reject_unsupported_query_features(&normalized_enum_literals)?;
        validate(schema, &normalized_enum_literals)?;

        // Phase 3: normalize structural predicate shape for deterministic planning.
        Ok(normalize(&normalized_enum_literals))
    }
}

///
/// SortExpr
/// Schema-agnostic sort expression for dynamic query input.
/// Lowered into a validated order spec at the intent boundary.
///

#[derive(Clone, Debug)]
pub struct SortExpr {
    pub fields: Vec<(String, OrderDirection)>,
}

impl SortExpr {
    /// Lower the sort expression into a validated order spec for the provided schema.
    pub(crate) fn lower_with(&self, schema: &SchemaInfo) -> Result<OrderSpec, SortLowerError> {
        let spec = OrderSpec {
            fields: self.fields.clone(),
        };

        validate_order(schema, &spec)?;

        Ok(spec)
    }
}

///
/// SortLowerError
/// Errors returned when lowering sort expressions into order specs.
///

#[derive(Debug, ThisError)]
pub(crate) enum SortLowerError {
    #[error("{0}")]
    Validate(#[from] ValidateError),

    #[error("{0}")]
    Plan(Box<PlanError>),
}

impl From<PlanError> for SortLowerError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}
