use crate::db::query::{
    plan::OrderDirection,
    plan::{PlanError, validate::validate_order},
    predicate::{self, Predicate, ValidateError, normalize, normalize_enum_literals},
};
use crate::db::{contracts::SchemaInfo, query::plan::OrderSpec};
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
        let normalized_enum_literals = normalize_enum_literals(schema, &self.0)?;
        predicate::validate::reject_unsupported_query_features(&normalized_enum_literals)?;
        predicate::validate(schema, &normalized_enum_literals)?;

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
