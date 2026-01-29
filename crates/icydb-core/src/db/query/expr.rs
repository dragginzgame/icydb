use crate::{
    db::query::{
        plan::{OrderDirection, OrderSpec, PlanError, validate::validate_order},
        predicate::{self, Predicate, SchemaInfo, ValidateError, normalize},
    },
    traits::EntityKind,
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
    /// Lower the filter expression into a validated predicate for `E`.
    pub fn lower<E: EntityKind>(&self) -> Result<Predicate, ValidateError> {
        let schema = SchemaInfo::from_entity_model(E::MODEL)?;
        predicate::validate(&schema, &self.0)?;

        Ok(normalize(&self.0))
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
    /// Lower the sort expression into a validated order spec for `E`.
    pub fn lower<E: EntityKind>(&self) -> Result<OrderSpec, SortLowerError> {
        let schema = SchemaInfo::from_entity_model(E::MODEL)?;
        let spec = OrderSpec {
            fields: self.fields.clone(),
        };

        validate_order(&schema, &spec)?;

        Ok(spec)
    }
}

///
/// SortLowerError
/// Errors returned when lowering sort expressions into order specs.
///

#[derive(Debug, ThisError)]
pub enum SortLowerError {
    #[error("{0}")]
    Validate(#[from] ValidateError),

    #[error("{0}")]
    Plan(#[from] PlanError),
}
