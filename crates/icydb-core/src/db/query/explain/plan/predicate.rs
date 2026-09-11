//! Charged projection of the canonical predicate into the diagnostic DTO.
//! Logical explain and execution descriptors retain only this DTO. Fingerprint
//! construction borrows source semantics without another retained model.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    predicate::Predicate,
    query::{explain::plan::ExplainPredicate, preparation::PreparationWork},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl ExplainPredicate {
    /// Project admitted syntax without normalization. Charge every retained
    /// operand and container before allocation; return no partial DTO on error.
    pub(in crate::db) fn from_predicate(
        predicate: &Predicate,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match predicate {
            Predicate::True => Self::True,
            Predicate::False => Self::False,
            Predicate::And(children) => {
                Self::And(work.copy_slice(children, |child| Self::from_predicate(child, work))?)
            }
            Predicate::Or(children) => {
                Self::Or(work.copy_slice(children, |child| Self::from_predicate(child, work))?)
            }
            Predicate::Not(inner) => {
                work.charge(Resource::TemporaryBytes, size_of::<Self>() as u64)?;
                Self::Not(Box::new(Self::from_predicate(inner, work)?))
            }
            Predicate::Compare(compare) => Self::Compare {
                field: work.copy_text(&compare.field)?,
                op: compare.op,
                value: work.copy_value(&compare.value)?,
                coercion: work.copy_coercion(&compare.coercion)?,
            },
            Predicate::CompareFields(compare) => Self::CompareFields {
                left_field: work.copy_text(compare.left_field())?,
                op: compare.op(),
                right_field: work.copy_text(compare.right_field())?,
                coercion: work.copy_coercion(compare.coercion())?,
            },
            Predicate::IsNull { field } => Self::IsNull {
                field: work.copy_text(field)?,
            },
            Predicate::IsNotNull { field } => Self::IsNotNull {
                field: work.copy_text(field)?,
            },
            Predicate::IsMissing { field } => Self::IsMissing {
                field: work.copy_text(field)?,
            },
            Predicate::IsEmpty { field } => Self::IsEmpty {
                field: work.copy_text(field)?,
            },
            Predicate::IsNotEmpty { field } => Self::IsNotEmpty {
                field: work.copy_text(field)?,
            },
            Predicate::TextContains { field, value } => Self::TextContains {
                field: work.copy_text(field)?,
                value: work.copy_value(value)?,
            },
            Predicate::TextContainsCi { field, value } => Self::TextContainsCi {
                field: work.copy_text(field)?,
                value: work.copy_value(value)?,
            },
        })
    }
}
