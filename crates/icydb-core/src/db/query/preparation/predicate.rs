//! Copy admitted predicate models under the existing request, without invoking
//! constructors that normalize field ordering or simplify boolean structure.

use crate::db::{
    QueryError,
    predicate::{CoercionSpec, CompareFieldsPredicate, ComparePredicate, Predicate},
    query::preparation::PreparationWork,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl PreparationWork<'_> {
    /// Copy canonical syntax without changing the model used for identity.
    /// The caller must admit depth before entering this recursive copy owner.
    pub(in crate::db) fn copy_predicate(
        &self,
        predicate: &Predicate,
    ) -> Result<Predicate, QueryError> {
        self.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match predicate {
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
            Predicate::And(children) => {
                Predicate::And(self.copy_slice(children, |child| self.copy_predicate(child))?)
            }
            Predicate::Or(children) => {
                Predicate::Or(self.copy_slice(children, |child| self.copy_predicate(child))?)
            }
            Predicate::Not(inner) => {
                self.charge(Resource::TemporaryBytes, size_of::<Predicate>() as u64)?;
                Predicate::Not(Box::new(self.copy_predicate(inner)?))
            }
            Predicate::Compare(compare) => Predicate::Compare(ComparePredicate {
                field: self.copy_text(&compare.field)?,
                op: compare.op,
                value: self.copy_value(&compare.value)?,
                coercion: self.copy_coercion(&compare.coercion)?,
            }),
            Predicate::CompareFields(compare) => Predicate::CompareFields(CompareFieldsPredicate {
                left_field: self.copy_text(compare.left_field())?,
                op: compare.op(),
                right_field: self.copy_text(compare.right_field())?,
                coercion: self.copy_coercion(compare.coercion())?,
            }),
            Predicate::IsNull { field } => Predicate::IsNull {
                field: self.copy_text(field)?,
            },
            Predicate::IsNotNull { field } => Predicate::IsNotNull {
                field: self.copy_text(field)?,
            },
            Predicate::IsMissing { field } => Predicate::IsMissing {
                field: self.copy_text(field)?,
            },
            Predicate::IsEmpty { field } => Predicate::IsEmpty {
                field: self.copy_text(field)?,
            },
            Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty {
                field: self.copy_text(field)?,
            },
            Predicate::TextContains { field, value } => Predicate::TextContains {
                field: self.copy_text(field)?,
                value: self.copy_value(value)?,
            },
            Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi {
                field: self.copy_text(field)?,
                value: self.copy_value(value)?,
            },
        })
    }

    /// Copy raw coercion parameters, preserving order and duplicates.
    pub(in crate::db) fn copy_coercion(
        &self,
        coercion: &CoercionSpec,
    ) -> Result<CoercionSpec, QueryError> {
        Ok(CoercionSpec {
            id: coercion.id(),
            params: self.copy_slice(coercion.params(), |(name, value)| {
                Ok((self.copy_text(name)?, self.copy_text(value)?))
            })?,
        })
    }
}
