use crate::db::{
    predicate::{CompareFieldsPredicate, ComparePredicate, Predicate},
    sql::parser::{SqlCompareFieldsPredicate, SqlComparePredicate, SqlPredicate},
};

/// Lower one parser-owned SQL predicate tree back onto the runtime predicate
/// authority without widening the admitted reduced SQL predicate family.
pub(in crate::db) fn lower_sql_predicate(predicate: SqlPredicate) -> Predicate {
    match predicate {
        SqlPredicate::True => Predicate::True,
        SqlPredicate::False => Predicate::False,
        SqlPredicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(lower_sql_predicate)
                .collect::<Vec<_>>(),
        ),
        SqlPredicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(lower_sql_predicate)
                .collect::<Vec<_>>(),
        ),
        SqlPredicate::Not(inner) => Predicate::Not(Box::new(lower_sql_predicate(*inner))),
        SqlPredicate::Compare(SqlComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion)),
        SqlPredicate::CompareFields(SqlCompareFieldsPredicate {
            left_field,
            op,
            right_field,
            coercion,
        }) => Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            op,
            right_field,
            coercion,
        )),
        SqlPredicate::IsNull { field } => Predicate::IsNull { field },
        SqlPredicate::IsNotNull { field } => Predicate::IsNotNull { field },
        SqlPredicate::IsMissing { field } => Predicate::IsMissing { field },
        SqlPredicate::IsEmpty { field } => Predicate::IsEmpty { field },
        SqlPredicate::IsNotEmpty { field } => Predicate::IsNotEmpty { field },
        SqlPredicate::TextContains { field, value } => Predicate::TextContains { field, value },
        SqlPredicate::TextContainsCi { field, value } => Predicate::TextContainsCi { field, value },
    }
}
