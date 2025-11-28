use crate::{
    db::{
        executor::coerce::family::coerce_basic,
        primitives::{Cmp, FilterClause, FilterExpr},
        query::{QueryError, QueryValidate},
    },
    traits::{EntityKind, FieldValues},
    value::Value,
};

///
/// FilterEvaluator
///

pub struct FilterEvaluator<'a> {
    values: &'a dyn FieldValues,
}

impl<'a> FilterEvaluator<'a> {
    #[must_use]
    pub const fn new(values: &'a dyn FieldValues) -> Self {
        Self { values }
    }

    #[must_use]
    pub fn eval(&self, expr: &FilterExpr) -> bool {
        match expr {
            FilterExpr::True => true,
            FilterExpr::False => false,
            FilterExpr::Clause(clause) => self.eval_clause(clause),
            FilterExpr::And(children) => children.iter().all(|e| self.eval(e)),
            FilterExpr::Or(children) => children.iter().any(|e| self.eval(e)),
            FilterExpr::Not(inner) => !self.eval(inner),
        }
    }

    // eval_clause
    fn eval_clause(&self, clause: &FilterClause) -> bool {
        self.values
            .get_value(clause.field.as_str())
            .is_some_and(|actual| Self::compare(&actual, clause.cmp, &clause.value))
    }

    /// Core comparator dispatch
    fn compare(left: &Value, cmp: Cmp, right: &Value) -> bool {
        // 0) Presence/null checks (RHS ignored)
        match cmp {
            Cmp::IsNone => return matches!(left, Value::None),
            Cmp::IsSome => return !matches!(left, Value::None),
            _ => {}
        }

        // --- 0b) is_empty / is_not_empty (single Option<bool>) ---
        if let Some(res) = match cmp {
            Cmp::IsEmpty => left.is_empty(),
            Cmp::IsNotEmpty => left.is_not_empty(),
            _ => None,
        } {
            return res;
        }

        // after handling IsNone/IsSome:
        if let Some(res) = coerce_basic(left, right, cmp) {
            return res;
        }

        //  final fallback: strict same-variant compare only
        match cmp {
            Cmp::Eq => left == right,
            Cmp::Ne => left != right,
            Cmp::Lt => left < right,
            Cmp::Lte => left <= right,
            Cmp::Gt => left > right,
            Cmp::Gte => left >= right,
            _ => false,
        }
    }
}

///
/// QueryValidate
///

impl<E: EntityKind> QueryValidate<E> for FilterExpr {
    #[allow(clippy::match_same_arms, clippy::too_many_lines)]
    fn validate(&self) -> Result<(), QueryError> {
        match self {
            Self::True | Self::False => Ok(()),

            Self::Clause(c) => {
                if !E::FIELDS.contains(&c.field.as_str()) {
                    return Err(QueryError::InvalidFilterField(c.field.clone()));
                }

                let v = &c.value;
                let field = &c.field;

                match c.cmp {
                    // Ordering comparators
                    Cmp::Lt | Cmp::Lte | Cmp::Gt | Cmp::Gte => {
                        if !(v.is_numeric() || v.is_text()) {
                            return Err(QueryError::InvalidFilterValue(format!(
                                "field '{field}' expects comparable RHS (numeric or text) for {cmp:?}",
                                cmp = c.cmp
                            )));
                        }
                    }

                    // Case-sensitive text
                    Cmp::StartsWith | Cmp::EndsWith | Cmp::Contains => {
                        if !v.is_text() {
                            // Allow non-text only for Contains (collection)
                            if !matches!(c.cmp, Cmp::Contains) {
                                return Err(QueryError::InvalidFilterValue(format!(
                                    "field '{field}' expects text RHS for {cmp:?}",
                                    cmp = c.cmp
                                )));
                            }
                        }
                    }

                    // Case-insensitive text
                    Cmp::EqCi
                    | Cmp::NeCi
                    | Cmp::ContainsCi
                    | Cmp::StartsWithCi
                    | Cmp::EndsWithCi => {
                        if !v.is_text() {
                            return Err(QueryError::InvalidFilterValue(format!(
                                "field '{field}' expects text RHS for {cmp:?}",
                                cmp = c.cmp
                            )));
                        }
                    }

                    // Null / presence
                    Cmp::IsSome | Cmp::IsNone | Cmp::IsEmpty | Cmp::IsNotEmpty => {
                        if !v.is_unit() {
                            return Err(QueryError::InvalidFilterValue(format!(
                                "field '{field}' expects unit RHS for {cmp:?}",
                                cmp = c.cmp
                            )));
                        }
                    }

                    // Membership & equality family
                    Cmp::In | Cmp::NotIn | Cmp::Eq | Cmp::Ne => {
                        // no strong type enforcement — allow scalar or list
                    }

                    // Collection membership
                    Cmp::AnyIn | Cmp::AllIn => {
                        // Allow list RHS; tolerate scalar
                        if !matches!(v, Value::List(_)) {
                            // scalar fallback allowed
                        }
                    }

                    // Case-insensitive collection membership
                    // Case-insensitive membership; allow any scalar/list so identifiers
                    // (e.g., ULID, Principal) can reuse the CI variants without failing
                    // validation. Text values will still be compared case-insensitively;
                    // other types fall back to normal equality.
                    Cmp::AnyInCi | Cmp::AllInCi | Cmp::InCi => {
                        // No extra validation beyond ensuring RHS is provided.
                        if matches!(v, Value::None) {
                            return Err(QueryError::InvalidFilterValue(format!(
                                "field '{field}' {cmp:?} requires a RHS value",
                                cmp = c.cmp
                            )));
                        }
                    }

                    // -------------------------
                    // MAP FILTERS
                    // -------------------------
                    Cmp::MapContainsKey | Cmp::MapNotContainsKey => {
                        if !v.is_scalar() {
                            return Err(QueryError::InvalidFilterValue(format!(
                                "field '{field}' expects scalar key for {cmp:?}",
                                cmp = c.cmp
                            )));
                        }
                    }

                    Cmp::MapContainsValue | Cmp::MapNotContainsValue => {
                        // Any Value allowed as map values
                    }

                    Cmp::MapContainsEntry | Cmp::MapNotContainsEntry => {
                        match v {
                            Value::List(pair) if pair.len() == 2 => {
                                // Key must be scalar
                                if !pair[0].is_scalar() {
                                    return Err(QueryError::InvalidFilterValue(format!(
                                        "field '{field}' expects scalar key in entry pair for {cmp:?}",
                                        cmp = c.cmp
                                    )));
                                }
                                // Value can be any Value
                            }
                            _ => {
                                return Err(QueryError::InvalidFilterValue(format!(
                                    "field '{field}' expects (key, value) pair for {cmp:?}",
                                    cmp = c.cmp
                                )));
                            }
                        }
                    }
                }

                Ok(())
            }

            Self::And(children) | Self::Or(children) => {
                for expr in children {
                    QueryValidate::<E>::validate(expr)?;
                }

                Ok(())
            }

            Self::Not(inner) => QueryValidate::<E>::validate(inner),
        }
    }
}
