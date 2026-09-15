//! Module: query::plan::parameters
//! Responsibility: value-independent predicate parameter contracts for shared planning.
//! Does not own: public prepared-statement APIs, SQL placeholder syntax, or execution routing.
//! Boundary: turns one validated normalized predicate into a reusable slot topology and
//! binds each execution to its own canonical values.

#[cfg(test)]
mod admission_tests;

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, Predicate},
        query::{
            construction::ConstructionBudget,
            plan::primary_key_input_resource::estimate_value_payload_bytes,
        },
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Maximum number of scalar/list slots retained by one shared query template.
/// Larger predicate shapes stay on the literal-specific cache path.
const MAX_PREPARED_QUERY_PARAMETER_SLOTS: usize = 64;
const MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS: usize = 1_024;
const MAX_PREPARED_QUERY_PARAMETER_BYTES: u32 = 256 * 1_024;

///
/// PreparedQueryParameterContract
///
/// Value-independent slot topology for the supported equality, ordered-range,
/// and membership predicate families. Field identity, operator, coercion, and
/// scalar/list shape remain part of the contract; literal payloads do not.
///
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct PreparedQueryParameterContract {
    predicate: ParameterPredicate,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ParameterPredicate {
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        field: String,
        operator: ParameterOperator,
        coercion: ParameterCoercion,
        slot: ParameterSlot,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ParameterOperator {
    ExactSet,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ParameterCoercion {
    id: CoercionId,
    params: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ParameterSlot {
    ExactSet { element_tag: u8 },
    Scalar { value_tag: u8 },
}

impl PreparedQueryParameterContract {
    /// Derive one bounded reusable parameter contract from an already
    /// schema-normalized predicate. Unsupported or oversized shapes keep the
    /// caller on the literal-specific cache path. Construction exhaustion is
    /// an error, never cache ineligibility or permission to continue compiling.
    pub(in crate::db) fn from_normalized_predicate(
        predicate: &Predicate,
        budget: &dyn ConstructionBudget,
    ) -> Result<Option<Self>, InternalError> {
        let mut slot_count = 0usize;
        let mut parameter_bytes = 0u32;
        let predicate = ParameterPredicate::from_normalized_predicate(
            predicate,
            &mut slot_count,
            &mut parameter_bytes,
            budget,
        )?;
        if slot_count == 0 {
            return Ok(None);
        }

        Ok(predicate.map(|predicate| Self { predicate }))
    }
}

impl ParameterPredicate {
    fn from_normalized_predicate(
        predicate: &Predicate,
        slot_count: &mut usize,
        parameter_bytes: &mut u32,
        budget: &dyn ConstructionBudget,
    ) -> Result<Option<Self>, InternalError> {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match predicate {
            Predicate::And(children) => {
                Self::from_children(children, slot_count, parameter_bytes, budget)?.map(Self::And)
            }
            Predicate::Or(children) => {
                Self::from_children(children, slot_count, parameter_bytes, budget)?.map(Self::Or)
            }
            Predicate::Compare(compare) => {
                // Stop before inspecting/copying another slot once the existing
                // template cap is reached. This is eligibility, not query admission.
                if *slot_count == MAX_PREPARED_QUERY_PARAMETER_SLOTS {
                    return Ok(None);
                }
                if let (CompareOp::In, Value::List(values)) = (compare.op(), compare.value()) {
                    if values.len() > MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS {
                        return Ok(None);
                    }
                    budget.charge(Resource::PredicateExpressionSteps, values.len() as u64)?;
                }
                let Some((operator, slot)) =
                    ParameterSlot::from_compare(compare.op(), compare.value())
                else {
                    return Ok(None);
                };
                *slot_count = slot_count.saturating_add(1);
                *parameter_bytes = parameter_bytes
                    .saturating_add(estimate_value_payload_bytes(compare.value(), budget)?);
                if *parameter_bytes > MAX_PREPARED_QUERY_PARAMETER_BYTES {
                    return Ok(None);
                }

                let field = budget.copy_text(compare.field())?;
                let mut params = budget.vec_with_capacity(compare.coercion().params().len())?;
                for (name, value) in compare.coercion().params() {
                    params.push((budget.copy_text(name)?, budget.copy_text(value)?));
                }

                Some(Self::Compare {
                    field,
                    operator,
                    coercion: ParameterCoercion {
                        id: compare.coercion().id(),
                        params,
                    },
                    slot,
                })
            }
            Predicate::True
            | Predicate::False
            | Predicate::Not(_)
            | Predicate::CompareFields(_)
            | Predicate::IsNull { .. }
            | Predicate::IsNotNull { .. }
            | Predicate::IsMissing { .. }
            | Predicate::IsEmpty { .. }
            | Predicate::IsNotEmpty { .. }
            | Predicate::TextContains { .. }
            | Predicate::TextContainsCi { .. } => None,
        })
    }

    fn from_children(
        children: &[Predicate],
        slot_count: &mut usize,
        parameter_bytes: &mut u32,
        budget: &dyn ConstructionBudget,
    ) -> Result<Option<Vec<Self>>, InternalError> {
        // Grow only for admitted children; a wide ineligible predicate must
        // not reserve its entire input width before reaching the slot cap.
        let mut parameterized = Vec::new();
        for child in children {
            let Some(child) =
                Self::from_normalized_predicate(child, slot_count, parameter_bytes, budget)?
            else {
                return Ok(None);
            };
            budget.reserve_vec(&mut parameterized, 1)?;
            parameterized.push(child);
        }

        Ok(Some(parameterized))
    }
}

impl ParameterSlot {
    fn from_compare(op: CompareOp, value: &Value) -> Option<(ParameterOperator, Self)> {
        match op {
            CompareOp::Eq => (!matches!(value, Value::List(_) | Value::Map(_) | Value::Unit))
                .then_some((
                    ParameterOperator::ExactSet,
                    Self::ExactSet {
                        element_tag: value.canonical_tag().to_u8(),
                    },
                )),
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
                let operator = match op {
                    CompareOp::Lt => ParameterOperator::Lt,
                    CompareOp::Lte => ParameterOperator::Lte,
                    CompareOp::Gt => ParameterOperator::Gt,
                    CompareOp::Gte => ParameterOperator::Gte,
                    _ => return None,
                };
                (!matches!(value, Value::List(_) | Value::Map(_) | Value::Unit)).then_some((
                    operator,
                    Self::Scalar {
                        value_tag: value.canonical_tag().to_u8(),
                    },
                ))
            }
            CompareOp::In => {
                let Value::List(values) = value else {
                    return None;
                };
                let first = values.first()?;
                let element_tag = first.canonical_tag().to_u8();
                values
                    .iter()
                    .all(|value| {
                        !matches!(value, Value::List(_) | Value::Map(_) | Value::Unit)
                            && value.canonical_tag().to_u8() == element_tag
                    })
                    .then_some((ParameterOperator::ExactSet, Self::ExactSet { element_tag }))
            }
            CompareOp::Ne
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PreparedQueryParameterContract;
    use crate::{
        db::{Predicate, query::preparation::with_preparation_work},
        value::Value,
    };

    fn contract(predicate: &Predicate) -> Option<PreparedQueryParameterContract> {
        with_preparation_work(|work| {
            PreparedQueryParameterContract::from_normalized_predicate(predicate, work).unwrap()
        })
    }

    #[test]
    fn equality_literals_share_one_parameter_contract() {
        let left = contract(&Predicate::eq("id".to_string(), Value::Nat64(1)));
        let right = contract(&Predicate::eq("id".to_string(), Value::Nat64(2)));

        assert_eq!(left, right);
    }

    #[test]
    fn membership_contract_does_not_depend_on_list_arity() {
        let short = contract(&Predicate::in_("id".to_string(), vec![Value::Nat64(1)]));
        let long = contract(&Predicate::in_(
            "id".to_string(),
            vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
        ));

        assert_eq!(short, long);
    }

    #[test]
    fn exact_and_ordered_slots_do_not_alias() {
        let exact = contract(&Predicate::eq("id".to_string(), Value::Nat64(1)));
        let ordered = contract(&Predicate::gte("id".to_string(), Value::Nat64(1)));

        assert_ne!(exact, ordered);
    }

    #[test]
    fn oversized_list_and_payload_do_not_become_templates() {
        let too_many = contract(&Predicate::in_(
            "id".to_string(),
            (0..=super::MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS)
                .map(|value| Value::Nat64(value as u64))
                .collect(),
        ));
        let too_many_bytes = contract(&Predicate::eq(
            "label".to_string(),
            Value::Text("x".repeat(super::MAX_PREPARED_QUERY_PARAMETER_BYTES as usize + 1)),
        ));

        assert!(too_many.is_none());
        assert!(too_many_bytes.is_none());
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(ParameterCoercion {
Self{id,params} => [id,params],
});
crate::retained::retained_copy!(ParameterOperator);
crate::retained::retained_fields!(ParameterPredicate {
Self::And(field_0) => [field_0],
Self::Or(field_0) => [field_0],
Self::Compare{field,operator,coercion,slot} => [field,operator,coercion,slot],
});
crate::retained::retained_copy!(ParameterSlot);
crate::retained::retained_fields!(PreparedQueryParameterContract {
Self{predicate} => [predicate],
});
