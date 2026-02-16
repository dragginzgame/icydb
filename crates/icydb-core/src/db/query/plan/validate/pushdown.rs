use crate::{
    db::query::plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection},
    model::entity::EntityModel,
};

///
/// SecondaryOrderPushdownEligibility
///
/// Planner-side eligibility decision for secondary-index ORDER BY pushdown.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownEligibility {
    Eligible {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// SecondaryOrderPushdownRejection
///
/// Deterministic reason why secondary-index ORDER BY pushdown is not eligible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownRejection {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    MissingPrimaryKeyTieBreak {
        field: String,
    },
    PrimaryKeyDirectionNotAscending {
        field: String,
    },
    NonAscendingDirection {
        field: String,
    },
    OrderFieldsDoNotMatchIndex {
        index: &'static str,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order) = plan.order.as_ref() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };

    if order.fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let AccessPlan::Path(AccessPath::IndexPrefix { index, values }) = &plan.access else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };

    if values.len() > index.fields.len() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                prefix_len: values.len(),
                index_field_len: index.fields.len(),
            },
        );
    }

    let pk_field = model.primary_key.name;
    let Some((last_field, last_direction)) = order.fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };

    if last_field != pk_field {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: pk_field.to_string(),
            },
        );
    }

    if *last_direction != OrderDirection::Asc {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                field: last_field.clone(),
            },
        );
    }

    for (field, direction) in &order.fields {
        if *direction != OrderDirection::Asc {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::NonAscendingDirection {
                    field: field.clone(),
                },
            );
        }
    }

    let actual_non_pk: Vec<String> = order
        .fields
        .iter()
        .take(order.fields.len().saturating_sub(1))
        .map(|(field, _)| field.clone())
        .collect();
    let expected_full = index
        .fields
        .iter()
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let expected_suffix = index
        .fields
        .iter()
        .skip(values.len())
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();

    if actual_non_pk == expected_suffix || actual_non_pk == expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index.name,
            prefix_len: values.len(),
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index.name,
            prefix_len: values.len(),
            expected_suffix,
            expected_full,
            actual: actual_non_pk,
        },
    )
}
