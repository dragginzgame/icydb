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
/// PushdownApplicability
///
/// Explicit applicability state for secondary-index ORDER BY pushdown.
///
/// This avoids overloading `Option<SecondaryOrderPushdownEligibility>` and
/// keeps "not applicable" separate from "applicable but rejected".
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PushdownApplicability {
    NotApplicable,
    Applicable(SecondaryOrderPushdownEligibility),
}

impl PushdownApplicability {
    /// Return true when this applicability state is eligible for secondary-order pushdown.
    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        matches!(
            self,
            Self::Applicable(SecondaryOrderPushdownEligibility::Eligible { .. })
        )
    }

    /// Return a shared surface projection when pushdown applicability is present.
    #[must_use]
    pub const fn surface_eligibility(&self) -> Option<PushdownSurfaceEligibility<'_>> {
        match self {
            Self::NotApplicable => None,
            Self::Applicable(SecondaryOrderPushdownEligibility::Eligible { index, prefix_len }) => {
                Some(PushdownSurfaceEligibility::EligibleSecondaryIndex {
                    index,
                    prefix_len: *prefix_len,
                })
            }
            Self::Applicable(SecondaryOrderPushdownEligibility::Rejected(reason)) => {
                Some(PushdownSurfaceEligibility::Rejected { reason })
            }
        }
    }
}

///
/// PushdownSurfaceEligibility
///
/// Shared conversion boundary from core eligibility into surface-facing
/// projections used by explain and trace layers.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownSurfaceEligibility<'a> {
    EligibleSecondaryIndex {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected {
        reason: &'a SecondaryOrderPushdownRejection,
    },
}

impl<'a> From<&'a SecondaryOrderPushdownEligibility> for PushdownSurfaceEligibility<'a> {
    fn from(value: &'a SecondaryOrderPushdownEligibility) -> Self {
        match value {
            SecondaryOrderPushdownEligibility::Eligible { index, prefix_len } => {
                Self::EligibleSecondaryIndex {
                    index,
                    prefix_len: *prefix_len,
                }
            }
            SecondaryOrderPushdownEligibility::Rejected(reason) => Self::Rejected { reason },
        }
    }
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

    assess_secondary_order_pushdown_for_applicable_shape(
        model,
        &order.fields,
        index.name,
        index.fields,
        values.len(),
    )
}

/// Evaluate pushdown eligibility for plans that are already known to be
/// structurally applicable (ORDER BY + single index-prefix access path).
///
/// This helper is shared by both defensive and validated-plan assessors.
fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[(String, OrderDirection)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let pk_field = model.primary_key.name;
    let Some((last_field, last_direction)) = order_fields.last() else {
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

    for (field, direction) in order_fields {
        if *direction != OrderDirection::Asc {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::NonAscendingDirection {
                    field: field.clone(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let actual_non_pk = || {
        order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| field.as_str())
    };

    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && actual_non_pk()
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);

    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && actual_non_pk()
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);

    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    let actual_non_pk = order_fields
        .iter()
        .take(actual_non_pk_len)
        .map(|(field, _)| field.clone())
        .collect::<Vec<_>>();
    let expected_full = index_fields
        .iter()
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();
    let expected_suffix = index_fields
        .iter()
        .skip(prefix_len)
        .map(|field| (*field).to_string())
        .collect::<Vec<_>>();

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix,
            expected_full,
            actual: actual_non_pk,
        },
    )
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
///
/// Returns `PushdownApplicability::NotApplicable` for non-applicable shapes:
/// - no ORDER BY fields
/// - access path is not a single index prefix
pub fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> PushdownApplicability {
    let eligibility = assess_secondary_order_pushdown(model, plan);
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
///
/// This variant keeps applicability explicit and assumes validated invariants
/// with debug assertions, while preserving safe fallbacks in release builds.
pub fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> PushdownApplicability {
    let Some(order) = plan.order.as_ref() else {
        return PushdownApplicability::NotApplicable;
    };

    if order.fields.is_empty() {
        debug_assert!(
            false,
            "validated plan must not contain an empty ORDER BY specification"
        );
        return PushdownApplicability::NotApplicable;
    }

    let AccessPlan::Path(AccessPath::IndexPrefix { index, values }) = &plan.access else {
        return PushdownApplicability::NotApplicable;
    };

    debug_assert!(
        values.len() <= index.fields.len(),
        "validated plan must keep index-prefix bounds within declared index fields"
    );
    let eligibility = if values.len() > index.fields.len() {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                prefix_len: values.len(),
                index_field_len: index.fields.len(),
            },
        )
    } else {
        assess_secondary_order_pushdown_for_applicable_shape(
            model,
            &order.fields,
            index.name,
            index.fields,
            values.len(),
        )
    };

    PushdownApplicability::Applicable(eligibility)
}
