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
pub(crate) enum PushdownApplicability {
    NotApplicable,
    Applicable(SecondaryOrderPushdownEligibility),
}

impl PushdownApplicability {
    /// Return true when this applicability state is eligible for secondary-order pushdown.
    #[must_use]
    pub(crate) const fn is_eligible(&self) -> bool {
        matches!(
            self,
            Self::Applicable(SecondaryOrderPushdownEligibility::Eligible { .. })
        )
    }

    /// Return a shared surface projection when pushdown applicability is present.
    #[must_use]
    pub(crate) const fn surface_eligibility(&self) -> Option<PushdownSurfaceEligibility<'_>> {
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
    AccessPathIndexRangeUnsupported {
        index: &'static str,
        prefix_len: usize,
    },
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
pub(crate) fn assess_secondary_order_pushdown<K>(
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

    let Some(access) = plan.access.as_path() else {
        if let Some((index, prefix_len)) = first_index_range_details(&plan.access) {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index,
                    prefix_len,
                },
            );
        }

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };

    match access {
        AccessPath::IndexPrefix { index, values } => {
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
        AccessPath::IndexRange { index, prefix, .. } => {
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: index.name,
                    prefix_len: prefix.len(),
                },
            )
        }
        _ => SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ),
    }
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

// Walk an access-plan tree and return the first index-range child, if any.
fn first_index_range_details<K>(access: &AccessPlan<K>) -> Option<(&'static str, usize)> {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::IndexRange { index, prefix, .. } => Some((index.name, prefix.len())),
            _ => None,
        },
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().find_map(first_index_range_details)
        }
    }
}

fn applicability_from_eligibility(
    eligibility: SecondaryOrderPushdownEligibility,
) -> PushdownApplicability {
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
///
/// Returns `PushdownApplicability::NotApplicable` for non-applicable shapes:
/// - no ORDER BY fields
/// - access path is not a secondary index path
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> PushdownApplicability {
    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
///
/// This variant keeps applicability explicit and assumes validated invariants
/// with debug assertions, while preserving safe fallbacks in release builds.
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> PushdownApplicability {
    if let Some(order) = plan.order.as_ref() {
        debug_assert!(
            !order.fields.is_empty(),
            "validated plan must not contain an empty ORDER BY specification"
        );
    }

    if let Some(access) = plan.access.as_path() {
        match access {
            AccessPath::IndexPrefix { index, values } => {
                debug_assert!(
                    values.len() <= index.fields.len(),
                    "validated plan must keep index-prefix bounds within declared index fields"
                );
            }
            AccessPath::IndexRange { index, prefix, .. } => {
                debug_assert!(
                    prefix.len() < index.fields.len(),
                    "validated plan must keep index-range prefix within declared index fields"
                );
            }
            _ => {}
        }
    }

    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
}
