use crate::{
    db::{
        access::{AccessPath, IndexRangePathRef},
        direction::Direction,
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is runtime-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPlan<K> {
    Path(Box<AccessPath<K>>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
    /// Construct a plan from one concrete access path.
    #[must_use]
    pub(crate) fn path(path: AccessPath<K>) -> Self {
        Self::Path(Box::new(path))
    }

    /// Construct a plan that forces a full scan.
    #[must_use]
    pub(crate) fn full_scan() -> Self {
        Self::path(AccessPath::FullScan)
    }

    /// Borrow the concrete path when this plan is a single-path node.
    #[must_use]
    pub(crate) fn as_path(&self) -> Option<&AccessPath<K>> {
        match self {
            Self::Path(path) => Some(path.as_ref()),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }

    /// Return true when this plan is exactly one full-scan path.
    #[must_use]
    pub(crate) const fn is_single_full_scan(&self) -> bool {
        matches!(self, Self::Path(path) if path.is_full_scan())
    }

    /// Borrow index-prefix access details when this is a single IndexPrefix path.
    #[must_use]
    pub(crate) fn as_index_prefix_path(&self) -> Option<(&IndexModel, &[Value])> {
        self.as_path().and_then(AccessPath::as_index_prefix)
    }

    /// Borrow index-range access details when this is a single IndexRange path.
    #[must_use]
    pub(crate) fn as_index_range_path(&self) -> Option<IndexRangePathRef<'_>> {
        self.as_path().and_then(AccessPath::as_index_range)
    }

    /// Walk the tree and return the first encountered IndexRange details.
    #[must_use]
    pub(crate) fn first_index_range_details(&self) -> Option<(&'static str, usize)> {
        match self {
            Self::Path(path) => path.index_range_details(),
            Self::Union(children) | Self::Intersection(children) => {
                children.iter().find_map(Self::first_index_range_details)
            }
        }
    }
}

impl<K> From<AccessPath<K>> for AccessPlan<K> {
    fn from(value: AccessPath<K>) -> Self {
        Self::path(value)
    }
}

type OrderFieldRef<'a> = (&'a str, Direction);

///
/// SecondaryOrderPushdownEligibility
///
/// Access-layer eligibility decision for secondary-index ORDER BY pushdown.
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
    MixedDirectionNotEligible {
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

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan shape.
pub(crate) fn assess_secondary_order_pushdown_from_parts<K>(
    model: &EntityModel,
    order_fields: Option<&[OrderFieldRef<'_>]>,
    access_plan: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order_fields) = order_fields else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if order_fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
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

    if let Some((index, values)) = access.as_index_prefix() {
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
            order_fields,
            index.name,
            index.fields,
            values.len(),
        )
    } else if let Some((index, prefix_len)) = access.index_range_details() {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len },
        )
    } else {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        )
    }
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[OrderFieldRef<'_>],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };

    if *last_field != model.primary_key.name {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);

    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);

    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

// Evaluate pushdown eligibility for plans that are already known to be
// structurally applicable (ORDER BY + single index-prefix access path).
fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[OrderFieldRef<'_>],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(model, order_fields, index_name, index_fields, prefix_len)
}
