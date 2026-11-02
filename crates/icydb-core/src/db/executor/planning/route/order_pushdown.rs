//! Module: executor::planning::route::order_pushdown
//! Responsibility: secondary-index ORDER BY pushdown route DTOs.
//! Does not own: access tree shape, logical ORDER BY validation, or executor dispatch.
//! Boundary: route planning derives these values; explain and trace surfaces project them.

#[cfg(feature = "sql")]
use std::fmt::Write as _;

use crate::db::{
    QueryError,
    query::{
        explain::{
            ExplainOrderPushdown,
            SecondaryOrderPushdownRejection as ExplainSecondaryOrderPushdownRejection,
        },
        preparation::PreparationWork,
    },
};

///
/// PushdownApplicability
///
/// Explicit applicability state for secondary-index ORDER BY pushdown.
///
/// This keeps "not applicable" separate from "applicable but rejected" without
/// nesting another eligibility enum inside the route-owned decision.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PushdownApplicability {
    NotApplicable,
    Eligible { index: String, prefix_len: usize },
    Rejected(SecondaryOrderPushdownRejection),
}

impl PushdownApplicability {
    /// Copy the route decision into detached diagnostics under this caller's budget.
    pub(in crate::db::executor) fn project_explain(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<ExplainOrderPushdown, QueryError> {
        let projected = match self {
            Self::NotApplicable => Self::NotApplicable,
            Self::Eligible { index, prefix_len } => Self::Eligible {
                index: work.copy_text(index)?,
                prefix_len: *prefix_len,
            },
            Self::Rejected(reason) => Self::Rejected(reason.copy_for_explain(work)?),
        };
        Ok(projected.into())
    }

    /// Return true when this applicability state is eligible for secondary-order pushdown.
    #[must_use]
    pub(in crate::db::executor) const fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible { .. })
    }

    /// Render the route diagnostic value used by explain and trace surfaces.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn diagnostic_label(&self) -> String {
        match self {
            Self::NotApplicable => "not_applicable".to_string(),
            Self::Eligible { index, prefix_len } => {
                format!("eligible(index={index},prefix_len={prefix_len})")
            }
            Self::Rejected(reason) => format!("rejected({})", reason.label()),
        }
    }

    /// Return eligible secondary-index details for descriptor projection.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) const fn eligible_secondary_index(&self) -> Option<(&str, usize)> {
        match self {
            Self::Eligible { index, prefix_len } => Some((index.as_str(), *prefix_len)),
            Self::NotApplicable | Self::Rejected(_) => None,
        }
    }
}

impl From<PushdownApplicability> for ExplainOrderPushdown {
    fn from(value: PushdownApplicability) -> Self {
        match value {
            PushdownApplicability::Eligible { index, prefix_len } => {
                Self::EligibleSecondaryIndex { index, prefix_len }
            }
            PushdownApplicability::Rejected(reason) => Self::Rejected(reason.into()),
            PushdownApplicability::NotApplicable => Self::NotApplicable,
        }
    }
}

///
/// SecondaryOrderPushdownRejection
///
/// Deterministic reason why secondary-index ORDER BY pushdown is not eligible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SecondaryOrderPushdownRejection {
    AccessPathIndexRangeUnsupported {
        index: String,
        prefix_len: usize,
    },
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    OrderFieldsDoNotMatchIndex {
        index: String,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
    VariablePrefixSuffixOrderUnsupported {
        index: String,
        prefix_len: usize,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
}

impl From<SecondaryOrderPushdownRejection> for ExplainSecondaryOrderPushdownRejection {
    fn from(value: SecondaryOrderPushdownRejection) -> Self {
        match value {
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index,
                prefix_len,
            } => Self::AccessPathIndexRangeUnsupported { index, prefix_len },
            SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                prefix_len,
                index_field_len,
            } => Self::InvalidIndexPrefixBounds {
                prefix_len,
                index_field_len,
            },
            SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                index,
                prefix_len,
                expected_suffix,
                expected_full,
                actual,
            } => Self::OrderFieldsDoNotMatchIndex {
                index,
                prefix_len,
                expected_suffix,
                expected_full,
                actual,
            },
            SecondaryOrderPushdownRejection::VariablePrefixSuffixOrderUnsupported {
                index,
                prefix_len,
                expected_full,
                actual,
            } => Self::VariablePrefixSuffixOrderUnsupported {
                index,
                prefix_len,
                expected_full,
                actual,
            },
        }
    }
}

impl SecondaryOrderPushdownRejection {
    // Diagnostic output owns its labels independently of route/cache lifetime.
    // Admit each string and list before allocating, including warm-cache calls.
    fn copy_for_explain(&self, work: &PreparationWork<'_>) -> Result<Self, QueryError> {
        Ok(match self {
            Self::AccessPathIndexRangeUnsupported { index, prefix_len } => {
                Self::AccessPathIndexRangeUnsupported {
                    index: work.copy_text(index)?,
                    prefix_len: *prefix_len,
                }
            }
            Self::InvalidIndexPrefixBounds {
                prefix_len,
                index_field_len,
            } => Self::InvalidIndexPrefixBounds {
                prefix_len: *prefix_len,
                index_field_len: *index_field_len,
            },
            Self::OrderFieldsDoNotMatchIndex {
                index,
                prefix_len,
                expected_suffix,
                expected_full,
                actual,
            } => Self::OrderFieldsDoNotMatchIndex {
                index: work.copy_text(index)?,
                prefix_len: *prefix_len,
                expected_suffix: work.copy_slice(expected_suffix, |text| work.copy_text(text))?,
                expected_full: work.copy_slice(expected_full, |text| work.copy_text(text))?,
                actual: work.copy_slice(actual, |text| work.copy_text(text))?,
            },
            Self::VariablePrefixSuffixOrderUnsupported {
                index,
                prefix_len,
                expected_full,
                actual,
            } => Self::VariablePrefixSuffixOrderUnsupported {
                index: work.copy_text(index)?,
                prefix_len: *prefix_len,
                expected_full: work.copy_slice(expected_full, |text| work.copy_text(text))?,
                actual: work.copy_slice(actual, |text| work.copy_text(text))?,
            },
        })
    }

    /// Render a stable diagnostic label for this rejection reason.
    #[must_use]
    #[cfg(feature = "sql")]
    fn label(&self) -> String {
        let mut out = String::new();
        self.write_label(&mut out);

        out
    }

    // Write the stable rejection label without forcing callers to duplicate
    // every rejection-variant match at projection sites.
    #[cfg(feature = "sql")]
    fn write_label(&self, out: &mut String) {
        match self {
            Self::AccessPathIndexRangeUnsupported { index, prefix_len } => {
                let _ = write!(
                    out,
                    "AccessPathIndexRangeUnsupported(index={index},prefix_len={prefix_len})",
                );
            }
            Self::InvalidIndexPrefixBounds {
                prefix_len,
                index_field_len,
            } => {
                let _ = write!(
                    out,
                    "InvalidIndexPrefixBounds(prefix_len={prefix_len},index_field_len={index_field_len})",
                );
            }
            Self::OrderFieldsDoNotMatchIndex {
                index,
                prefix_len,
                expected_suffix,
                expected_full,
                actual,
            } => {
                let _ = write!(
                    out,
                    "OrderFieldsDoNotMatchIndex(index={index},prefix_len={prefix_len},expected_suffix={expected_suffix:?},expected_full={expected_full:?},actual={actual:?})",
                );
            }
            Self::VariablePrefixSuffixOrderUnsupported {
                index,
                prefix_len,
                expected_full,
                actual,
            } => {
                let _ = write!(
                    out,
                    "VariablePrefixSuffixOrderUnsupported(index={index},prefix_len={prefix_len},expected_full={expected_full:?},actual={actual:?})",
                );
            }
        }
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(PushdownApplicability {
Self::NotApplicable => [],
Self::Eligible{index,prefix_len} => [index,prefix_len],
Self::Rejected(field_0) => [field_0],
});
crate::retained::retained_fields!(SecondaryOrderPushdownRejection {
Self::AccessPathIndexRangeUnsupported{index,prefix_len} => [index,prefix_len],
Self::InvalidIndexPrefixBounds{prefix_len,index_field_len} => [prefix_len,index_field_len],
Self::OrderFieldsDoNotMatchIndex{index,prefix_len,expected_suffix,expected_full,actual} => [index,prefix_len,expected_suffix,expected_full,actual],
Self::VariablePrefixSuffixOrderUnsupported{index,prefix_len,expected_full,actual} => [index,prefix_len,expected_full,actual],
});
