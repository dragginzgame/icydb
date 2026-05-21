//! Module: query::intent::access_requirement
//! Responsibility: fail-closed query access assertions evaluated after planning.
//! Does not own: optimizer ranking or physical access selection.
//! Boundary: fluent query contracts inspect the selected plan without acting as hints.

use crate::db::query::{
    explain::{ExplainAccessDecisionKind, ExplainAccessDecisionV1},
    intent::QueryError,
    plan::AccessPlannedQuery,
};
use thiserror::Error as ThisError;

/// Required selected access path for fail-closed fluent query contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredAccessPath {
    /// Require primary-key lookup.
    ByKey,
    /// Require multiple primary-key lookup.
    ByKeys,
    /// Require primary-key range lookup.
    KeyRange,
    /// Require secondary-index equality-prefix access.
    IndexPrefix,
    /// Require secondary-index multi-lookup access.
    IndexMultiLookup,
    /// Require secondary-index range access.
    IndexRange,
    /// Require full scan access.
    FullScan,
    /// Require union access.
    Union,
    /// Require intersection access.
    Intersection,
}

impl RequiredAccessPath {
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::ByKey => "ByKey",
            Self::ByKeys => "ByKeys",
            Self::KeyRange => "KeyRange",
            Self::IndexPrefix => "IndexPrefix",
            Self::IndexMultiLookup => "IndexMultiLookup",
            Self::IndexRange => "IndexRange",
            Self::FullScan => "FullScan",
            Self::Union => "Union",
            Self::Intersection => "Intersection",
        }
    }

    const fn matches(self, actual: ExplainAccessDecisionKind) -> bool {
        matches!(
            (self, actual),
            (Self::ByKey, ExplainAccessDecisionKind::ByKey)
                | (Self::ByKeys, ExplainAccessDecisionKind::ByKeys)
                | (Self::KeyRange, ExplainAccessDecisionKind::KeyRange)
                | (Self::IndexPrefix, ExplainAccessDecisionKind::IndexPrefix)
                | (
                    Self::IndexMultiLookup,
                    ExplainAccessDecisionKind::IndexMultiLookup
                )
                | (Self::IndexRange, ExplainAccessDecisionKind::IndexRange)
                | (Self::FullScan, ExplainAccessDecisionKind::FullScan)
                | (Self::Union, ExplainAccessDecisionKind::Union)
                | (Self::Intersection, ExplainAccessDecisionKind::Intersection)
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AccessRequirements {
    index_required: bool,
    named_index: Option<String>,
    access_path: Option<RequiredAccessPath>,
    no_residual_filter: bool,
}

impl AccessRequirements {
    pub(in crate::db) const fn new() -> Self {
        Self {
            index_required: false,
            named_index: None,
            access_path: None,
            no_residual_filter: false,
        }
    }

    pub(in crate::db) const fn require_index(&mut self) {
        self.index_required = true;
    }

    pub(in crate::db) fn require_index_named(&mut self, index_name: impl Into<String>) {
        self.index_required = true;
        self.named_index = Some(index_name.into());
    }

    pub(in crate::db) const fn require_access_path(&mut self, path: RequiredAccessPath) {
        self.access_path = Some(path);
    }

    pub(in crate::db) const fn require_no_residual_filter(&mut self) {
        self.no_residual_filter = true;
    }

    pub(in crate::db) fn validate(&self, plan: &AccessPlannedQuery) -> Result<(), QueryError> {
        if self.is_empty() {
            return Ok(());
        }

        let explain = plan.explain();
        let decision = explain.access_decision();

        if self.index_required && !selected_access_is_secondary_index(decision.selected.kind) {
            return Err(QueryError::from(AccessRequirementError::new(
                AccessRequirementViolation::IndexRequired,
                decision.clone(),
            )));
        }

        if let Some(required_index_name) = &self.named_index
            && decision.selected.index_name.as_deref() != Some(required_index_name.as_str())
        {
            return Err(QueryError::from(AccessRequirementError::new(
                AccessRequirementViolation::NamedIndexRequired {
                    expected: required_index_name.clone(),
                },
                decision.clone(),
            )));
        }

        if let Some(required_path) = self.access_path
            && !required_path.matches(decision.selected.kind)
        {
            return Err(QueryError::from(AccessRequirementError::new(
                AccessRequirementViolation::AccessPathRequired {
                    expected: required_path,
                },
                decision.clone(),
            )));
        }

        if self.no_residual_filter && plan.has_any_residual_filter() {
            return Err(QueryError::from(AccessRequirementError::new(
                AccessRequirementViolation::ResidualFilterForbidden,
                decision.clone(),
            )));
        }

        Ok(())
    }

    pub(in crate::db) const fn is_empty(&self) -> bool {
        !self.index_required
            && self.named_index.is_none()
            && self.access_path.is_none()
            && !self.no_residual_filter
    }
}

/// Query access requirement failure with the selected decision preserved.
#[derive(Debug, ThisError)]
#[error(
    "query access requirement failed: {violation}; selected={selected_label}",
    selected_label = decision.selected.label
)]
pub struct AccessRequirementError {
    violation: AccessRequirementViolation,
    decision: ExplainAccessDecisionV1,
}

impl AccessRequirementError {
    pub(in crate::db) const fn new(
        violation: AccessRequirementViolation,
        decision: ExplainAccessDecisionV1,
    ) -> Self {
        Self {
            violation,
            decision,
        }
    }

    /// Borrow the violated access requirement.
    #[must_use]
    pub const fn violation(&self) -> &AccessRequirementViolation {
        &self.violation
    }

    /// Borrow the selected access decision that failed the requirement.
    #[must_use]
    pub const fn decision(&self) -> &ExplainAccessDecisionV1 {
        &self.decision
    }
}

/// Specific fail-closed access requirement that was not satisfied.
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum AccessRequirementViolation {
    /// A secondary-index route was required but not selected.
    #[error("secondary index access required")]
    IndexRequired,
    /// One specific semantic index name was required but not selected.
    #[error("index '{expected}' required")]
    NamedIndexRequired {
        /// Required semantic index name.
        expected: String,
    },
    /// One selected access path kind was required but not selected.
    #[error("access path '{}' required", expected.code())]
    AccessPathRequired {
        /// Required selected access path.
        expected: RequiredAccessPath,
    },
    /// Residual predicate or scalar filter work was forbidden.
    #[error("residual filter forbidden")]
    ResidualFilterForbidden,
}

const fn selected_access_is_secondary_index(kind: ExplainAccessDecisionKind) -> bool {
    matches!(
        kind,
        ExplainAccessDecisionKind::IndexPrefix
            | ExplainAccessDecisionKind::IndexMultiLookup
            | ExplainAccessDecisionKind::IndexRange
    )
}
