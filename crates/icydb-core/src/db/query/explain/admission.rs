//! Default public-read policy projection for accepted EXPLAIN plans.
//! Policy and rejection precedence remain owned by query admission.

use crate::{
    db::query::{
        admission::{
            QueryAdmissionLane, QueryAdmissionPlanShape, QueryAdmissionPolicy,
            QueryAdmissionRejection, QueryAdmissionSummary,
        },
        explain::writer::JsonWriter,
        plan::AccessPlannedQuery,
    },
    error::InternalError,
};
use icydb_diagnostic_code::QueryReadAdmissionCode;
use std::fmt::{self, Write};

/// Default public-read policy decision for the displayed accepted read plan.
///
/// This is plan admission only, not authorization or a promise of successful
/// execution. No rows are read and no caller-specific page envelope is added.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExplainPublicReadAdmission {
    rejection: Option<QueryAdmissionRejection>,
}

impl ExplainPublicReadAdmission {
    /// Whether the displayed plan satisfies the default bounded-read policy.
    #[must_use]
    pub const fn is_admitted(self) -> bool {
        self.rejection.is_none()
    }

    /// Typed reason returned by the same policy used for public execution.
    #[must_use]
    pub fn rejection(self) -> Option<QueryReadAdmissionCode> {
        self.rejection.map(QueryAdmissionRejection::code)
    }

    /// Stable label for the first policy rejection, if any.
    #[must_use]
    pub fn rejection_label(self) -> Option<&'static str> {
        self.rejection.map(QueryAdmissionRejection::as_str)
    }

    pub(in crate::db) const fn decision_label(self) -> &'static str {
        if self.is_admitted() {
            "admitted"
        } else {
            "rejected"
        }
    }

    pub(in crate::db) fn from_plan(
        plan: &AccessPlannedQuery,
    ) -> Result<Option<Self>, InternalError> {
        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, plan)?;
        Ok(Self::from_summary(&summary))
    }

    pub(in crate::db) fn from_summary(summary: &QueryAdmissionSummary) -> Option<Self> {
        // A mutation plan must never acquire a read-admission verdict merely
        // because its access path satisfies read policy.
        if matches!(summary.plan_shape(), QueryAdmissionPlanShape::Delete) {
            return None;
        }
        Some(Self {
            rejection: QueryAdmissionPolicy::default_bounded_read().rejection_for_summary(summary),
        })
    }
}

pub(in crate::db::query::explain) fn write_public_read_admission(
    admission: Option<ExplainPublicReadAdmission>,
    out: &mut dyn Write,
) -> fmt::Result {
    let Some(admission) = admission else {
        return out.write_str("null");
    };
    let mut object = JsonWriter::begin_object(out)?;
    object.field_str("policy", "default_bounded_read")?;
    object.field_str("decision", admission.decision_label())?;
    match admission.rejection_label() {
        Some(reason) => object.field_str("reason", reason)?,
        None => object.field_null("reason")?,
    }
    object.finish()
}
